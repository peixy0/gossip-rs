use crate::event::*;
use crate::timer;
use crate::timer::TimerService;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::*;

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub(crate) struct InitiateHeartbeat(u64);

#[derive(Debug)]
pub(crate) struct InitiateElection;

pub(crate) enum Inbound {
    MakeRequest(MakeRequest),
    InitiateHeartbeat(InitiateHeartbeat),
    InitiateElection(InitiateElection),
    RequestVote(RequestVote),
    Vote(Vote),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    Shutdown,
}

pub struct JoinHandle(tokio::sync::oneshot::Receiver<()>);

impl JoinHandle {
    pub async fn wait(self) {
        let _ = self.0.await;
    }
}

pub trait DependencyProvider {
    type TimerService: timer::TimerService + Send;
}

#[derive(Clone)]
pub struct DefaultDependencyProvider;

impl DependencyProvider for DefaultDependencyProvider {
    type TimerService = timer::DefaultTimerService;
}

#[derive(Clone)]
pub struct Node<Provider> {
    tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
    _marker: std::marker::PhantomData<Provider>,
}

impl<Provider: DependencyProvider + 'static> Node<Provider> {
    pub fn new(
        node_id: u64,
        num_nodes: u64,
        outbound_tx: tokio::sync::mpsc::UnboundedSender<(u64, Outbound)>,
        timer_service: Provider::TimerService,
        quorum: u64,
    ) -> (Self, JoinHandle) {
        let (runner_tx, runner_rx) = tokio::sync::mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let runner = Runner::<Provider>::new(
            node_id,
            num_nodes,
            runner_rx,
            runner_tx.clone(),
            outbound_tx,
            timer_service,
            quorum,
        );
        tokio::spawn(async move {
            runner.run_eventloop().await;
            let _ = exit_tx.send(());
        });
        (
            Self {
                tx: runner_tx,
                _marker: std::marker::PhantomData,
            },
            JoinHandle(exit_rx),
        )
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(Inbound::Shutdown);
    }
}

impl<Provider> Recv<RequestVote> for Node<Provider> {
    fn recv(&self, event: RequestVote) {
        let _ = self.tx.send(Inbound::RequestVote(event));
    }
}

impl<Provider> Recv<Vote> for Node<Provider> {
    fn recv(&self, event: Vote) {
        let _ = self.tx.send(Inbound::Vote(event));
    }
}

impl<Provider> Recv<AppendEntries> for Node<Provider> {
    fn recv(&self, event: AppendEntries) {
        let _ = self.tx.send(Inbound::AppendEntries(event));
    }
}

impl<Provider> Recv<AppendEntriesResponse> for Node<Provider> {
    fn recv(&self, event: AppendEntriesResponse) {
        let _ = self.tx.send(Inbound::AppendEntriesResponse(event));
    }
}

impl<Provider> Recv<MakeRequest> for Node<Provider> {
    fn recv(&self, event: MakeRequest) {
        let _ = self.tx.send(Inbound::MakeRequest(event));
    }
}

#[cfg(test)]
impl<Provider> Recv<Inbound> for Node<Provider> {
    fn recv(&self, event: Inbound) {
        let _ = self.tx.send(event);
    }
}

trait EventLoop<Event> {
    async fn run_eventloop(self);
}

trait OnEvent<T> {
    fn on_event(&mut self, event: T);
}

struct Runner<Provider: DependencyProvider> {
    node_id: u64,
    num_nodes: u64,
    leader_id: Option<u64>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<(u64, Outbound)>,
    timer_service: Provider::TimerService,

    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    next_backoff_index: HashMap<u64, u64>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    heartbeat_timeout_base_in_ms: u64,
    heartbeat_timer: HashMap<
        u64,
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,
    >,
    election_timeout_base_in_ms: u64,
    election_timer:
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,

    votes_collected: HashSet<u64>,
    quorum: u64,
}

impl<Provider: DependencyProvider> Runner<Provider> {
    fn new(
        node_id: u64,
        num_nodes: u64,
        rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
        internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
        outbound_tx: tokio::sync::mpsc::UnboundedSender<(u64, Outbound)>,
        timer_service: Provider::TimerService,
        quorum: u64,
    ) -> Self {
        Self {
            node_id,
            num_nodes,
            leader_id: None,
            rx,
            internal_tx,
            outbound_tx,
            timer_service,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_backoff_index: HashMap::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            heartbeat_timeout_base_in_ms: 1000,
            heartbeat_timer: HashMap::new(),
            election_timeout_base_in_ms: 5000,
            election_timer: None,
            votes_collected: HashSet::new(),
            quorum,
        }
    }

    fn send_to(&self, peer_id: u64, event: impl Into<Outbound>) {
        let msg = event.into();
        debug!("[Node {}] -> [Node {}] {:?}", self.node_id, peer_id, msg);
        let _ = self.outbound_tx.send((peer_id, msg));
    }

    fn get_last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_log_term(&self, log_index: u64) -> u64 {
        if log_index == 0 || log_index as usize > self.log.len() {
            return 0;
        }
        self.log
            .get(log_index as usize - 1)
            .map(|e| e.term)
            .unwrap_or(0)
    }

    fn start_heartbeat_timer(&mut self, node_id: u64) {
        let tx = self.internal_tx.clone();
        let timeout = tokio::time::Duration::from_millis(self.heartbeat_timeout_base_in_ms);
        self.heartbeat_timer
            .entry(node_id)
            .or_default()
            .replace(self.timer_service.create(timeout, async move {
                let _ = tx.send(Inbound::InitiateHeartbeat(InitiateHeartbeat(node_id)));
            }));
    }

    fn stop_heartbeat_timers(&mut self) {
        self.heartbeat_timer.clear();
    }

    fn start_election_timer(&mut self) {
        let tx = self.internal_tx.clone();
        let r = rand::random_range(0..self.election_timeout_base_in_ms);
        let timeout = tokio::time::Duration::from_millis(self.election_timeout_base_in_ms + r);
        self.election_timer = Some(self.timer_service.create(timeout, async move {
            let _ = tx.send(Inbound::InitiateElection(InitiateElection));
        }));
    }

    fn stop_election_timer(&mut self) {
        self.election_timer = None;
    }

    fn become_follower(&mut self, term: u64) {
        let prev_role = self.role;
        self.role = Role::Follower;
        self.leader_id = None;
        self.current_term = term;
        self.voted_for = None;
        self.stop_heartbeat_timers();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn advance_commit_index(&mut self) {
        while self.last_applied < self.commit_index {
            if let Some(entry) = self.log.get(self.last_applied as usize) {
                info!(
                    "[Node {}] commit log {} {:?}",
                    self.node_id,
                    self.last_applied + 1,
                    entry
                );
                self.last_applied += 1;
            } else {
                break;
            }
        }
    }

    fn become_candidate(&mut self) {
        let prev_role = self.role;
        self.role = Role::Candidate;
        self.leader_id = None;
        self.current_term += 1;
        self.voted_for = Some(self.node_id);
        self.votes_collected.clear();
        self.votes_collected.insert(self.node_id);
        self.stop_election_timer();
        self.start_election_timer();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn become_leader(&mut self) {
        let prev_role = self.role;
        self.role = Role::Leader;
        self.leader_id = Some(self.node_id);
        self.next_backoff_index.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.stop_election_timer();
        for node_id in 1..=self.num_nodes {
            if node_id != self.node_id {
                self.next_backoff_index.insert(node_id, 1);
                self.next_index
                    .insert(node_id, self.get_last_log_index() + 1);
                self.match_index.insert(node_id, 0);
            }
        }
        self.broadcast_heartbeat();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn broadcast_heartbeat(&mut self) {
        for node_id in 1..=self.num_nodes {
            if node_id != self.node_id {
                let _ = self
                    .internal_tx
                    .send(Inbound::InitiateHeartbeat(InitiateHeartbeat(node_id)));
            }
        }
    }

    fn try_advance_commit_index(&mut self) {
        let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
        match_indices.push(self.get_last_log_index());
        match_indices.sort_unstable_by(|a, b| b.cmp(a));
        let new_commit_index = match_indices[(self.quorum - 1) as usize];
        if new_commit_index > self.commit_index
            && self.get_log_term(new_commit_index) == self.current_term
        {
            self.commit_index = new_commit_index;
            self.advance_commit_index();
        }
    }
}

impl<Provider: DependencyProvider> EventLoop<Inbound> for Runner<Provider> {
    async fn run_eventloop(mut self) {
        self.become_follower(self.current_term);
        self.start_election_timer();
        while let Some(event) = self.rx.recv().await {
            match event {
                Inbound::MakeRequest(e) => self.on_event(e),
                Inbound::InitiateHeartbeat(e) => self.on_event(e),
                Inbound::InitiateElection(e) => self.on_event(e),
                Inbound::RequestVote(e) => self.on_event(e),
                Inbound::Vote(e) => self.on_event(e),
                Inbound::AppendEntries(e) => self.on_event(e),
                Inbound::AppendEntriesResponse(e) => self.on_event(e),
                Inbound::Shutdown => break,
            }
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<MakeRequest> for Runner<Provider> {
    fn on_event(&mut self, event: MakeRequest) {
        debug!("[Node {}] <- {:?}", self.node_id, event);
        if self.role != Role::Leader {
            if let Some(leader_id) = self.leader_id {
                debug!("[Node {}] forward to leader", self.node_id);
                self.send_to(leader_id, event);
            } else {
                // TODO: buffer the request until a leader is elected
                debug!("[Node {}] leader not elected", self.node_id);
            }
            return;
        }
        self.log.push(LogEntry {
            term: self.current_term,
            request: event.request,
        });
        self.broadcast_heartbeat();
    }
}

impl<Provider: DependencyProvider> OnEvent<InitiateHeartbeat> for Runner<Provider> {
    fn on_event(&mut self, event: InitiateHeartbeat) {
        debug!("[Node {}] <- {:?}", self.node_id, event);
        let node_id = event.0;
        if self.role == Role::Leader {
            let prev_log_index = self.next_index[&node_id] - 1;
            let prev_log_term = self.get_log_term(prev_log_index);
            self.send_to(
                node_id,
                AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id,
                    prev_log_index: prev_log_index,
                    prev_log_term: prev_log_term,
                    entries: self.log[prev_log_index as usize..].to_vec(),
                    leader_commit: self.commit_index,
                },
            );
            self.start_heartbeat_timer(node_id);
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<InitiateElection> for Runner<Provider> {
    fn on_event(&mut self, event: InitiateElection) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        self.become_candidate();
        let last_log_index = self.get_last_log_index();
        let rv = RequestVote {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: last_log_index,
            last_log_term: self.get_log_term(last_log_index),
        };
        for node_id in 1..=self.num_nodes {
            if node_id != self.node_id {
                self.send_to(node_id, rv.clone());
            }
        }
        self.start_election_timer();
    }
}

impl<Provider: DependencyProvider> OnEvent<RequestVote> for Runner<Provider> {
    fn on_event(&mut self, event: RequestVote) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term < self.current_term {
            debug!(
                "[Node {}] rejecting event, term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );

            let reply = Vote {
                term: self.current_term,
                voter_id: self.node_id,
                granted: false,
            };
            self.send_to(event.candidate_id, reply);
            return;
        }

        if event.term > self.current_term {
            self.become_follower(event.term);
        }

        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_log_term(last_log_index);
        let log_ok = event.last_log_term > last_log_term
            || (event.last_log_term == last_log_term && event.last_log_index >= last_log_index);

        let grant_vote =
            log_ok && (self.voted_for.is_none() || self.voted_for == Some(event.candidate_id));
        if grant_vote {
            self.voted_for = Some(event.candidate_id);
            self.start_election_timer();
        }

        let reply = Vote {
            term: self.current_term,
            voter_id: self.node_id,
            granted: grant_vote,
        };

        self.send_to(event.candidate_id, reply);
    }
}

impl<Provider: DependencyProvider> OnEvent<Vote> for Runner<Provider> {
    fn on_event(&mut self, event: Vote) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term > self.current_term {
            info!(
                "[Node {}] term {} > current_term {}",
                self.node_id, event.term, self.current_term
            );
            self.become_follower(event.term);
            self.start_election_timer();
            return;
        }

        if self.role != Role::Candidate || event.term != self.current_term {
            debug!(
                "[Node {}] dropping event, role {:?} current term {}",
                self.node_id, self.role, self.current_term
            );
            return;
        }

        if event.granted {
            self.votes_collected.insert(event.voter_id);
            debug!(
                "[Node {}] current votes {}",
                self.node_id,
                self.votes_collected.len()
            );
            if self.votes_collected.len() >= self.quorum as usize {
                info!(
                    "[Node {}] won election for term {}",
                    self.node_id, self.current_term
                );
                self.become_leader();
            }
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<AppendEntries> for Runner<Provider> {
    fn on_event(&mut self, event: AppendEntries) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term < self.current_term {
            debug!(
                "[Node {}] dropping event, term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );

            let resp = AppendEntriesResponse {
                node_id: self.node_id,
                term: self.current_term,
                prev_log_index: 0,
                success: false,
            };
            self.send_to(event.leader_id, resp);
            return;
        }

        let prev_log_term = self.get_log_term(event.prev_log_index);
        if event.prev_log_index != 0 && prev_log_term != event.prev_log_term {
            debug!(
                "[Node {}] log inconsistency at index {} term {}",
                self.node_id, event.prev_log_index, prev_log_term
            );
            let resp = AppendEntriesResponse {
                node_id: self.node_id,
                term: self.current_term,
                prev_log_index: 0,
                success: false,
            };
            self.send_to(event.leader_id, resp);
            return;
        }

        self.current_term = event.term;
        self.leader_id = Some(event.leader_id);

        match self.role {
            Role::Leader | Role::Candidate => {
                info!(
                    "[Node {}] lost election for term {}",
                    self.node_id, self.current_term
                );
                self.become_follower(self.current_term);
            }
            _ => {}
        }
        self.start_election_timer();

        self.log.truncate(event.prev_log_index as usize);
        self.log.extend(event.entries.into_iter());

        self.commit_index = std::cmp::max(self.commit_index, event.leader_commit);
        self.advance_commit_index();

        let resp = AppendEntriesResponse {
            node_id: self.node_id,
            term: self.current_term,
            prev_log_index: self.log.len() as u64,
            success: true,
        };
        self.send_to(event.leader_id, resp);
    }
}

impl<Provider: DependencyProvider> OnEvent<AppendEntriesResponse> for Runner<Provider> {
    fn on_event(&mut self, event: AppendEntriesResponse) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term > self.current_term {
            info!(
                "[Node {}] term {} > current_term {}",
                self.node_id, event.term, self.current_term
            );
            self.become_follower(event.term);
            self.start_election_timer();
            return;
        }

        if event.term < self.current_term {
            debug!(
                "[Node {}] dropping event, term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );
            return;
        }

        if self.role != Role::Leader {
            debug!("[Node {}] dropping event, node is not leader", self.node_id);
            return;
        }

        if event.success {
            self.next_backoff_index.insert(event.node_id, 1);
            self.next_index
                .insert(event.node_id, event.prev_log_index + 1);
            self.match_index.insert(event.node_id, event.prev_log_index);
            self.try_advance_commit_index();
        } else {
            let backoff = self
                .next_backoff_index
                .get(&event.node_id)
                .unwrap_or(&1)
                .saturating_mul(2);
            self.next_backoff_index.insert(event.node_id, backoff);
            self.next_index.entry(event.node_id).and_modify(|idx| {
                *idx = idx.checked_sub(backoff).unwrap_or_default().max(1);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future;

    struct NoopTimer;

    impl timer::Timer for NoopTimer {}

    #[derive(Clone)]
    struct NoopTimerService;

    impl timer::TimerService for NoopTimerService {
        type Timer = NoopTimer;
        fn create(
            &self,
            _duration: tokio::time::Duration,
            _f: impl future::Future<Output = ()> + Send + 'static,
        ) -> Self::Timer {
            NoopTimer
        }
    }

    struct TestDependencyProvider;

    impl DependencyProvider for TestDependencyProvider {
        type TimerService = NoopTimerService;
    }

    #[tokio::test]
    async fn test_single_node_elect_itself_as_leader() {
        let num_nodes = 1;
        let node_id = 1;
        let quorum = 1;
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        let (node, join_handle) = Node::<TestDependencyProvider>::new(
            node_id,
            num_nodes,
            outbound_tx,
            NoopTimerService,
            quorum,
        );
        node.recv(Inbound::InitiateElection(InitiateElection));
        let mut sent_to_nodes = HashSet::new();
        while let Ok(Some((peer_id, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            sent_to_nodes.insert(peer_id);
            if let Outbound::AppendEntries(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.leader_id, 1);
                assert_eq!(event.entries.len(), 0);
            } else {
                panic!("unexpected event");
            }
            if sent_to_nodes.len() == num_nodes as usize - 1 {
                break;
            }
        }
        node.shutdown();
        join_handle.wait().await;
    }

    #[tokio::test]
    async fn test_three_node_cluster_elect_leader() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));

        let mut request_vote_recv = HashSet::new();
        for _ in 0..num_nodes - 1 {
            let (peer_id, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::RequestVote(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.candidate_id, 1);
                request_vote_recv.insert(peer_id);
            } else {
                panic!("unexpected event");
            }
        }
        assert_eq!(request_vote_recv, HashSet::from([2, 3]));

        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }

        let mut heartbeat_recv = HashSet::new();
        for _ in 0..num_nodes - 1 {
            let (peer_id, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::AppendEntries(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.leader_id, 1);
                heartbeat_recv.insert(peer_id);
            } else {
                panic!("unexpected event");
            }
        }
        assert_eq!(heartbeat_recv, HashSet::from([2, 3]));

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_leader_election_failure() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));

        for _ in 0..num_nodes - 1 {
            let (_, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::RequestVote(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.candidate_id, 1);
            } else {
                panic!("unexpected event");
            }
        }

        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: 2,
            granted: false,
        }));
        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: 3,
            granted: false,
        }));

        assert!(
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .is_err()
        );

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_basic_log_replication() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));

        for _ in 0..num_nodes - 1 {
            let (_, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::RequestVote(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.candidate_id, 1);
            } else {
                panic!("unexpected event");
            }
        }

        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }

        for _ in 0..num_nodes - 1 {
            let (_, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::AppendEntries(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.leader_id, 1);
            } else {
                panic!("unexpected event");
            }
        }

        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: "test".to_string(),
            }));

        let mut log_replication_recv = HashSet::new();
        for _ in 0..num_nodes - 1 {
            let (peer_id, event) =
                tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if let Outbound::AppendEntries(event) = event {
                assert_eq!(event.term, 1);
                assert_eq!(event.leader_id, 1);
                assert_eq!(event.entries.len(), 1);
                assert_eq!(event.entries[0].request, "test");
                log_replication_recv.insert(peer_id);
            } else {
                panic!("unexpected event");
            }
        }
        assert_eq!(log_replication_recv, HashSet::from([2, 3]));

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_log_replication_with_inconsistencies() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        // Elect leader 1
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain RequestVotes
        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }
        // Leader sends initial heartbeats. next_index[2] is 1.
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain heartbeats

        // Now, let's add something to the leader's log.
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: "test".to_string(),
            }));
        // Leader sends AppendEntries. next_index[2] is now 2, prev_log_index is 1.
        // Let's drain these.
        let mut sent_to = HashSet::new();
        for _ in 0..num_nodes - 1 {
            let (peer_id, event) = outbound_rx.recv().await.unwrap();
            if let Outbound::AppendEntries(e) = event {
                if e.entries.len() == 1 {
                    sent_to.insert(peer_id);
                }
            }
        }
        assert_eq!(sent_to.len(), 2);

        // Now, we tell the leader that node 2 failed.
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                prev_log_index: 0, // This doesn't matter for the test
                success: false,
            }));

        // Now we trigger a heartbeat to node 2 to force a retry.
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

        // Check the retry AppendEntries
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer_id, 2);
        if let Outbound::AppendEntries(event) = event {
            assert_eq!(event.term, 1);
            // After failure, next_index[2] was 2. It gets decremented. Let's say to 1.
            // So prev_log_index should be 0.
            assert_eq!(event.prev_log_index, 0);
            // And it should contain the full log for that follower.
            assert_eq!(event.entries.len(), 1);
            assert_eq!(event.entries[0].request, "test");
        } else {
            panic!("unexpected event");
        }

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_candidate_receives_append_entries_and_becomes_follower() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        // 1. Node 1 becomes a candidate
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));
        // Drain RequestVotes
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        }

        // 2. Node 2 sends an AppendEntries to Node 1 (simulating it won the election)
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }));

        // 3. Verify Node 1 sends a success response to Node 2
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer_id, 2);
        if let Outbound::AppendEntriesResponse(resp) = event {
            assert!(resp.success);
            assert_eq!(resp.term, 1);
        } else {
            panic!("unexpected event, expected AppendEntriesResponse");
        }

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_follower_forwards_request_to_leader() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        // 1. Elect leader 1
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain RequestVotes
        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain heartbeats

        // 2. Manually send AppendEntries to follower 2 to ensure it knows the leader
        nodes
            .get(&2)
            .unwrap()
            .0
            .recv(Inbound::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }));
        // Drain the response
        let _ = outbound_rx.recv().await;

        // 3. Send a request to a follower (node 2)
        nodes
            .get(&2)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: "test".to_string(),
            }));

        // 4. Verify the request is forwarded to the leader (node 1)
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer_id, 1);
        if let Outbound::MakeRequest(req) = event {
            assert_eq!(req.request, "test");
        } else {
            panic!("unexpected event, expected MakeRequest");
        }

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_reject_vote_with_stale_term() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        // 1. Elect leader 1 to establish term 1
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain RequestVotes
        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain heartbeats

        // 2. Node 2 sends a RequestVote to Node 1 with a stale term
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::RequestVote(RequestVote {
                term: 0,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }));

        // 3. Verify Node 1 rejects the vote
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer_id, 2);
        if let Outbound::Vote(vote) = event {
            assert!(!vote.granted);
            assert_eq!(vote.term, 1);
        } else {
            panic!("unexpected event, expected Vote");
        }

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }

    #[tokio::test]
    async fn test_reject_append_entries_with_stale_term() {
        let num_nodes = 3;
        let quorum = 2;
        let mut nodes = HashMap::new();
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=num_nodes {
            let (node, join_handle) = Node::<TestDependencyProvider>::new(
                i,
                num_nodes,
                outbound_tx.clone(),
                NoopTimerService,
                quorum,
            );
            nodes.insert(i, (node, join_handle));
        }

        // 1. Elect leader 1 to establish term 1
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::InitiateElection(InitiateElection));
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain RequestVotes
        for i in 2..=num_nodes {
            nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }
        for _ in 0..num_nodes - 1 {
            let _ = outbound_rx.recv().await;
        } // Drain heartbeats

        // 2. Node 2 sends an AppendEntries to Node 1 with a stale term
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::AppendEntries(AppendEntries {
                term: 0,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }));

        // 3. Verify Node 1 rejects the request
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(peer_id, 2);
        if let Outbound::AppendEntriesResponse(resp) = event {
            assert!(!resp.success);
            assert_eq!(resp.term, 1);
        } else {
            panic!("unexpected event, expected AppendEntriesResponse");
        }

        for (node, join_handle) in nodes.into_values() {
            node.shutdown();
            join_handle.wait().await;
        }
    }
}
