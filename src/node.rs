use crate::event::*;
use crate::timer;
use crate::timer::TimerService;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::*;

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub(crate) struct InitiateHeartbeat(pub(crate) u64);

#[derive(Debug)]
pub(crate) struct InitiateElection;

pub(crate) enum Inbound {
    MakeRequest(MakeRequest),
    InitiateHeartbeat(InitiateHeartbeat),
    InitiateElection(InitiateElection),
    StateUpdateRequest(StateUpdateRequest),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResponse(InstallSnapshotResponse),
    RequestPreVote(RequestPreVote),
    PreVote(PreVote),
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
    node_id: u64,
    tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
    _marker: std::marker::PhantomData<Provider>,
}

impl<Provider: DependencyProvider + 'static> Node<Provider> {
    pub fn new(
        node_id: u64,
        node_pool: Vec<u64>,
        outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
        timer_service: Provider::TimerService,
        quorum: u64,
        heartbeat_timeout: tokio::time::Duration,
        election_timeout_base: tokio::time::Duration,
    ) -> (Self, JoinHandle) {
        let (runner_tx, runner_rx) = tokio::sync::mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let runner = Runner::<Provider>::new(
            node_id,
            node_pool,
            runner_rx,
            runner_tx.clone(),
            outbound_tx,
            timer_service,
            quorum,
            heartbeat_timeout,
            election_timeout_base,
        );
        tokio::spawn(async move {
            runner.run_eventloop().await;
            let _ = exit_tx.send(());
        });
        (
            Self {
                node_id,
                tx: runner_tx,
                _marker: std::marker::PhantomData,
            },
            JoinHandle(exit_rx),
        )
    }

    pub fn get_id(&self) -> u64 {
        self.node_id
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(Inbound::Shutdown);
    }
}

impl<Provider> Recv<RequestPreVote> for Node<Provider> {
    fn recv(&self, event: RequestPreVote) {
        let _ = self.tx.send(Inbound::RequestPreVote(event));
    }
}

impl<Provider> Recv<PreVote> for Node<Provider> {
    fn recv(&self, event: PreVote) {
        let _ = self.tx.send(Inbound::PreVote(event));
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

impl<Provider> Recv<InstallSnapshot> for Node<Provider> {
    fn recv(&self, event: InstallSnapshot) {
        let _ = self.tx.send(Inbound::InstallSnapshot(event));
    }
}

impl<Provider> Recv<InstallSnapshotResponse> for Node<Provider> {
    fn recv(&self, event: InstallSnapshotResponse) {
        let _ = self.tx.send(Inbound::InstallSnapshotResponse(event));
    }
}

impl<Provider> Recv<MakeRequest> for Node<Provider> {
    fn recv(&self, event: MakeRequest) {
        let _ = self.tx.send(Inbound::MakeRequest(event));
    }
}

impl<Provider> Recv<StateUpdateRequest> for Node<Provider> {
    fn recv(&self, event: StateUpdateRequest) {
        let _ = self.tx.send(Inbound::StateUpdateRequest(event));
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
    node_pool: Vec<u64>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
    timer_service: Provider::TimerService,

    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    last_included_index: u64,
    last_included_term: u64,
    snapshot: Option<Vec<u8>>,

    next_backoff_index: HashMap<u64, u64>,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    heartbeat_timeout: tokio::time::Duration,
    heartbeat_timer: HashMap<
        u64,
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,
    >,
    election_timeout_base: tokio::time::Duration,
    election_timer:
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,

    votes_collected: HashSet<u64>,
    pre_votes_collected: HashSet<u64>,
    quorum: u64,
}

impl<Provider: DependencyProvider> Runner<Provider> {
    fn new(
        node_id: u64,
        node_pool: Vec<u64>,
        rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
        internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
        outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
        timer_service: Provider::TimerService,
        quorum: u64,
        heartbeat_timeout: tokio::time::Duration,
        election_timeout_base: tokio::time::Duration,
    ) -> Self {
        Self {
            node_id,
            node_pool,
            rx,
            internal_tx,
            outbound_tx,
            timer_service,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_included_index: 0,
            last_included_term: 0,
            snapshot: None,
            commit_index: 0,
            last_applied: 0,
            next_backoff_index: HashMap::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            heartbeat_timeout,
            heartbeat_timer: HashMap::new(),
            election_timeout_base,
            election_timer: None,
            votes_collected: HashSet::new(),
            pre_votes_collected: HashSet::new(),
            quorum,
        }
    }

    fn send(&self, event: impl Into<Outbound>) {
        let msg = event.into();
        debug!("[Node {}] -> {:?}", self.node_id, msg);
        let _ = self.outbound_tx.send(msg);
    }

    fn send_to(&self, peer_id: u64, event: impl Into<Protocol>) {
        let msg = event.into();
        self.send(Outbound::MessageToPeer(peer_id, msg));
    }

    fn get_last_log_index(&self) -> u64 {
        self.last_included_index + self.log.len() as u64
    }

    fn get_log_term(&self, log_index: u64) -> u64 {
        if log_index == 0 {
            return 0;
        }
        if log_index < self.last_included_index {
            return self.last_included_term;
        }

        let relative = log_index.saturating_sub(self.last_included_index);
        if relative == 0 {
            return self.last_included_term;
        }
        if relative as usize > self.log.len() {
            return 0;
        }
        self.log
            .get(relative as usize - 1)
            .map(|e| e.term)
            .unwrap_or(0)
    }

    fn start_heartbeat_timer(&mut self, node_id: u64) {
        let tx = self.internal_tx.clone();
        let timeout = self.heartbeat_timeout;
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
        let r = rand::random_range(0..self.election_timeout_base.as_millis() as u64);
        let timeout = self.election_timeout_base + tokio::time::Duration::from_millis(r);
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
            if self.last_applied < self.last_included_index {
                self.last_applied = self.last_included_index;
                continue;
            }

            let log_index = (self.last_applied - self.last_included_index) as usize;

            if let Some(entry) = self.log.get(log_index) {
                self.send(CommitNotification {
                    node_id: self.node_id,
                    index: self.last_applied + 1,
                    term: entry.term,
                    request: entry.request.clone(),
                });
                self.last_applied += 1;
            } else {
                break;
            }
        }
    }

    fn become_pre_candidate(&mut self) {
        let prev_role = self.role;
        self.role = Role::PreCandidate;
        self.pre_votes_collected.clear();
        self.pre_votes_collected.insert(self.node_id);
        self.start_election_timer();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn become_candidate(&mut self) {
        let prev_role = self.role;
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.node_id);
        self.votes_collected.clear();
        self.votes_collected.insert(self.node_id);
        self.start_election_timer();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn become_leader(&mut self) {
        let prev_role = self.role;
        self.role = Role::Leader;
        self.next_backoff_index.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.stop_election_timer();
        self.send(NetworkUpdateInd {
            leader_id: self.node_id,
        });

        for node_id in &self.node_pool {
            if *node_id != self.node_id {
                self.next_backoff_index.insert(*node_id, 1);
                self.next_index
                    .insert(*node_id, self.get_last_log_index() + 1);
                self.match_index.insert(*node_id, 0);
            }
        }
        self.broadcast_heartbeat();

        self.maybe_advance_commit_index();
        info!(
            "[Node {}] role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn broadcast_heartbeat(&mut self) {
        for node_id in &self.node_pool {
            if *node_id != self.node_id {
                let _ = self
                    .internal_tx
                    .send(Inbound::InitiateHeartbeat(InitiateHeartbeat(*node_id)));
            }
        }
    }

    fn maybe_advance_commit_index(&mut self) {
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
                Inbound::RequestPreVote(e) => self.on_event(e),
                Inbound::PreVote(e) => self.on_event(e),
                Inbound::RequestVote(e) => self.on_event(e),
                Inbound::Vote(e) => self.on_event(e),
                Inbound::AppendEntries(e) => self.on_event(e),
                Inbound::AppendEntriesResponse(e) => self.on_event(e),
                Inbound::StateUpdateRequest(e) => self.on_event(e),
                Inbound::InstallSnapshot(e) => self.on_event(e),
                Inbound::InstallSnapshotResponse(e) => self.on_event(e),
                Inbound::Shutdown => break,
            }
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<MakeRequest> for Runner<Provider> {
    fn on_event(&mut self, event: MakeRequest) {
        debug!("[Node {}] <- {:?}", self.node_id, event);
        if self.role != Role::Leader {
            warn!("[Node {}] dropping event, not leader", self.node_id);
            return;
        }
        self.log.push(LogEntry {
            term: self.current_term,
            request: event.request,
        });
        self.broadcast_heartbeat();
        self.maybe_advance_commit_index();
    }
}

impl<Provider: DependencyProvider> OnEvent<InitiateHeartbeat> for Runner<Provider> {
    fn on_event(&mut self, event: InitiateHeartbeat) {
        debug!("[Node {}] <- {:?}", self.node_id, event);
        let node_id = event.0;
        if self.role == Role::Leader {
            let next_index = self.next_index[&node_id];

            // If next_index <= last_included_index, follower is too far behind
            // Send InstallSnapshot instead of AppendEntries
            if next_index <= self.last_included_index {
                debug!(
                    "[Node {}] follower {} too far behind (next_index={}, last_included_index={}), sending InstallSnapshot",
                    self.node_id, node_id, next_index, self.last_included_index
                );
                if let Some(snapshot_data) = &self.snapshot {
                    self.send_to(
                        node_id,
                        InstallSnapshot {
                            term: self.current_term,
                            leader_id: self.node_id,
                            last_included_index: self.last_included_index,
                            last_included_term: self.last_included_term,
                            data: snapshot_data.clone(),
                        },
                    );
                } else {
                    warn!("[Node {}] no snapshot available", self.node_id);
                }
                self.start_heartbeat_timer(node_id);
                return;
            }

            let prev_log_index = next_index - 1;
            let prev_log_term = self.get_log_term(prev_log_index);
            let start_offset = prev_log_index.saturating_sub(self.last_included_index) as usize;
            let entries = self.log[start_offset..].to_vec();

            self.send_to(
                node_id,
                AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
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

        self.become_pre_candidate();

        if self.pre_votes_collected.len() >= self.quorum as usize {
            info!(
                "[Node {}] won pre-vote for term {} (single-node cluster), proceeding to real election",
                self.node_id,
                self.current_term + 1
            );
            self.current_term += 1;
            self.voted_for = Some(self.node_id);
            self.become_leader();
            return;
        }

        let last_log_index = self.get_last_log_index();
        let rv = RequestPreVote {
            term: self.current_term + 1,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term: self.get_log_term(last_log_index),
        };
        for node_id in &self.node_pool {
            if *node_id != self.node_id {
                self.send_to(*node_id, rv.clone());
            }
        }
        self.start_election_timer();
    }
}

impl<Provider: DependencyProvider> OnEvent<RequestPreVote> for Runner<Provider> {
    fn on_event(&mut self, event: RequestPreVote) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        // Pre-vote doesn't update term - that's the key difference
        // We only grant pre-vote if:
        // 1. The candidate's term is >= our current term
        // 2. The candidate's log is at least as up-to-date as ours
        // 3. We haven't heard from a leader recently (implicitly checked via role)

        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_log_term(last_log_index);
        let log_ok = event.last_log_term > last_log_term
            || (event.last_log_term == last_log_term && event.last_log_index >= last_log_index);

        let grant_pre_vote = log_ok && event.term > self.current_term;

        let reply = PreVote {
            term: event.term,
            voter_id: self.node_id,
            granted: grant_pre_vote,
        };

        self.send_to(event.candidate_id, reply);
    }
}

impl<Provider: DependencyProvider> OnEvent<PreVote> for Runner<Provider> {
    fn on_event(&mut self, event: PreVote) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term > self.current_term + 1 {
            info!(
                "[Node {}] term {} > current_term {}",
                self.node_id, event.term, self.current_term
            );
            self.become_follower(event.term);
            self.start_election_timer();
            return;
        }

        if event.term <= self.current_term {
            debug!(
                "[Node {}] dropping event, term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );
            return;
        }

        if self.role != Role::PreCandidate {
            debug!(
                "[Node {}] dropping event, role {:?}",
                self.node_id, self.role
            );
            return;
        }

        if event.granted {
            self.pre_votes_collected.insert(event.voter_id);
            debug!(
                "[Node {}] current pre-votes {}",
                self.node_id,
                self.pre_votes_collected.len()
            );
            if self.pre_votes_collected.len() >= self.quorum as usize {
                self.become_candidate();
                info!(
                    "[Node {}] won pre-vote, starting real election for term {}",
                    self.node_id, self.current_term
                );

                let last_log_index = self.get_last_log_index();
                let rv = RequestVote {
                    term: self.current_term,
                    candidate_id: self.node_id,
                    last_log_index,
                    last_log_term: self.get_log_term(last_log_index),
                };
                for node_id in &self.node_pool {
                    if *node_id != self.node_id {
                        self.send_to(*node_id, rv.clone());
                    }
                }
            }
        }
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
                "[Node {}] rejecting event, term {} < current_term {}",
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

        self.current_term = event.term;

        match self.role {
            Role::Leader | Role::Candidate | Role::PreCandidate => {
                info!(
                    "[Node {}] stepping down from {:?} for term {}",
                    self.node_id, self.role, self.current_term
                );
                self.become_follower(self.current_term);
            }
            _ => {}
        }
        self.start_election_timer();

        if event.prev_log_index < self.last_included_index {
            debug!(
                "[Node {}] rejecting event, prev_log_index {} < last_included_index {}",
                self.node_id, event.prev_log_index, self.last_included_index
            );
            let resp = AppendEntriesResponse {
                node_id: self.node_id,
                term: self.current_term,
                prev_log_index: self.last_included_index,
                success: false,
            };
            self.send_to(event.leader_id, resp);
            return;
        }

        let prev_log_term = self.get_log_term(event.prev_log_index);
        if prev_log_term != event.prev_log_term {
            debug!(
                "[Node {}] log inconsistency at index {} term {}",
                self.node_id, event.prev_log_index, prev_log_term
            );
            let resp = AppendEntriesResponse {
                node_id: self.node_id,
                term: self.current_term,
                prev_log_index: event.prev_log_index - 1,
                success: false,
            };
            self.send_to(event.leader_id, resp);
            return;
        }

        let truncate_from = (event.prev_log_index - self.last_included_index) as usize;
        if truncate_from < self.log.len() {
            self.log.drain(truncate_from..);
        }
        self.log.extend(event.entries);

        self.commit_index = std::cmp::max(self.commit_index, event.leader_commit);
        self.advance_commit_index();

        let resp = AppendEntriesResponse {
            node_id: self.node_id,
            term: self.current_term,
            prev_log_index: self.get_last_log_index(),
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
            debug!("[Node {}] dropping event, not leader", self.node_id);
            return;
        }

        if event.success {
            self.next_backoff_index.insert(event.node_id, 1);
            self.next_index
                .insert(event.node_id, event.prev_log_index + 1);
            self.match_index.insert(event.node_id, event.prev_log_index);
            self.maybe_advance_commit_index();
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
            let _ = self
                .internal_tx
                .send(Inbound::InitiateHeartbeat(InitiateHeartbeat(event.node_id)));
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<StateUpdateRequest> for Runner<Provider> {
    fn on_event(&mut self, event: StateUpdateRequest) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.included_index <= self.last_included_index {
            warn!(
                "[Node {}] dropping event, included_index {} < last_included_index {}",
                self.node_id, event.included_index, self.last_included_index
            );
            return;
        }

        if event.included_index > self.commit_index {
            warn!(
                "[Node {}] dropping event, included_index {} > commit_index {}",
                self.node_id, event.included_index, self.commit_index
            );
            return;
        }

        let remove = (event.included_index - self.last_included_index) as usize;
        if remove > self.log.len() {
            return;
        }

        self.last_included_term = self.get_log_term(event.included_index);
        self.last_included_index = event.included_index;
        self.log.drain(0..remove);
        self.snapshot = Some(event.data);

        self.send(StateUpdateResponse {
            node_id: self.node_id,
            included_index: self.last_included_index,
            included_term: self.last_included_term,
        });
    }
}

impl<Provider: DependencyProvider> OnEvent<InstallSnapshot> for Runner<Provider> {
    fn on_event(&mut self, event: InstallSnapshot) {
        debug!("[Node {}] <- {:?}", self.node_id, event);

        if event.term < self.current_term {
            debug!(
                "[Node {}] dropping event, term {} < current {}",
                self.node_id, event.term, self.current_term
            );
            let resp = InstallSnapshotResponse {
                node_id: self.node_id,
                term: self.current_term,
            };
            self.send_to(event.leader_id, resp);
            return;
        }

        if event.term > self.current_term {
            self.become_follower(event.term);
        }
        self.start_election_timer();

        if self.commit_index < self.last_included_index {
            self.commit_index = self.last_included_index;
            self.last_applied = self.last_included_index;
        }

        self.last_included_term = event.last_included_term;
        self.last_included_index = event.last_included_index;
        self.snapshot = Some(event.data.clone());
        self.log.clear();

        let state_update = StateUpdateCommand {
            node_id: self.node_id,
            included_index: event.last_included_index,
            included_term: event.last_included_term,
            data: event.data.clone(),
        };
        self.send(state_update);
        let resp = InstallSnapshotResponse {
            node_id: self.node_id,
            term: self.current_term,
        };
        self.send_to(event.leader_id, resp);
    }
}

impl<Provider: DependencyProvider> OnEvent<InstallSnapshotResponse> for Runner<Provider> {
    fn on_event(&mut self, event: InstallSnapshotResponse) {
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

        if self.role != Role::Leader {
            debug!("[Node {}] dropping event, not leader", self.node_id);
            return;
        }

        self.match_index
            .insert(event.node_id, self.last_included_index);
        self.next_index
            .insert(event.node_id, self.last_included_index + 1);
        self.next_backoff_index.insert(event.node_id, 1);
        self.maybe_advance_commit_index();
        let _ = self
            .internal_tx
            .send(Inbound::InitiateHeartbeat(InitiateHeartbeat(event.node_id)));
    }
}
