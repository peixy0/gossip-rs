use crate::event::*;
use crate::timer;
use crate::timer::TimerService;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::*;

#[derive(PartialEq, Debug, Clone, Copy)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct InitiateHeartbeat;
struct InitiateElection;

enum Inbound {
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
        outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
        timer_service: Provider::TimerService,
        quorum: usize,
    ) -> (Self, JoinHandle) {
        let (runner_tx, runner_rx) = tokio::sync::mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let runner = Runner::<Provider>::new(
            node_id,
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

trait EventLoop<Event> {
    async fn run_eventloop(self);
}

trait OnEvent<T> {
    fn on_event(&mut self, event: T);
}

struct Runner<Provider: DependencyProvider> {
    node_id: u64,
    rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
    timer_service: Provider::TimerService,

    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    log: HashMap<u64, String>,

    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    heartbeat_timeout_base_in_ms: u64,
    heartbeat_timer:
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,
    election_timeout_base_in_ms: u64,
    election_timer:
        Option<<<Provider as DependencyProvider>::TimerService as timer::TimerService>::Timer>,

    votes_collected: HashSet<u64>,
    quorum: usize,
}

impl<Provider: DependencyProvider> Runner<Provider> {
    fn new(
        node_id: u64,
        rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
        internal_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
        outbound_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
        timer_service: Provider::TimerService,
        quorum: usize,
    ) -> Self {
        Self {
            node_id,
            rx,
            internal_tx,
            outbound_tx,
            timer_service,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: HashMap::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            heartbeat_timeout_base_in_ms: 1000,
            heartbeat_timer: None,
            election_timeout_base_in_ms: 3000,
            election_timer: None,
            votes_collected: HashSet::new(),
            quorum,
        }
    }

    fn start_heartbeat_timer(&mut self) {
        let tx = self.internal_tx.clone();
        let timeout = tokio::time::Duration::from_millis(self.heartbeat_timeout_base_in_ms);
        self.heartbeat_timer = Some(self.timer_service.create(timeout, async move {
            let _ = tx.send(Inbound::InitiateHeartbeat(InitiateHeartbeat));
        }));
    }

    fn stop_heartbeat_timer(&mut self) {
        self.heartbeat_timer = None;
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
        self.current_term = term;
        self.voted_for = None;
        self.stop_heartbeat_timer();
        self.start_election_timer();
        info!(
            "[Node {}] Role {:?} -> {:?}",
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
        self.stop_election_timer();
        self.start_election_timer();
        info!(
            "[Node {}] Role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }

    fn become_leader(&mut self) {
        let prev_role = self.role;
        self.role = Role::Leader;
        self.next_index.clear();
        self.match_index.clear();
        self.stop_election_timer();
        self.start_heartbeat_timer();
        info!(
            "[Node {}] Role {:?} -> {:?}",
            self.node_id, prev_role, self.role
        );
    }
}

impl<Provider: DependencyProvider> EventLoop<Inbound> for Runner<Provider> {
    async fn run_eventloop(mut self) {
        self.become_follower(self.current_term);
        while let Some(event) = self.rx.recv().await {
            match event {
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

impl<Provider: DependencyProvider> OnEvent<InitiateHeartbeat> for Runner<Provider> {
    fn on_event(&mut self, _event: InitiateHeartbeat) {
        debug!("[Node {}] InitiateHeartbeat received", self.node_id);
        if self.role == Role::Leader {
            let _ = self
                .outbound_tx
                .send(Outbound::AppendEntries(AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id,
                }));
            self.start_heartbeat_timer();
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<InitiateElection> for Runner<Provider> {
    fn on_event(&mut self, _event: InitiateElection) {
        debug!("[Node {}] InitiateElection received", self.node_id);

        self.become_candidate();
        let rv = RequestVote {
            term: self.current_term,
            candidate_id: self.node_id,
            last_log_index: 0,
            last_log_term: 0,
        };
        let _ = self.outbound_tx.send(Outbound::RequestVote(rv));
        self.start_election_timer();
    }
}

impl<Provider: DependencyProvider> OnEvent<RequestVote> for Runner<Provider> {
    fn on_event(&mut self, event: RequestVote) {
        debug!("[Node {}] RequestVote received: {:?}", self.node_id, event);

        if event.term < self.current_term {
            let reply = Vote {
                term: self.current_term,
                voter_id: self.node_id,
                granted: false,
            };
            let _ = self
                .outbound_tx
                .send(Outbound::Vote(event.candidate_id, reply));
            return;
        }

        if event.term > self.current_term {
            self.current_term = event.term;
            self.voted_for = None;
            self.role = Role::Follower;
        }

        let can_grant = match self.voted_for {
            None => true,
            Some(v) => v == event.candidate_id,
        };

        let reply = Vote {
            term: self.current_term,
            voter_id: self.node_id,
            granted: can_grant,
        };

        if can_grant {
            self.voted_for = Some(event.candidate_id);
            self.start_election_timer();
        }

        let _ = self
            .outbound_tx
            .send(Outbound::Vote(event.candidate_id, reply));
    }
}

impl<Provider: DependencyProvider> OnEvent<Vote> for Runner<Provider> {
    fn on_event(&mut self, event: Vote) {
        debug!("[Node {}] Vote received: {:?}", self.node_id, event);
        if self.role != Role::Candidate || event.term != self.current_term {
            return;
        }

        if event.granted {
            self.votes_collected.insert(event.voter_id);
            if self.votes_collected.len() >= self.quorum {
                info!(
                    "[Node {}] Won election for term {}",
                    self.node_id, self.current_term
                );
                self.become_leader();
            }
        }
    }
}

impl<Provider: DependencyProvider> OnEvent<AppendEntries> for Runner<Provider> {
    fn on_event(&mut self, event: AppendEntries) {
        debug!(
            "[Node {}] AppendEntries received: {:?}",
            self.node_id, event
        );

        if event.term < self.current_term {
            debug!(
                "[Node {}] Reject AppendEntries term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );

            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
            let _ = self
                .outbound_tx
                .send(Outbound::AppendEntriesResponse(event.leader_id, resp));

            return;
        }

        if event.term > self.current_term {
            info!(
                "[Node {}] AppendEntries term {} > current_term {} becoming follower",
                self.node_id, event.term, self.current_term
            );
            self.become_follower(event.term);
        } else {
            match self.role {
                Role::Leader | Role::Candidate => {
                    info!(
                        "[Node {}] AppendEntries in same term {} becoming follower",
                        self.node_id, event.term
                    );
                    self.become_follower(self.current_term);
                }
                Role::Follower => {
                    self.start_election_timer();
                }
            }
        }

        let resp = AppendEntriesResponse {
            term: self.current_term,
            success: true,
        };
        let _ = self
            .outbound_tx
            .send(Outbound::AppendEntriesResponse(event.leader_id, resp));
    }
}

impl<Provider: DependencyProvider> OnEvent<AppendEntriesResponse> for Runner<Provider> {
    fn on_event(&mut self, event: AppendEntriesResponse) {
        debug!(
            "[Node {}] AppendEntriesResponse received: {:?}",
            self.node_id, event
        );

        if event.term > self.current_term {
            info!(
                "[Node {}] AppendEntriesResponse term {} > current_term {} becoming follower",
                self.node_id, event.term, self.current_term
            );
            self.become_follower(event.term);
            return;
        }

        if event.term < self.current_term {
            debug!(
                "[Node {}] Drop AppendEntriesResponse term {} < current_term {}",
                self.node_id, event.term, self.current_term
            );
            return;
        }

        if self.role != Role::Leader {
            debug!(
                "[Node {}] Drop AppendEntriesResponse because node is not leader",
                self.node_id
            );
            return;
        }
    }
}
