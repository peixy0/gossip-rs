use crate::timer;

use rand::Rng;
use tracing::*;

#[derive(PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct InitiateHeartbeat;

struct InitiateElection;

enum Event {
    InitiateHeartbeat(InitiateHeartbeat),
    InitiateElection(InitiateElection),
    Shutdown,
}

pub struct JoinHandle(tokio::sync::oneshot::Receiver<()>);

impl JoinHandle {
    pub async fn wait(self) {
        let _ = self.0.await;
    }
}

#[derive(Clone)]
pub struct Node {
    tx: tokio::sync::mpsc::UnboundedSender<Event>,
}

impl Node {
    pub fn new() -> (Self, JoinHandle) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let mut runner = Runner::new(tx.clone(), rx);
        tokio::spawn(async move {
            runner.run_eventloop().await;
            let _ = exit_tx.send(());
        });
        (Self { tx }, JoinHandle(exit_rx))
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(Event::Shutdown);
    }
}

trait EventLoop<Event> {
    async fn run_eventloop(&mut self);
}

trait OnEvent<T> {
    fn on_event(&mut self, event: T);
}

struct Runner {
    tx: tokio::sync::mpsc::UnboundedSender<Event>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    role: Role,
    heartbeat_timeout_base_in_ms: u64,
    heartbeat_timer: Option<timer::Timer>,
    election_timeout_base_in_ms: u64,
    election_timer: Option<timer::Timer>,
}

impl Runner {
    fn new(
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
        rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    ) -> Self {
        Self {
            tx,
            rx,
            role: Role::Follower,
            heartbeat_timeout_base_in_ms: 100,
            heartbeat_timer: None,
            election_timeout_base_in_ms: 300,
            election_timer: None,
        }
    }

    fn get_heartbeat_timeout(&self) -> tokio::time::Duration {
        tokio::time::Duration::from_millis(self.heartbeat_timeout_base_in_ms)
    }

    fn start_heartbeat_timer(&mut self) {
        let tx = self.tx.clone();
        let timeout = self.get_heartbeat_timeout();
        self.heartbeat_timer = Some(timer::Timer::new(timeout, async move {
            let _ = tx.send(Event::InitiateHeartbeat(InitiateHeartbeat));
        }));
    }

    fn get_election_timeout(&self) -> tokio::time::Duration {
        let mut rng = rand::rng();
        let r = rng.random_range(0..self.election_timeout_base_in_ms);
        tokio::time::Duration::from_millis(self.election_timeout_base_in_ms + r)
    }

    fn start_election_timer(&mut self) {
        let tx = self.tx.clone();
        let timeout = self.get_election_timeout();
        self.election_timer = Some(timer::Timer::new(timeout, async move {
            let _ = tx.send(Event::InitiateElection(InitiateElection));
        }));
    }
}

impl EventLoop<Event> for Runner {
    async fn run_eventloop(&mut self) {
        self.start_heartbeat_timer();
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::InitiateHeartbeat(e) => {
                    self.on_event(e);
                }
                Event::InitiateElection(e) => {
                    self.on_event(e);
                }
                Event::Shutdown => {
                    break;
                }
            }
        }
    }
}

impl OnEvent<InitiateHeartbeat> for Runner {
    fn on_event(&mut self, _event: InitiateHeartbeat) {
        debug!("InitiateHeartbeat received");
        if self.role != Role::Leader {
            return;
        }
        self.start_heartbeat_timer();
    }
}

impl OnEvent<InitiateElection> for Runner {
    fn on_event(&mut self, _event: InitiateElection) {
        debug!("InitiateElection received");
        self.role = Role::Candidate;
        self.start_election_timer();
    }
}
