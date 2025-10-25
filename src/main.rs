use std::collections::HashMap;

use gossip_rs::event::*;
use gossip_rs::node::{DefaultDependencyProvider, Node};
use gossip_rs::timer;
use tracing::*;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const NUM_NODES: u64 = 5;
const QUORUM: u64 = (NUM_NODES / 2) + 1;
const MESSAGE_DROP_PROBABILITY: f64 = 0.33;
const MAX_MESSAGE_DELAY: tokio::time::Duration = tokio::time::Duration::from_millis(15);
const HEARTBEAT_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_millis(10);
const ELECTION_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_millis(30);
const REQUEST_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_millis(20);
const SNAPSHOT_THRESHOLD: u64 = 10;

/// Configuration for the Raft simulation
struct SimulationConfig {
    num_nodes: u64,
    quorum: u64,
    message_drop_probability: f64,
    max_message_delay: tokio::time::Duration,
    heatbeat_interval: tokio::time::Duration,
    election_interval: tokio::time::Duration,
    snapshot_threshold: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            num_nodes: NUM_NODES,
            quorum: QUORUM,
            message_drop_probability: MESSAGE_DROP_PROBABILITY,
            max_message_delay: MAX_MESSAGE_DELAY,
            heatbeat_interval: HEARTBEAT_INTERVAL,
            election_interval: ELECTION_INTERVAL,
            snapshot_threshold: SNAPSHOT_THRESHOLD,
        }
    }
}

/// Main simulation structure that manages the Raft cluster
struct RaftSimulation {
    config: SimulationConfig,
    nodes: HashMap<u64, Node<DefaultDependencyProvider>>,
    network_tx: tokio::sync::mpsc::UnboundedSender<Outbound>,
    request_tx: tokio::sync::mpsc::UnboundedSender<MakeRequest>,
}

impl RaftSimulation {
    /// Creates a new Raft simulation with default configuration
    fn new() -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<Outbound>,
        tokio::sync::mpsc::UnboundedReceiver<MakeRequest>,
    ) {
        Self::with_config(SimulationConfig::default())
    }

    /// Creates a new Raft simulation with custom configuration
    fn with_config(
        config: SimulationConfig,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<Outbound>,
        tokio::sync::mpsc::UnboundedReceiver<MakeRequest>,
    ) {
        let (network_tx, network_rx) = tokio::sync::mpsc::unbounded_channel::<Outbound>();
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<MakeRequest>();

        let simulation = Self {
            config,
            nodes: HashMap::new(),
            network_tx,
            request_tx,
        };

        (simulation, network_rx, request_rx)
    }

    /// Initializes all nodes in the cluster
    fn initialize_nodes(&mut self) {
        let node_pool: Vec<u64> = (1..=self.config.num_nodes).collect();

        for node_id in &node_pool {
            let timer_service = timer::DefaultTimerService;
            let (node, _handle) = Node::new(
                *node_id,
                node_pool.clone(),
                self.network_tx.clone(),
                timer_service,
                self.config.quorum,
                self.config.heatbeat_interval,
                self.config.election_interval,
            );
            self.nodes.insert(*node_id, node);
        }

        info!("Initialized {} nodes", self.config.num_nodes);
    }

    /// Prints simulation information
    fn print_info(&self) {
        info!(
            "Raft simulation starting with {} nodes. Quorum size is {}.",
            self.config.num_nodes, self.config.quorum
        );
        info!(
            "Network will randomly drop {:.0}% of messages and delay others up to {}ms.",
            self.config.message_drop_probability * 100.0,
            self.config.max_message_delay.as_millis(),
        );
        info!("Press Ctrl+C to stop.");
    }

    /// Shuts down all nodes gracefully
    fn shutdown(self) {
        info!("Shutdown signal received. Shutting down all nodes.");
        for (id, node) in self.nodes {
            info!("Sending shutdown to node {}", id);
            node.shutdown();
        }
        info!("Simulation finished.");
    }

    /// Simulates message dispatch with network unreliability
    fn maybe_dispatch_event<E>(
        node: Node<DefaultDependencyProvider>,
        event: E,
        drop_probability: f64,
        max_delay_ms: u64,
    ) where
        Node<DefaultDependencyProvider>: Recv<E>,
        E: Send + 'static,
    {
        if rand::random_bool(drop_probability) {
            debug!("[Network] dropping message to node {}", node.get_id());
            return;
        }

        tokio::spawn(async move {
            let delay = std::time::Duration::from_millis(rand::random_range(0..=max_delay_ms));
            tokio::time::sleep(delay).await;
            node.recv(event);
        });
    }
}

/// Network simulator that handles message routing and network unreliability
struct NetworkSimulator {
    nodes: HashMap<u64, Node<DefaultDependencyProvider>>,
    leader: Option<u64>,
    committed_count: HashMap<u64, u64>,
    config: SimulationConfig,
}

impl NetworkSimulator {
    fn new(nodes: HashMap<u64, Node<DefaultDependencyProvider>>, config: SimulationConfig) -> Self {
        Self {
            nodes,
            leader: None,
            committed_count: HashMap::new(),
            config,
        }
    }

    /// Handles a client request by forwarding it to the current leader
    fn handle_client_request(&self, event: MakeRequest) {
        if let Some(leader_id) = &self.leader {
            self.nodes[leader_id].recv(event);
        } else {
            warn!(
                "[Network] dropping request {}, leader not elected",
                String::from_utf8(event.request).unwrap()
            );
        }
    }

    /// Handles network update indication (leader change)
    fn handle_network_update(&mut self, e: NetworkUpdateInd) {
        self.leader = Some(e.leader_id);
    }

    /// Handles message routing to a peer node
    fn handle_message_to_peer(&self, dest_node_id: u64, protocol: Protocol) {
        let node = self.nodes[&dest_node_id].clone();
        let drop_prob = self.config.message_drop_probability;
        let max_delay = self.config.max_message_delay.as_millis() as u64;

        match protocol {
            Protocol::RequestPreVote(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::PreVote(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::RequestVote(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::Vote(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::AppendEntries(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::AppendEntriesResponse(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::InstallSnapshot(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
            Protocol::InstallSnapshotResponse(e) => {
                RaftSimulation::maybe_dispatch_event(node, e, drop_prob, max_delay)
            }
        }
    }

    /// Handles commit notifications and triggers snapshots when threshold is reached
    fn handle_commit_notification(&mut self, e: CommitNotification) {
        info!(
            "[Commit] node {} committed term {} index {} {}",
            e.node_id,
            e.term,
            e.index,
            String::from_utf8(e.request).unwrap()
        );

        let count = self.committed_count.entry(e.node_id).or_default();
        *count += 1;
        let current_commit_index = e.index;
        let node_id = e.node_id;

        if *count >= self.config.snapshot_threshold {
            *count = 0;
            self.trigger_snapshot(node_id, current_commit_index);
        }
    }

    /// Triggers a snapshot for a specific node
    fn trigger_snapshot(&self, node_id: u64, commit_index: u64) {
        let snapshot_data = format!("Snapshot-Up-To-{}", commit_index)
            .as_bytes()
            .to_vec();

        info!(
            "[Snapshot] node {} triggering compaction {}",
            node_id,
            String::from_utf8(snapshot_data.clone()).unwrap()
        );

        self.nodes[&node_id].recv(StateUpdateRequest {
            included_index: commit_index,
            data: snapshot_data,
        });
    }

    /// Handles state update responses (snapshot creation acknowledgment)
    fn handle_state_update_response(&self, e: StateUpdateResponse) {
        info!("[Snapshot] node {} snapshot created", e.node_id);
    }

    /// Handles state update commands (snapshot commit)
    fn handle_state_update_command(&self, e: StateUpdateCommand) {
        info!(
            "[Snapshot] node {} commit snapshot {}",
            e.node_id,
            String::from_utf8(e.data.clone()).unwrap(),
        );
    }

    /// Main event loop for processing network messages
    async fn run(
        mut self,
        mut network_rx: tokio::sync::mpsc::UnboundedReceiver<Outbound>,
        mut request_rx: tokio::sync::mpsc::UnboundedReceiver<MakeRequest>,
    ) {
        loop {
            tokio::select! {
                Some(event) = request_rx.recv() => {
                    self.handle_client_request(event);
                },
                Some(message) = network_rx.recv() => {
                    match message {
                        Outbound::NetworkUpdateInd(e) => self.handle_network_update(e),
                        Outbound::MessageToPeer(dest_node_id, protocol) => {
                            self.handle_message_to_peer(dest_node_id, protocol);
                        }
                        Outbound::CommitNotification(e) => self.handle_commit_notification(e),
                        Outbound::StateUpdateResponse(e) => self.handle_state_update_response(e),
                        Outbound::StateUpdateCommand(e) => self.handle_state_update_command(e),
                    }
                }
            }
        }
    }
}

/// Client simulator that periodically sends requests to the cluster
struct ClientSimulator {
    request_tx: tokio::sync::mpsc::UnboundedSender<MakeRequest>,
    request_interval: tokio::time::Duration,
}

impl ClientSimulator {
    fn new(request_tx: tokio::sync::mpsc::UnboundedSender<MakeRequest>) -> Self {
        Self {
            request_tx,
            request_interval: REQUEST_INTERVAL,
        }
    }

    /// Runs the client simulator, sending periodic requests
    async fn run(self) {
        let mut request_id: u64 = 1;
        loop {
            tokio::time::sleep(self.request_interval).await;
            info!("[Client] sending request #{}", request_id);

            let request = MakeRequest {
                request: format!("Request-{}", request_id).as_bytes().to_vec(),
            };

            let _ = self.request_tx.send(request);
            request_id += 1;
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    // Create simulation with channels
    let (mut simulation, network_rx, request_rx) = RaftSimulation::new();

    // Initialize all nodes
    simulation.initialize_nodes();
    simulation.print_info();

    // Clone nodes for network simulator
    let nodes_clone = simulation.nodes.clone();
    let config_clone = SimulationConfig::default();

    // Spawn network simulator task
    let network_simulator = NetworkSimulator::new(nodes_clone, config_clone);
    tokio::spawn(network_simulator.run(network_rx, request_rx));

    // Spawn client simulator task
    let client_simulator = ClientSimulator::new(simulation.request_tx.clone());
    tokio::spawn(client_simulator.run());

    // Wait for shutdown signal
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");

    // Shutdown simulation
    simulation.shutdown();
}
