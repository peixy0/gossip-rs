use std::collections::HashMap;

use gossip_rs::event::*;
use gossip_rs::node::{DefaultDependencyProvider, Node};
use gossip_rs::timer;
use tracing::*;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const NUM_NODES: u64 = 5;
const QUORUM: u64 = (NUM_NODES / 2) + 1;
const MESSAGE_DROP_PROBABILITY: f64 = 0.33; // 33% chance of dropping a message
const MAX_MESSAGE_DELAY_MS: u64 = 150; // Max network latency in milliseconds
const SNAPSHOT_THRESHOLD: u64 = 5;

fn maybe_dispatch_event<E>(node: Node<DefaultDependencyProvider>, event: E)
where
    Node<DefaultDependencyProvider>: Recv<E>,
    E: Send + 'static,
{
    // --- Simulate message dropping ---
    if rand::random_bool(MESSAGE_DROP_PROBABILITY) {
        warn!("[Network] dropping message to node {}", node.get_id());
        return; // Skip to the next message
    }
    // Spawn a new task to handle the delayed delivery.
    tokio::spawn(async move {
        let delay = std::time::Duration::from_millis(rand::random_range(0..=MAX_MESSAGE_DELAY_MS));
        tokio::time::sleep(delay).await;
        node.recv(event);
    });
}

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    // The "Network": A channel that receives all outbound messages from all nodes.
    let (network_tx, mut network_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Outbound)>();

    // A place to store handles to our running nodes.
    let mut nodes: HashMap<u64, Node<DefaultDependencyProvider>> = HashMap::new();

    // Create and start all the Raft nodes.
    for i in 1..=NUM_NODES {
        let timer_service = timer::DefaultTimerService;
        let (node, _handle) = Node::new(i, NUM_NODES, network_tx.clone(), timer_service, QUORUM);
        nodes.insert(i, node);
    }

    let nodes_clone = nodes.clone();
    // --- Network Simulator Task ---
    // This task reads from the central network channel and simulates unreliability.
    // It also tracks CommitNotifications to trigger log compaction.
    tokio::spawn(async move {
        let mut leader = None;
        let mut committed_count = 0u64;
        while let Some((dest_node_id, message)) = network_rx.recv().await {
            if let Some(node) = nodes_clone.get(&dest_node_id).cloned() {
                match message {
                    Outbound::MakeRequest(e) => {
                        if let Some(leader_id) = leader {
                            nodes_clone
                                .get(&leader_id)
                                .map(|leader_node| leader_node.recv(e));
                        }
                    }
                    Outbound::RequestVote(e) => maybe_dispatch_event(node, e),
                    Outbound::Vote(e) => maybe_dispatch_event(node, e),
                    Outbound::AppendEntries(e) => {
                        leader = Some(e.leader_id);
                        maybe_dispatch_event(node, e);
                    }
                    Outbound::AppendEntriesResponse(e) => maybe_dispatch_event(node, e),
                    Outbound::InstallSnapshot(e) => {
                        leader = Some(e.leader_id);
                        maybe_dispatch_event(node, e);
                    }
                    Outbound::InstallSnapshotResponse(e) => maybe_dispatch_event(node, e),
                    Outbound::CommitNotification(e) => {
                        info!(
                            "[Node {}] commit request index {} term {} {}",
                            dest_node_id,
                            e.index,
                            e.term,
                            String::from_utf8(e.request).unwrap()
                        );

                        committed_count += 1;
                        let current_commit_index = e.index;

                        if committed_count >= SNAPSHOT_THRESHOLD * NUM_NODES {
                            let snapshot_data = format!("snapshot_up_to_{}", current_commit_index)
                                .as_bytes()
                                .to_vec();
                            info!(
                                "[Compaction] triggering compaction to {}",
                                String::from_utf8(snapshot_data.clone()).unwrap()
                            );
                            committed_count = 0;

                            if let Some(leader_id) = leader {
                                nodes_clone.get(&leader_id).map(|leader_node| {
                                    leader_node.recv(StateUpdateRequest {
                                        included_index: current_commit_index,
                                        data: snapshot_data,
                                    })
                                });
                            }
                        }
                    }
                    Outbound::StateUpdateResponse(e) => {
                        info!(
                            "[Node {}] snapshot created at index {} term {}",
                            dest_node_id, e.included_index, e.included_term,
                        );
                    }
                    Outbound::StateUpdateCommand(e) => {
                        info!(
                            "[Node {}] commit snapshot {}",
                            dest_node_id,
                            String::from_utf8(e.data).unwrap(),
                        );
                    }
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut request_id: u64 = 1;
        loop {
            // Wait for a few seconds before sending a new request.
            tokio::time::sleep(tokio::time::Duration::from_millis(4000)).await;
            info!("[Client] sending request #{}", request_id);
            let request = MakeRequest {
                request: format!("Request-{}", request_id).as_bytes().to_vec(),
            };
            let _ = network_tx.send((request_id % NUM_NODES + 1, request.into()));
            request_id += 1;
        }
    });

    // Run the simulation until Ctrl+C is pressed.
    info!(
        "Raft simulation starting with {} nodes. Quorum size is {}.",
        NUM_NODES, QUORUM
    );
    info!(
        "Network will randomly drop {:.0}% of messages and delay others up to {}ms.",
        MESSAGE_DROP_PROBABILITY * 100.0,
        MAX_MESSAGE_DELAY_MS
    );
    info!("Press Ctrl+C to stop.");

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c");

    info!("Shutdown signal received. Shutting down all nodes.");
    for (id, node) in nodes {
        info!("Sending shutdown to node {}", id);
        node.shutdown();
    }
    info!("Simulation finished.");
}
