use std::collections::HashMap;

use gossip_rs::event::*;
use gossip_rs::node::{DefaultDependencyProvider, Node};
use gossip_rs::timer;
use tracing::*;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() {
    const NUM_NODES: u64 = 5;
    const QUORUM: u64 = (NUM_NODES / 2) + 1;
    const MESSAGE_DROP_PROBABILITY: f64 = 0.03; // 3% chance of dropping a message
    const MAX_MESSAGE_DELAY_MS: u64 = 150; // Max network latency in milliseconds

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
    tokio::spawn(async move {
        while let Some((dest_node_id, message)) = network_rx.recv().await {
            // --- Simulate message dropping ---
            if rand::random_bool(MESSAGE_DROP_PROBABILITY) {
                warn!("NETWORK: Dropping message to node {}", dest_node_id);
                continue; // Skip to the next message
            }

            // --- Simulate message delay ---
            let delay =
                tokio::time::Duration::from_millis(rand::random_range(0..=MAX_MESSAGE_DELAY_MS));

            if let Some(node) = nodes_clone.get(&dest_node_id).cloned() {
                // Spawn a new task to handle the delayed delivery.
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    // Dispatch the message to the destination node's correct receiver method.
                    match message {
                        Outbound::MakeRequest(e) => node.recv(e),
                        Outbound::RequestVote(e) => node.recv(e),
                        Outbound::Vote(e) => node.recv(e),
                        Outbound::AppendEntries(e) => node.recv(e),
                        Outbound::AppendEntriesResponse(e) => node.recv(e),
                    }
                });
            }
        }
    });

    // --- Client Request Injector Task ---
    // This task simulates a client sending commands to the cluster.
    let nodes_clone = nodes.clone();
    tokio::spawn(async move {
        let mut request_id: u64 = 1;
        loop {
            // Wait for a few seconds before sending a new request.
            tokio::time::sleep(tokio::time::Duration::from_millis(4000)).await;

            // Send the request to a random node. Raft will handle forwarding if it's not the leader.
            let target_node_id = rand::random_range(1..=NUM_NODES);

            if let Some(node) = nodes_clone.get(&target_node_id) {
                info!(
                    "CLIENT: Sending request #{} to random node {}",
                    request_id, target_node_id
                );
                let request = MakeRequest {
                    request: format!("Request-{}", request_id),
                };
                node.recv(request);
                request_id += 1;
            }
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
