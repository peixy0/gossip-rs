use std::collections::HashMap;

use gossip_rs::event::*;
use gossip_rs::node::{DefaultDependencyProvider, Node};
use gossip_rs::timer;
use tracing::*;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    info!("Starting Raft voting simulation...");

    let num_nodes: u64 = 4;
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Outbound)>();
    let mut nodes = HashMap::new();
    let mut futs = vec![];
    let timer_service = timer::DefaultTimerService;
    for node_id in 1..=num_nodes {
        let (node, join) = Node::<DefaultDependencyProvider>::new(
            node_id,
            num_nodes,
            outbound_tx.clone(),
            timer_service.clone(),
            num_nodes / 2 + 1,
        );
        nodes.insert(node_id, node);
        futs.push(join.wait());
    }

    tokio::spawn(async move {
        loop {
            tokio::select! {
                e = outbound_rx.recv() => {
                    if let Some((recipient, event)) = e {
                        match event {
                            Outbound::RequestVote(ev) => {
                                if let Some(node) = nodes.get(&recipient) {
                                    node.recv(ev);
                                }
                            }
                            Outbound::Vote(ev) => {
                                if let Some(node) = nodes.get(&recipient) {
                                    node.recv(ev);
                                }
                            }
                            Outbound::AppendEntries(ev) => {
                                if let Some(node) = nodes.get(&recipient) {
                                    node.recv(ev);
                                }
                            }
                            Outbound::AppendEntriesResponse(ev) => {
                                if let Some(node) = nodes.get(&recipient) {
                                    node.recv(ev);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    });

    for fut in futs {
        fut.await;
    }

    info!("Simulation complete — all nodes exited cleanly.");
}
