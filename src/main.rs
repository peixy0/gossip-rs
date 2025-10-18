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

    let num_nodes: usize = 4;
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<Outbound>();
    let mut nodes = vec![];
    let mut futs = vec![];
    let timer_service = timer::DefaultTimerService;
    for node_id in 1..=num_nodes {
        let (node, join) = Node::<DefaultDependencyProvider>::new(
            node_id as u64,
            outbound_tx.clone(),
            timer_service.clone(),
            num_nodes / 2 + 1,
        );
        nodes.push((node_id as u64, node));
        futs.push(join.wait());
    }

    tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            if rand::random_range(0.0..1.0) < 0.3 {
                warn!("Dropping {:?}", msg);
                continue;
            }
            match msg {
                Outbound::RequestVote(e) => {
                    for (node_id, node) in &nodes {
                        if *node_id != e.candidate_id {
                            node.recv(e.clone());
                        }
                    }
                }
                Outbound::Vote(recipient, e) => {
                    for (node_id, node) in &nodes {
                        if *node_id == recipient {
                            node.recv(e.clone());
                        }
                    }
                }
                Outbound::AppendEntries(e) => {
                    for (node_id, node) in &nodes {
                        if *node_id != e.leader_id {
                            node.recv(e.clone());
                        }
                    }
                }
                Outbound::AppendEntriesResponse(recipient, e) => {
                    for (node_id, node) in &nodes {
                        if *node_id == recipient {
                            node.recv(e.clone());
                        }
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
