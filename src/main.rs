use gossip_rs::node::Node;
use tracing::*;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
    info!("Node starting...");
    let (node, node_join) = Node::new();
    let node_clone = node.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        node_clone.shutdown();
    });
    node_join.wait().await;
    info!("Node exiting...");
}
