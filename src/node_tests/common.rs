// Common test utilities following TESTING.MD guidelines
use crate::event::*;
use crate::node::{DependencyProvider, Inbound, InitiateElection, Node};
use crate::timer;
use std::collections::HashMap;
use std::future;

/// NoopTimer implementation for deterministic testing
pub(crate) struct NoopTimer;

impl timer::Timer for NoopTimer {}

/// NoopTimerService as required by TESTING.md - does not rely on real-time
#[derive(Clone)]
pub(crate) struct NoopTimerService;

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

/// Test dependency provider using NoopTimerService
pub(crate) struct TestDependencyProvider;

impl DependencyProvider for TestDependencyProvider {
    type TimerService = NoopTimerService;
}

/// Helper to create a cluster of nodes for testing
pub(crate) fn create_cluster(
    num_nodes: u64,
    quorum: u64,
) -> (
    HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    tokio::sync::mpsc::UnboundedReceiver<(u64, Outbound)>,
) {
    let mut nodes = HashMap::new();
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();

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

    (nodes, outbound_rx)
}

/// Helper to drain a specific number of messages from the outbound channel
pub(crate) async fn drain_messages(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, Outbound)>,
    count: usize,
) {
    for _ in 0..count {
        let _ = rx.recv().await;
    }
}

/// Helper to drain messages, skipping CommitNotification events
/// This is useful when we want to get to the next non-commit message
pub(crate) async fn get_next_non_commit_message(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, Outbound)>,
) -> Option<(u64, Outbound)> {
    loop {
        if let Some((peer_id, event)) = rx.recv().await {
            match event {
                Outbound::CommitNotification(_) => continue, // Skip commit notifications
                _ => return Some((peer_id, event)),
            }
        } else {
            return None;
        }
    }
}

/// Helper to shutdown all nodes in a cluster
pub(crate) async fn shutdown_cluster(
    nodes: HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
) {
    for (node, join_handle) in nodes.into_values() {
        node.shutdown();
        join_handle.wait().await;
    }
}

/// Helper to elect a leader by sending votes from all peers
pub(crate) async fn elect_leader(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    leader_id: u64,
    num_nodes: u64,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, Outbound)>,
) {
    // Initiate election
    nodes
        .get(&leader_id)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Drain RequestVotes
    drain_messages(rx, (num_nodes - 1) as usize).await;

    // Send votes
    for i in 1..=num_nodes {
        if i != leader_id {
            nodes.get(&leader_id).unwrap().0.recv(Inbound::Vote(Vote {
                term: 1,
                voter_id: i,
                granted: true,
            }));
        }
    }

    // Drain heartbeats
    drain_messages(rx, (num_nodes - 1) as usize).await;
}
