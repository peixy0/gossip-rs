// Common test utilities following TESTING.MD guidelines
use crate::event::*;
use crate::node::{DependencyProvider, Inbound, InitiateElection, Node};
use crate::timer;
use std::collections::HashMap;
use std::future;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

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

/// Helper to initialize tracing for debugging
#[allow(dead_code)]
pub(crate) fn setup_tracing() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
}

/// Helper to create a cluster of nodes for testing
pub(crate) fn create_cluster(
    num_nodes: u64,
    quorum: u64,
) -> (
    HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) {
    let mut nodes = HashMap::new();
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();

    let node_pool: Vec<u64> = (1..=num_nodes).collect();
    for node_id in &node_pool {
        let (node, join_handle) = Node::<TestDependencyProvider>::new(
            *node_id,
            node_pool.clone(),
            outbound_tx.clone(),
            NoopTimerService,
            quorum,
            tokio::time::Duration::from_secs(1),
            tokio::time::Duration::from_secs(3),
        );
        nodes.insert(*node_id, (node, join_handle));
    }

    (nodes, outbound_rx)
}

/// This is useful when we want to get to the next non-commit message
pub(crate) async fn get_next_non_commit_message(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> Option<Outbound> {
    loop {
        if let Some(event) = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("Timeout waiting for message in get_next_non_commit_message")
        {
            match event {
                Outbound::CommitNotification(_) => continue, // Skip commit notifications
                _ => return Some(event),
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

/// Helper to expect a leader elected event
pub(crate) async fn expect_leader_elected(
    leader_id: u64,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) {
    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event,
        Outbound::NetworkUpdateInd(NetworkUpdateInd { leader_id })
    );
}

/// Helper to elect a leader by sending votes from all peers
pub(crate) async fn elect_leader(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    num_nodes: u64,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) {
    // Initiate election
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Drain RequestPreVotes (new pre-vote phase)
    collect_request_prevotes(rx, (num_nodes - 1) as usize).await;

    // Send pre-votes
    send_prevotes(
        nodes,
        1,
        (2..=num_nodes).collect::<Vec<u64>>().as_slice(),
        1,
        true,
    );

    // Drain RequestVotes (real vote phase)
    collect_request_votes(rx, (num_nodes - 1) as usize).await;

    // Send real votes
    send_votes(
        nodes,
        1,
        (2..=num_nodes).collect::<Vec<u64>>().as_slice(),
        1,
        true,
    );

    expect_leader_elected(1, rx).await;

    // Drain heartbeats
    collect_append_entries(rx, (num_nodes - 1) as usize).await;
}

/// Helper to receive and assert a message with timeout
pub(crate) async fn recv_with_timeout(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    duration: tokio::time::Duration,
) -> Outbound {
    tokio::time::timeout(duration, rx.recv())
        .await
        .expect("Timeout waiting for message")
        .expect("Channel closed unexpectedly")
}

/// Helper to assert no message is received within timeout
pub(crate) async fn assert_no_message(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    duration: tokio::time::Duration,
) {
    assert!(
        tokio::time::timeout(duration, rx.recv()).await.is_err(),
        "Expected no message but received one"
    );
}

/// Helper to collect RequestVote messages sent to peers
/// Returns a HashMap of peer_id -> RequestVote event
pub(crate) async fn collect_request_votes(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_count: usize,
) -> HashMap<u64, RequestVote> {
    let mut votes = HashMap::new();
    for _ in 0..expected_count {
        let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
        if let Outbound::MessageToPeer(peer_id, Protocol::RequestVote(rv)) = event {
            votes.insert(peer_id, rv);
        } else {
            panic!("Expected RequestVote, got: {:?}", event);
        }
    }
    votes
}

/// Helper to collect RequestPreVote messages sent to peers
/// Returns a HashMap of peer_id -> RequestPreVote event
pub(crate) async fn collect_request_prevotes(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_count: usize,
) -> HashMap<u64, RequestPreVote> {
    let mut prevotes = HashMap::new();
    for _ in 0..expected_count {
        let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
        if let Outbound::MessageToPeer(peer_id, Protocol::RequestPreVote(rpv)) = event {
            prevotes.insert(peer_id, rpv);
        } else {
            panic!("Expected RequestPreVote, got: {:?}", event);
        }
    }
    prevotes
}

/// Helper to collect AppendEntries messages sent to peers
/// Returns a HashMap of peer_id -> AppendEntries event
pub(crate) async fn collect_append_entries(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_count: usize,
) -> HashMap<u64, AppendEntries> {
    let mut entries = HashMap::new();
    for _ in 0..expected_count {
        let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
        if let Outbound::MessageToPeer(peer_id, Protocol::AppendEntries(ae)) = event {
            entries.insert(peer_id, ae);
        } else {
            panic!("Expected AppendEntries, got: {:?}", event);
        }
    }
    entries
}

/// Helper to assert RequestVote messages were sent to expected peers
pub(crate) async fn assert_request_votes_sent(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_peers: &[u64],
    expected_term: u64,
    expected_candidate_id: u64,
) {
    let votes = collect_request_votes(rx, expected_peers.len()).await;

    for &peer_id in expected_peers {
        let vote = votes
            .get(&peer_id)
            .unwrap_or_else(|| panic!("Missing RequestVote for peer {}", peer_id));
        assert_eq!(
            vote.term, expected_term,
            "Wrong term in RequestVote to peer {}",
            peer_id
        );
        assert_eq!(
            vote.candidate_id, expected_candidate_id,
            "Wrong candidate_id in RequestVote to peer {}",
            peer_id
        );
    }
}

/// Helper to assert AppendEntries heartbeats were sent to expected peers
pub(crate) async fn assert_heartbeats_sent(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_peers: &[u64],
    expected_term: u64,
    expected_leader_id: u64,
) {
    let entries = collect_append_entries(rx, expected_peers.len()).await;

    for &peer_id in expected_peers {
        let ae = entries
            .get(&peer_id)
            .unwrap_or_else(|| panic!("Missing AppendEntries for peer {}", peer_id));
        assert_eq!(
            ae.term, expected_term,
            "Wrong term in AppendEntries to peer {}",
            peer_id
        );
        assert_eq!(
            ae.leader_id, expected_leader_id,
            "Wrong leader_id in AppendEntries to peer {}",
            peer_id
        );
    }
}

/// Helper to expect a specific message type, panic otherwise
pub(crate) async fn expect_append_entries_response(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> (u64, AppendEntriesResponse) {
    let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(peer_id, Protocol::AppendEntriesResponse(resp)) = event {
        (peer_id, resp)
    } else {
        panic!("Expected AppendEntriesResponse, got: {:?}", event);
    }
}

/// Helper to send votes to a candidate
pub(crate) fn send_votes(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    candidate_id: u64,
    voter_ids: &[u64],
    term: u64,
    granted: bool,
) {
    for &voter_id in voter_ids {
        nodes
            .get(&candidate_id)
            .unwrap()
            .0
            .recv(Inbound::Vote(Vote {
                term,
                voter_id,
                granted,
            }));
    }
}

/// Helper to send pre-votes to a candidate
pub(crate) fn send_prevotes(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    candidate_id: u64,
    voter_ids: &[u64],
    term: u64,
    granted: bool,
) {
    for &voter_id in voter_ids {
        nodes
            .get(&candidate_id)
            .unwrap()
            .0
            .recv(Inbound::PreVote(PreVote {
                term,
                voter_id,
                granted,
            }));
    }
}

/// Helper to expect a single AppendEntries message
pub(crate) async fn expect_append_entries(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> (u64, AppendEntries) {
    let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(peer_id, Protocol::AppendEntries(ae)) = event {
        (peer_id, ae)
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }
}

/// Helper to expect a CommitNotification
pub(crate) async fn expect_commit_notification(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> CommitNotification {
    let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::CommitNotification(notif) = event {
        notif
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }
}

/// Helper to expect a StateUpdateResponse
pub(crate) async fn expect_state_update_response(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> StateUpdateResponse {
    let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::StateUpdateResponse(resp) = event {
        resp
    } else {
        panic!("Expected StateUpdateResponse, got: {:?}", event);
    }
}

/// Helper to expect a Vote response
pub(crate) async fn expect_vote(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) -> (u64, Vote) {
    let event = recv_with_timeout(rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(peer_id, Protocol::Vote(vote)) = event {
        (peer_id, vote)
    } else {
        panic!("Expected Vote, got: {:?}", event);
    }
}

/// Helper to commit a Request
pub(crate) async fn make_request_and_commit(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    leader_id: u64,
    num_nodes: u64,
    request: Vec<u8>,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
    expected_index: u64,
    expected_term: u64,
) {
    // Leader receives request
    nodes
        .get(&leader_id)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: request.clone(),
        }));

    // Collect AppendEntries messages sent to followers and send responses
    for _ in 1..num_nodes {
        let event = rx.recv().await.expect("Should receive AppendEntries");
        if let Outbound::MessageToPeer(peer_id, Protocol::AppendEntries(ae)) = event {
            nodes
                .get(&leader_id)
                .unwrap()
                .0
                .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
                    node_id: peer_id,
                    term: ae.term,
                    prev_log_index: ae.prev_log_index + ae.entries.len() as u64,
                    success: true,
                }));
        } else {
            panic!("Expected AppendEntries, got: {:?}", event);
        }
    }

    // Expect CommitNotification
    let notif = expect_commit_notification(rx).await;
    assert_eq!(notif.index, expected_index, "Commit index mismatch");
    assert_eq!(notif.term, expected_term, "Commit term mismatch");
    assert_eq!(notif.request, request, "Commit request mismatch");
}

/// Helper to make a follower aware of the leader
pub(crate) async fn make_follower_aware_of_leader(
    nodes: &HashMap<u64, (Node<TestDependencyProvider>, crate::node::JoinHandle)>,
    follower_id: u64,
    leader_id: u64,
    term: u64,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Outbound>,
) {
    nodes
        .get(&follower_id)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("Timeout waiting for AppendEntriesResponse")
        .expect("Channel closed unexpectedly");
}
