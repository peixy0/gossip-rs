// Election tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection};

#[tokio::test]
async fn test_single_node_elect_itself_as_leader() {
    let node_id = 1;
    let quorum = 1;
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (node, join_handle) = crate::node::Node::<TestDependencyProvider>::new(
        node_id,
        vec![1u64],
        outbound_tx,
        NoopTimerService,
        quorum,
    );

    node.recv(Inbound::InitiateElection(InitiateElection));
    expect_leader_elected(1, &mut outbound_rx).await;

    node.shutdown();
    join_handle.wait().await;
}

#[tokio::test]
async fn test_three_node_cluster_elect_leader() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Verify RequestVote messages sent to all peers
    assert_request_votes_sent(&mut outbound_rx, &[2, 3], 1, 1).await;

    // Send votes from all peers
    send_votes(&nodes, 1, &[2, 3], 1, true);

    expect_leader_elected(1, &mut outbound_rx).await;

    // Verify heartbeats sent to all peers
    assert_heartbeats_sent(&mut outbound_rx, &[2, 3], 1, 1).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_election_failure() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Verify RequestVote messages sent
    assert_request_votes_sent(&mut outbound_rx, &[2, 3], 1, 1).await;

    // All votes are rejected
    send_votes(&nodes, 1, &[2, 3], 1, false);

    // No leader should be elected
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_candidate_receives_append_entries_and_becomes_follower() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));

    // Expect AppendEntriesResponse
    let (_peer_id, resp) = expect_append_entries_response(&mut outbound_rx).await;
    assert!(resp.success);
    assert_eq!(resp.term, 1);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_split_vote_scenario() {
    let num_nodes = 4;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // One vote granted, one rejected - not enough for quorum
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 2,
        granted: true,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 3,
        granted: false,
    }));

    // No leader should be elected
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_candidate_receives_higher_term_vote() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Receive vote with higher term
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 2,
        voter_id: 2,
        granted: false,
    }));

    // No leader should be elected
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_exact_quorum_vote() {
    let num_nodes = 5;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Exactly quorum votes (including self)
    send_votes(&nodes, 1, &[2, 3], 1, true);

    expect_leader_elected(1, &mut outbound_rx).await;

    // Verify heartbeats sent to all peers
    let heartbeats = collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
    assert_eq!(heartbeats.len(), 4);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_receives_append_entries_from_higher_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));

    // Expect AppendEntriesResponse with higher term
    let (_peer_id, resp) = expect_append_entries_response(&mut outbound_rx).await;
    assert!(resp.success);
    assert_eq!(resp.term, 2);

    shutdown_cluster(nodes).await;
}
