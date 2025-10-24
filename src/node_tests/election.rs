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
        tokio::time::Duration::from_secs(1),
        tokio::time::Duration::from_secs(3),
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

    // Verify RequestPreVote messages sent to all peers
    collect_request_prevotes(&mut outbound_rx, num_nodes as usize - 1).await;

    // Send pre-votes from all peers
    send_prevotes(&nodes, 1, &[2, 3], 1, true);

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

    // Verify RequestPreVote messages sent
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // All pre-votes are rejected
    send_prevotes(&nodes, 1, &[2, 3], 1, false);

    // No leader should be elected (pre-vote failed, so no RequestVote sent)
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

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

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Send enough pre-votes to proceed (need quorum including self)
    send_prevotes(&nodes, 1, &[2, 3], 1, true);

    // Collect RequestVotes
    collect_request_votes(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Receive pre-vote with higher term - should step down
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
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

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Exactly quorum pre-votes (including self)
    send_prevotes(&nodes, 1, &[2, 3], 1, true);

    // Collect RequestVotes
    collect_request_votes(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

#[tokio::test]
async fn test_candidate_receives_vote_with_stale_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Send pre-votes with correct term
    send_prevotes(&nodes, 1, &[2, 3], 1, true);

    // Collect RequestVotes
    collect_request_votes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // One vote with stale term should be ignored
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 0,
        voter_id: 2,
        granted: true,
    }));

    // Vote with correct term
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 3,
        granted: true,
    }));

    // Should become leader with quorum (self + node 3)
    expect_leader_elected(1, &mut outbound_rx).await;

    shutdown_cluster(nodes).await;
}
