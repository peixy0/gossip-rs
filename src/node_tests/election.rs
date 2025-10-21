// Election tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection};
use std::collections::HashSet;

#[tokio::test]
async fn test_single_node_elect_itself_as_leader() {
    let num_nodes = 1;
    let node_id = 1;
    let quorum = 1;
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (node, join_handle) = crate::node::Node::<TestDependencyProvider>::new(
        node_id,
        num_nodes,
        outbound_tx,
        NoopTimerService,
        quorum,
    );

    node.recv(Inbound::InitiateElection(InitiateElection));

    // Single node should immediately become leader after becoming candidate
    // since it votes for itself and that meets the quorum of 1.
    // No outbound messages should be sent (no peers to send RequestVote to).
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

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

    // Collect RequestVote messages
    let mut request_vote_recv = HashSet::new();
    for _ in 0..num_nodes - 1 {
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        if let Outbound::RequestVote(event) = event {
            assert_eq!(event.term, 1);
            assert_eq!(event.candidate_id, 1);
            request_vote_recv.insert(peer_id);
        } else {
            panic!("unexpected event: {:?}", event);
        }
    }
    assert_eq!(request_vote_recv, HashSet::from([2, 3]));

    // Send votes from all peers
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    // Verify heartbeats are sent after becoming leader
    let mut heartbeat_recv = HashSet::new();
    for _ in 0..num_nodes - 1 {
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        if let Outbound::AppendEntries(event) = event {
            assert_eq!(event.term, 1);
            assert_eq!(event.leader_id, 1);
            heartbeat_recv.insert(peer_id);
        } else {
            panic!("unexpected event: {:?}", event);
        }
    }
    assert_eq!(heartbeat_recv, HashSet::from([2, 3]));

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

    // Drain RequestVotes
    for _ in 0..num_nodes - 1 {
        let (_, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        if let Outbound::RequestVote(event) = event {
            assert_eq!(event.term, 1);
            assert_eq!(event.candidate_id, 1);
        } else {
            panic!("unexpected event: {:?}", event);
        }
    }

    // All peers reject the vote
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 2,
        granted: false,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 3,
        granted: false,
    }));

    // Node should not become leader (no heartbeats sent)
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_candidate_receives_append_entries_and_becomes_follower() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 becomes a candidate
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Drain RequestVotes
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Node 2 sends an AppendEntries to Node 1 (simulating it won the election)
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

    // Verify Node 1 sends a success response and accepts the new leader
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntriesResponse(resp) = event {
        assert!(resp.success);
        assert_eq!(resp.term, 1);
    } else {
        panic!(
            "unexpected event, expected AppendEntriesResponse: {:?}",
            event
        );
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_split_vote_scenario() {
    // Test where votes are split and no leader is elected
    let num_nodes = 4;
    let quorum = 3; // Need 3 votes to win
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 becomes candidate
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Drain RequestVotes
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Only 2 peers vote (not enough for quorum of 3)
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

    // Node should not become leader
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_candidate_receives_higher_term_vote() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 becomes candidate at term 1
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Receives a vote response with higher term - should step down
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 2,
        voter_id: 2,
        granted: false,
    }));

    // Should not send heartbeats (not a leader)
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_exact_quorum_vote() {
    // Test that exactly meeting quorum is sufficient to become leader
    let num_nodes = 5;
    let quorum = 3; // Need 3 votes (including self)
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Exactly 2 more votes (self + 2 = 3, meets quorum)
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 2,
        granted: true,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 1,
        voter_id: 3,
        granted: true,
    }));

    // Should become leader and send heartbeats
    let mut heartbeat_count = 0;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(_) = event {
                heartbeat_count += 1;
            }
        }
    }
    assert_eq!(heartbeat_count, 4); // Should send to all 4 peers

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_receives_append_entries_from_higher_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect node 1 as leader at term 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Node 2 sends AppendEntries with higher term
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

    // Leader should step down and send success response
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntriesResponse(resp) = event {
        assert!(resp.success);
        assert_eq!(resp.term, 2);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}
