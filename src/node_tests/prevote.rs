// Pre-vote tests to prevent unnecessary term inflation
use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection};

#[tokio::test]
async fn test_prevote_single_node_cluster() {
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
async fn test_prevote_three_node_cluster_successful() {
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
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    // After winning pre-vote, should send RequestVote
    let mut vote_requests = vec![];
    for _ in 0..num_nodes - 1 {
        let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
        if let Outbound::MessageToPeer(peer_id, Protocol::RequestVote(rv)) = event {
            assert_eq!(rv.term, 1, "Real vote should be for term 1");
            assert_eq!(rv.candidate_id, 1);
            vote_requests.push((peer_id, rv));
        } else {
            panic!("Expected RequestVote, got: {:?}", event);
        }
    }

    // Send real votes from all peers
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    expect_leader_elected(1, &mut outbound_rx).await;

    // Verify heartbeats sent to all peers
    collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_rejected_prevents_term_inflation() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Collect RequestPreVote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // All pre-votes are rejected
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
            term: 1,
            voter_id: i,
            granted: false,
        }));
    }

    // Should NOT send RequestVote (no term increment)
    // Should NOT become leader
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_prevents_disruption_from_partitioned_node() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 becomes leader
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Send pre-votes
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    // Collect vote messages
    collect_request_votes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Send real votes
    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    expect_leader_elected(1, &mut outbound_rx).await;
    collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Simulate node 3 being partitioned and timing out
    // It starts election but others reject pre-vote because they have a leader
    nodes
        .get(&3)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    // Collect RequestPreVote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Nodes 1 and 2 reject pre-vote because they're receiving heartbeats
    // (In real scenario, they'd reject because they haven't timed out)
    nodes.get(&3).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 1,
        granted: false,
    }));
    nodes.get(&3).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 2,
        granted: false,
    }));

    // Node 3 should NOT send RequestVote (term stays at 1, not incrementing)
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_with_stale_log() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 with stale log tries to get pre-votes
    // Simulate by sending RequestPreVote with lower log indices
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::RequestPreVote(RequestPreVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }));

    let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(peer_id, Protocol::PreVote(pv)) = event {
        assert_eq!(peer_id, 1);
        // Should grant pre-vote if log is ok (in this case it is)
        assert!(pv.granted);
    } else {
        panic!("Expected PreVote response, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_candidate_receives_appendentries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // While in pre-candidate state, receives AppendEntries from leader
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

    // Should step down and send AppendEntriesResponse
    let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntriesResponse(resp)) = event {
        assert!(resp.success);
        assert_eq!(resp.term, 1);
    } else {
        panic!("Expected AppendEntriesResponse, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_insufficient_responses() {
    let num_nodes = 5;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Only one pre-vote granted (need 3 for quorum including self = 2 total)
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 2,
        granted: true,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 3,
        granted: false,
    }));

    // Should not proceed to real election
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_exact_quorum() {
    let num_nodes = 5;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Exactly quorum pre-votes (including self)
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 2,
        granted: true,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 3,
        granted: true,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 4,
        granted: false,
    }));

    // Should proceed to real election
    let vote_requests = collect_request_votes(&mut outbound_rx, (num_nodes - 1) as usize).await;
    assert_eq!(vote_requests.len(), 4);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_higher_term_response() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Receive pre-vote response with higher term
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 5,
        voter_id: 2,
        granted: false,
    }));

    // Should not proceed to real election
    // Should become follower
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_grants_prevote() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 2 receives pre-vote request from node 1
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::RequestPreVote(RequestPreVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }));

    // Should grant pre-vote
    let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
    if let Outbound::MessageToPeer(peer_id, Protocol::PreVote(pv)) = event {
        assert_eq!(peer_id, 1);
        assert!(pv.granted, "Should grant pre-vote with up-to-date log");
        assert_eq!(pv.term, 1, "Term should not change during pre-vote");
    } else {
        panic!("Expected PreVote response, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_prevote_does_not_increment_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // All nodes start at term 0
    // Node 1 initiates election (pre-vote)
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Send all pre-votes as rejected
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 2,
        granted: false,
    }));
    nodes.get(&1).unwrap().0.recv(Inbound::PreVote(PreVote {
        term: 1,
        voter_id: 3,
        granted: false,
    }));

    // No RequestVote should be sent (term should still be 0)
    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    // Now try another election - should still use term 1 for pre-vote
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    for _ in 0..num_nodes - 1 {
        let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
        if let Outbound::MessageToPeer(_peer_id, Protocol::RequestPreVote(rpv)) = event {
            assert_eq!(
                rpv.term, 1,
                "Pre-vote should still be for term 1 after failed pre-vote"
            );
        } else {
            panic!("Expected RequestPreVote, got: {:?}", event);
        }
    }

    shutdown_cluster(nodes).await;
}
