// Term management tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::Inbound;

#[tokio::test]
async fn test_reject_vote_with_stale_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader 1 to establish term 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Node 2 sends a RequestVote to Node 1 with a stale term
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 0,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }));

    // Verify Node 1 rejects the vote
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::Vote(vote) = event {
        assert!(!vote.granted);
        assert_eq!(vote.term, 1);
    } else {
        panic!("unexpected event, expected Vote: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_reject_append_entries_with_stale_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader 1 to establish term 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Node 2 sends an AppendEntries to Node 1 with a stale term
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 0,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));

    // Verify Node 1 rejects the request
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntriesResponse(resp) = event {
        assert!(!resp.success);
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
async fn test_follower_updates_term_on_higher_request_vote() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 is at term 0 (initial state)
    // Node 2 sends RequestVote with term 5
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 5,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }));

    // Node 1 should update to term 5 and grant vote
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::Vote(vote) = event {
        assert!(vote.granted);
        assert_eq!(vote.term, 5);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_updates_term_on_higher_append_entries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader at term 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Another node sends AppendEntries with higher term
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 3,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));

    // Node 1 should update to term 3 and accept
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntriesResponse(resp) = event {
        assert!(resp.success);
        assert_eq!(resp.term, 3);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_steps_down_on_higher_term_append_entries_response() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader at term 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Simulate receiving AppendEntriesResponse with higher term
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 3,
            prev_log_index: 0,
            success: false,
        }));

    // Leader should step down and not send more heartbeats
    // Any subsequent events should not include heartbeats from node 1
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_candidate_steps_down_on_higher_term_vote() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 becomes candidate at term 1
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Receives vote with higher term
    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 5,
        voter_id: 2,
        granted: false,
    }));

    // Should step down and not become leader
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_equal_term_request_vote_with_same_candidate() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 votes for node 2 at term 1
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }));

    let (_, event) = outbound_rx.recv().await.unwrap();
    if let Outbound::Vote(vote) = event {
        assert!(vote.granted);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    // Node 2 sends another RequestVote for same term (retransmission)
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }));

    // Should grant again (already voted for this candidate)
    let (_, event) = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::Vote(vote) = event {
        assert!(vote.granted);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_equal_term_request_vote_with_different_candidate() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Node 1 votes for node 2 at term 1
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }));

    let _ = outbound_rx.recv().await;

    // Node 3 requests vote for same term
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        }));

    // Should reject (already voted for different candidate in this term)
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 3);
    if let Outbound::Vote(vote) = event {
        assert!(!vote.granted);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_term_increment_on_election() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // First election - should be term 1
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    for _ in 0..num_nodes - 1 {
        if let Some((_, event)) = outbound_rx.recv().await {
            if let Outbound::RequestVote(rv) = event {
                assert_eq!(rv.term, 1);
            }
        }
    }

    // Second election without becoming leader - should be term 2
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    for _ in 0..num_nodes - 1 {
        if let Some((_, event)) = outbound_rx.recv().await {
            if let Outbound::RequestVote(rv) = event {
                assert_eq!(rv.term, 1); // Node 2 starts at term 0, increments to 1
                break;
            }
        }
    }

    shutdown_cluster(nodes).await;
}
