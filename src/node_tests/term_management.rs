// Term management tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::Inbound;

#[tokio::test]
async fn test_reject_vote_with_stale_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::Vote(vote)) = event {
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntriesResponse(resp)) = event {
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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::Vote(vote)) = event {
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntriesResponse(resp)) = event {
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    // Collect pre-vote messages
    collect_request_prevotes(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 5,
        voter_id: 2,
        granted: false,
    }));

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Timeout waiting for response")
        .expect("Channel closed unexpectedly");
    if let Outbound::MessageToPeer(_peer_id, Protocol::Vote(vote)) = event {
        assert!(vote.granted);
    } else {
        panic!("unexpected event: {:?}", event);
    }

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::Vote(vote)) = event {
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

    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Timeout waiting for response")
        .expect("Channel closed unexpectedly");

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

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::Vote(vote)) = event {
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

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    for _ in 0..num_nodes - 1 {
        if let Some(event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("Timeout waiting for message")
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::RequestVote(rv)) = event {
                assert_eq!(rv.term, 1);
            }
        }
    }

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(crate::node::InitiateElection));

    for _ in 0..num_nodes - 1 {
        if let Some(event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .expect("Timeout waiting for message")
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::RequestVote(rv)) = event {
                assert_eq!(rv.term, 1);
                break;
            }
        }
    }

    shutdown_cluster(nodes).await;
}
