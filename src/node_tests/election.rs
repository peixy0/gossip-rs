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

    for i in 2..=num_nodes {
        nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
            term: 1,
            voter_id: i,
            granted: true,
        }));
    }

    expect_leader_elected(1, &mut outbound_rx).await;

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
    let num_nodes = 4;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes.get(&1).unwrap().0.recv(Inbound::Vote(Vote {
        term: 2,
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

    expect_leader_elected(1, &mut outbound_rx).await;

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
    assert_eq!(heartbeat_count, 4);

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
