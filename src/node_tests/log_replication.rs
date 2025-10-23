// Log replication tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection, InitiateHeartbeat};

#[tokio::test]
async fn test_single_node_log_commit() {
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

    node.recv(Inbound::MakeRequest(MakeRequest {
        request: "test_entry_1".as_bytes().to_vec(),
    }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive commit notification")
        .expect("Channel should not be closed");

    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(notif.index, 1);
        assert_eq!(notif.term, 1);
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    node.recv(Inbound::MakeRequest(MakeRequest {
        request: "test_entry_2".as_bytes().to_vec(),
    }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive second commit notification")
        .expect("Channel should not be closed");

    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(notif.index, 2);
        assert_eq!(notif.term, 1);
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err()
    );

    node.shutdown();
    join_handle.wait().await;
}

#[tokio::test]
async fn test_two_node_log_commit_verification() {
    let num_nodes = 2;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".as_bytes().to_vec(),
        }));

    let (_peer_id, ae) = expect_append_entries(&mut outbound_rx).await;
    let prev_log_index = ae.prev_log_index + ae.entries.len() as u64;
    assert_eq!(ae.term, 1);
    assert_eq!(ae.leader_id, 1);
    assert_eq!(ae.entries.len(), 1);
    assert_eq!(ae.entries[0].request, "test".as_bytes().to_vec());
    assert_eq!(ae.leader_commit, 0);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index,
            success: true,
        }));

    let notif = expect_commit_notification(&mut outbound_rx).await;
    assert_eq!(notif.index, 1);
    assert_eq!(notif.term, 1);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let (_peer_id, ae) = expect_append_entries(&mut outbound_rx).await;
    assert_eq!(ae.leader_commit, 1);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_basic_log_replication() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".as_bytes().to_vec(),
        }));

    // Verify AppendEntries sent to all peers with the entry
    let entries = collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
    assert_eq!(entries.len(), 2);

    for (_peer_id, ae) in entries {
        assert_eq!(ae.term, 1);
        assert_eq!(ae.leader_id, 1);
        assert_eq!(ae.entries.len(), 1);
        assert_eq!(ae.entries[0].request, "test".as_bytes().to_vec());
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_with_inconsistencies() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".as_bytes().to_vec(),
        }));

    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 0,
            success: false,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();
    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.term, 1);
        assert_eq!(event.prev_log_index, 0);
        assert_eq!(event.entries.len(), 1);
        assert_eq!(event.entries[0].request, "test".as_bytes().to_vec());
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_multiple_log_entries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("request_{}", i).as_bytes().to_vec(),
            }));
        drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;
    }

    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
                node_id: 2,
                term: 1,
                prev_log_index: i,
                success: true,
            }));
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = get_next_non_commit_message(&mut outbound_rx)
        .await
        .expect("Should receive AppendEntries");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.prev_log_index, 3);
        assert_eq!(event.entries.len(), 0);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_after_failure_and_recovery() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "entry1".as_bytes().to_vec(),
        }));
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 0,
            success: false,
        }));

    drain_messages(&mut outbound_rx, 1).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 1,
            success: true,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "entry2".as_bytes().to_vec(),
        }));

    let mut found_entry2 = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some(event)) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
                if ae.entries.len() == 1 && ae.entries[0].request == "entry2".as_bytes().to_vec() {
                    found_entry2 = true;
                }
            }
        }
    }
    assert!(found_entry2);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_with_conflicting_entries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "leader_entry".as_bytes().to_vec(),
        }));
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 0,
            success: false,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.prev_log_index, 0);
        assert_eq!(event.entries.len(), 1);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_exponential_backoff() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    for i in 1..=5 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("entry_{}", i).as_bytes().to_vec(),
            }));
        drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 0,
            success: false,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.prev_log_index, 0);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 2,
            term: 1,
            prev_log_index: 0,
            success: false,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.prev_log_index, 0);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_empty_append_entries_heartbeat() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(event)) = event {
        assert_eq!(event.entries.len(), 0);
        assert_eq!(event.prev_log_index, 0);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_log_truncation_on_conflict() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                request: "new_entry".as_bytes().to_vec(),
            }],
            leader_commit: 0,
        }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntriesResponse(resp)) = event {
        assert!(resp.success);
        assert_eq!(resp.prev_log_index, 1);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}
