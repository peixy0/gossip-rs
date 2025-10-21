// Log replication tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection, InitiateHeartbeat};
use std::collections::HashSet;

#[tokio::test]
async fn test_single_node_log_commit() {
    // Test that a single-node cluster commits entries immediately
    // We verify this by checking for CommitNotification events
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

    // Become leader first
    node.recv(Inbound::InitiateElection(InitiateElection));

    // Verify no messages sent during election (no peers in single-node cluster)
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err(),
        "Single node should not send any messages during election"
    );

    // Send a request
    node.recv(Inbound::MakeRequest(MakeRequest {
        request: "test_entry_1".to_string(),
    }));

    // Should receive a CommitNotification for the committed entry
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("Should receive commit notification")
            .expect("Channel should not be closed");

    assert_eq!(
        peer_id, node_id,
        "CommitNotification should be sent to self"
    );
    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(notif.index, 1, "First entry should be committed at index 1");
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    // Send another request
    node.recv(Inbound::MakeRequest(MakeRequest {
        request: "test_entry_2".to_string(),
    }));

    // Should receive another CommitNotification
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("Should receive second commit notification")
            .expect("Channel should not be closed");

    assert_eq!(peer_id, node_id);
    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(
            notif.index, 2,
            "Second entry should be committed at index 2"
        );
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    // Verify no more messages
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err(),
        "Should not receive any more messages"
    );

    node.shutdown();
    join_handle.wait().await;
}

#[tokio::test]
async fn test_two_node_log_commit_verification() {
    // This test verifies that commits actually happen by checking:
    // 1. CommitNotification events
    // 2. leader_commit field in AppendEntries messages
    let num_nodes = 2;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Leader receives a request
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".to_string(),
        }));

    // Verify AppendEntries is sent with the new entry
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    let prev_log_index = if let Outbound::AppendEntries(ae) = event {
        assert_eq!(ae.term, 1);
        assert_eq!(ae.leader_id, 1);
        assert_eq!(ae.entries.len(), 1);
        assert_eq!(ae.entries[0].request, "test");
        // Commit index should still be 0 (not committed yet, waiting for quorum)
        assert_eq!(
            ae.leader_commit, 0,
            "Entry should not be committed before receiving responses"
        );
        ae.prev_log_index + ae.entries.len() as u64
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    };

    // Send success response from follower (now quorum is reached: leader + follower = 2)
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

    // Should receive CommitNotification for the leader's own commit
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 1, "CommitNotification should be sent to leader");
    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(
            notif.index, 1,
            "Entry should be committed after quorum reached"
        );
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    // Trigger another heartbeat to observe the updated commit index
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    // Verify the next AppendEntries shows the entry is committed
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntries(ae) = event {
        assert_eq!(
            ae.leader_commit, 1,
            "leader_commit should be updated to 1 in subsequent AppendEntries"
        );
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_basic_log_replication() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Leader receives a request
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".to_string(),
        }));

    // Verify AppendEntries are sent with the new entry
    let mut log_replication_recv = HashSet::new();
    for _ in 0..num_nodes - 1 {
        let (peer_id, event) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
                .await
                .unwrap()
                .unwrap();
        if let Outbound::AppendEntries(event) = event {
            assert_eq!(event.term, 1);
            assert_eq!(event.leader_id, 1);
            assert_eq!(event.entries.len(), 1);
            assert_eq!(event.entries[0].request, "test");
            log_replication_recv.insert(peer_id);
        } else {
            panic!("unexpected event: {:?}", event);
        }
    }
    assert_eq!(log_replication_recv, HashSet::from([2, 3]));

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_with_inconsistencies() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Leader adds an entry
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".to_string(),
        }));

    // Drain AppendEntries
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Simulate follower 2 rejecting the entry
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

    // Trigger retry by sending heartbeat
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    // Verify retry with decremented next_index
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntries(event) = event {
        assert_eq!(event.term, 1);
        assert_eq!(event.prev_log_index, 0);
        assert_eq!(event.entries.len(), 1);
        assert_eq!(event.entries[0].request, "test");
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

    // Add multiple entries
    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("request_{}", i),
            }));
        drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;
    }

    // Send success responses to update next_index
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

    // Trigger another heartbeat to verify all entries
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    // Use helper to skip CommitNotification and get the next AppendEntries
    let (_, event) = get_next_non_commit_message(&mut outbound_rx)
        .await
        .expect("Should receive AppendEntries");

    if let Outbound::AppendEntries(event) = event {
        // After receiving success responses for all 3 entries, next_index should be 4
        // So prev_log_index should be 3
        assert_eq!(event.prev_log_index, 3);
        assert_eq!(event.entries.len(), 0); // No new entries
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

    // Add entry
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "entry1".to_string(),
        }));
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Follower 2 fails first append
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

    // Retry
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));
    drain_messages(&mut outbound_rx, 1).await;

    // Follower 2 succeeds on retry
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

    // Add another entry - should now replicate successfully
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "entry2".to_string(),
        }));

    let mut found_entry2 = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(ae) = event {
                if ae.entries.len() == 1 && ae.entries[0].request == "entry2" {
                    found_entry2 = true;
                }
            }
        }
    }
    assert!(
        found_entry2,
        "Should successfully replicate entry2 after recovery"
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_replication_with_conflicting_entries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Add entry to leader
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "leader_entry".to_string(),
        }));
    drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Simulate follower rejecting due to log inconsistency
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

    // Leader retries with prev_log_index = 0
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let (_, event) = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::AppendEntries(event) = event {
        // Should retry from the beginning
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

    // Add multiple entries
    for i in 1..=5 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("entry_{}", i),
            }));
        drain_messages(&mut outbound_rx, (num_nodes - 1) as usize).await;
    }

    // Initially next_index[2] = 1 (log was empty when leader was elected)
    // After each AppendEntries with entries, next_index doesn't change until we get responses

    // Simulate repeated failures from follower 2
    // Failure 1: backoff = 1, next_index = max(1, 1-1) = 1, prev_log_index = 0
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

    let (_, event) = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::AppendEntries(event) = event {
        assert_eq!(event.prev_log_index, 0);
    } else {
        panic!("unexpected event: {:?}", event);
    }

    // Failure 2: backoff doubles to 2, next_index = max(1, 1-2) = 1, prev_log_index = 0
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

    let (_, event) = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    if let Outbound::AppendEntries(event) = event {
        // Still at 0 because next_index can't go below 1
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

    // Send heartbeat without any new entries
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(peer_id, 2);
    if let Outbound::AppendEntries(event) = event {
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

    // Simulate follower 2 receiving AppendEntries that should truncate its log
    // This happens when leader sends entries with prev_log_index that matches,
    // but the follower has additional uncommitted entries
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
                request: "new_entry".to_string(),
            }],
            leader_commit: 0,
        }));

    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();

    assert_eq!(peer_id, 1);
    if let Outbound::AppendEntriesResponse(resp) = event {
        assert!(resp.success);
        assert_eq!(resp.prev_log_index, 1); // Log should have 1 entry now
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}
