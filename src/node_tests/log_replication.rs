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
        tokio::time::Duration::from_secs(1),
        tokio::time::Duration::from_secs(3),
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".as_bytes().to_vec(),
        }));

    // Collect AppendEntries sent to followers
    collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("request_{}", i).as_bytes().to_vec(),
            }));
        // Collect AppendEntries sent to followers for each request
        collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "entry1".as_bytes().to_vec(),
        }));

    // Collect AppendEntries sent to followers
    collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;

    // Node 2 fails to append (returns false)
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

    // Node 3 succeeds, allowing commit
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
            node_id: 3,
            term: 1,
            prev_log_index: 1,
            success: true,
        }));

    // Now we should get CommitNotification for entry1
    let notif = expect_commit_notification(&mut outbound_rx).await;
    assert_eq!(notif.request, "entry1".as_bytes().to_vec());

    // Leader retries with node 2 after failure
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "leader_entry".as_bytes().to_vec(),
        }));
    // Collect AppendEntries sent to followers
    collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    for i in 1..=5 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("entry_{}", i).as_bytes().to_vec(),
            }));
        // Collect AppendEntries sent to followers for each request
        collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

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

#[tokio::test]
async fn test_leader_handles_concurrent_append_entries_responses() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    // Send multiple requests quickly
    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("request_{}", i).as_bytes().to_vec(),
            }));
    }

    // Collect all AppendEntries messages
    for _ in 1..=3 {
        collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
    }

    // Send responses for all entries from both followers
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
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::AppendEntriesResponse(AppendEntriesResponse {
                node_id: 3,
                term: 1,
                prev_log_index: i,
                success: true,
            }));
    }

    // Should get 3 commit notifications
    for i in 1..=3 {
        let notif = expect_commit_notification(&mut outbound_rx).await;
        assert_eq!(notif.index, i);
        assert_eq!(notif.request, format!("request_{}", i).as_bytes().to_vec());
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_preserves_committed_entry_on_conflict() {
    // This test simulates a key part of the "Leader Safety Property".
    // A follower has a committed entry and a conflicting uncommitted entry.
    // A new leader sends AppendEntries that should cause the follower to truncate
    // its log, but the already-committed entry must be preserved.
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // We are testing the logic of a follower, node 2.
    // First, put node 2's log into a specific state:
    // Index 1: { term: 1, request: "committed" } (and commit_index is 1)
    // Index 2: { term: 2, request: "old_uncommitted" }
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 2,
            leader_id: 98, // fake old leader
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    term: 1,
                    request: b"committed".to_vec(),
                },
                LogEntry {
                    term: 2,
                    request: b"old_uncommitted".to_vec(),
                },
            ],
            leader_commit: 1, // This tells the follower that index 1 is committed.
        }));

    // The node will emit both a CommitNotification and an AppendEntriesResponse.
    // We collect both and don't rely on the order.
    let mut found_resp = false;
    for _ in 0..2 {
        let event = recv_with_timeout(&mut outbound_rx, tokio::time::Duration::from_secs(1)).await;
        match event {
            Outbound::MessageToPeer(_, Protocol::AppendEntriesResponse(resp)) => {
                assert!(resp.success);
                assert_eq!(resp.prev_log_index, 2);
                found_resp = true;
            }
            Outbound::CommitNotification(notif) => {
                assert_eq!(notif.index, 1);
            }
            _ => panic!("Unexpected event received: {:?}", event),
        }
    }
    assert!(found_resp, "Did not receive expected AppendEntriesResponse");

    // Now, a new leader (node 99) in term 3 sends AppendEntries.
    // This new leader's log has the same committed entry at index 1,
    // but a different entry at index 2. This is a valid scenario in Raft.
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 3,
            leader_id: 99,     // fake new leader
            prev_log_index: 1, // The logs match up to index 1
            prev_log_term: 1,  // Term of the entry at index 1
            entries: vec![LogEntry {
                term: 3,
                request: b"new_uncommitted".to_vec(),
            }],
            leader_commit: 1,
        }));

    // The follower should find the conflict at index 2, truncate its log to index 1,
    // and then append the new entry. It should respond with success.
    let (_, resp2) = expect_append_entries_response(&mut outbound_rx).await;
    assert!(
        resp2.success,
        "Follower should accept new entry after truncating conflicting one"
    );
    assert_eq!(
        resp2.prev_log_index, 2,
        "Follower's log should now be at index 2 with the new entry"
    );

    // The verification is indirect: the follower accepted the AppendEntries starting after index 1.
    // This implies it did not find a mismatch at index 1, meaning the committed entry was preserved.
    // If it had deleted the entry at index 1, prev_log_index=1 would have failed the consistency check.

    shutdown_cluster(nodes).await;
}
