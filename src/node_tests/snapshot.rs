use super::common::*;
use crate::event::*;
use crate::node::{Inbound, InitiateElection, InitiateHeartbeat};

#[tokio::test]
async fn test_leader_receives_snapshot_and_compacts_log() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 5 requests and commit them
    for i in 1..=5 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("entry_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    let snapshot_data = b"snapshot_data_1_to_3".to_vec();
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 3,
            data: snapshot_data.clone(),
        }));

    // Leader sends StateUpdateResponse to itself
    let event = tokio::time::timeout(tokio::time::Duration::from_millis(50), outbound_rx.recv())
        .await
        .expect("Should receive StateUpdateResponse")
        .expect("Channel should not be closed");
    assert!(matches!(event, Outbound::StateUpdateResponse(_)));

    // Leader should not broadcast to followers immediately after snapshot creation
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err(),
        "Leader should not send messages to followers immediately after snapshot creation"
    );

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
        assert_eq!(
            ae.prev_log_index, 5,
            "prev_log_index should be 5 (follower caught up)"
        );
        assert_eq!(ae.prev_log_term, 1);
        assert_eq!(
            ae.entries.len(),
            0,
            "Should have 0 new entries (follower is caught up)"
        );
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_installs_snapshot() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    let snapshot_data = b"snapshot_state".to_vec();
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InstallSnapshot(InstallSnapshot {
            term: 1,
            leader_id: 1,
            last_included_index: 10,
            last_included_term: 1,
            data: snapshot_data.clone(),
        }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive CommitStateUpdate")
        .expect("Channel should not be closed");

    if let Outbound::StateUpdateCommand(update) = event {
        assert_eq!(update.included_index, 10);
        assert_eq!(update.included_term, 1);
        assert_eq!(update.data, snapshot_data);
    } else {
        panic!("Expected CommitStateUpdate, got: {:?}", event);
    }

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive InstallSnapshotResponse")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::InstallSnapshotResponse(resp)) = event {
        assert_eq!(resp.node_id, 2);
        assert_eq!(resp.term, 1);
    } else {
        panic!("Expected InstallSnapshotResponse, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_rejects_snapshot_with_stale_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Update follower 2 to term 2
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::RequestVote(RequestVote {
            term: 2,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        }));

    drain_messages(&mut outbound_rx, 1).await;

    // Follower receives InstallSnapshot with stale term 1
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InstallSnapshot(InstallSnapshot {
            term: 1,
            leader_id: 1,
            last_included_index: 5,
            last_included_term: 1,
            data: b"stale_snapshot".to_vec(),
        }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive response")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::InstallSnapshotResponse(resp)) = event {
        assert_eq!(resp.node_id, 2);
        assert_eq!(resp.term, 2, "Response should have current term (2)");
    } else {
        panic!("Expected InstallSnapshotResponse, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_processes_snapshot_response() {
    let num_nodes = 3;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 11 requests and commit them
    for i in 1..=11 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("request_{}", i).into_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 10,
            data: b"snapshot_up_to_10".to_vec(),
        }));
    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");
    assert_eq!(
        event,
        Outbound::StateUpdateResponse(StateUpdateResponse {
            node_id: 1,
            included_index: 10,
            included_term: 1,
        })
    );

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InstallSnapshotResponse(InstallSnapshotResponse {
            node_id: 2,
            term: 1,
        }));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
        // After InstallSnapshotResponse, next_index should be last_included_index + 1
        assert_eq!(
            ae.prev_log_index, 10,
            "prev_log_index should be at snapshot point"
        );
        assert_eq!(ae.entries.len(), 1, "Should send entry 11");
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_non_leader_ignores_snapshot_available() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 5,
            data: b"snapshot".to_vec(),
        }));

    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(50), outbound_rx.recv())
            .await
            .is_err(),
        "Follower should ignore SnapshotAvailableInd"
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_snapshot_installation_with_higher_term() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InstallSnapshot(InstallSnapshot {
            term: 3,
            leader_id: 1,
            last_included_index: 10,
            last_included_term: 3,
            data: b"snapshot".to_vec(),
        }));

    drain_messages(&mut outbound_rx, 1).await;

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive response")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::InstallSnapshotResponse(resp)) = event {
        assert_eq!(resp.term, 3, "Follower should update to term 3");
    } else {
        panic!("Expected InstallSnapshotResponse, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_log_compaction_with_partial_log() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 10 requests and commit them
    for i in 1..=10 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("entry_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 6,
            data: b"snapshot_1_to_6".to_vec(),
        }));

    // Drain StateUpdateResponse from the snapshot
    drain_messages(&mut outbound_rx, 1).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
        assert!(
            ae.prev_log_index >= 6,
            "prev_log_index should be >= 6 (after snapshot point)"
        );
        assert_eq!(ae.prev_log_term, 1);
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_snapshot_covering_all_entries() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 5 requests and commit them
    for i in 1..=5 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("entry_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    while let Ok(Some(_)) =
        tokio::time::timeout(tokio::time::Duration::from_millis(10), outbound_rx.recv()).await
    {}

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 5,
            data: b"full_snapshot".to_vec(),
        }));

    // Drain StateUpdateResponse from the snapshot
    drain_messages(&mut outbound_rx, 1).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
        assert_eq!(ae.prev_log_index, 5);
        assert_eq!(
            ae.entries.len(),
            0,
            "Should have no entries after full snapshot"
        );
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_new_entries_after_snapshot() {
    let num_nodes = 1;
    let quorum = 1;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    expect_leader_elected(1, &mut outbound_rx).await;

    for i in 1..=3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("entry_{}", i).as_bytes().to_vec(),
            }));
    }

    while let Ok(Some(event)) =
        tokio::time::timeout(tokio::time::Duration::from_millis(10), outbound_rx.recv()).await
    {
        if let Outbound::CommitNotification(_) = event {
            continue;
        }
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 3,
            data: b"snapshot".to_vec(),
        }));

    // Drain StateUpdateResponse from the snapshot
    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive StateUpdateResponse")
        .expect("Channel should not be closed");
    assert!(matches!(event, Outbound::StateUpdateResponse(_)));

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: b"entry_4".to_vec(),
        }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive commit notification")
        .expect("Channel should not be closed");

    if let Outbound::CommitNotification(notif) = event {
        assert_eq!(notif.index, 4);
        assert_eq!(notif.request, b"entry_4");
    } else {
        panic!("Expected CommitNotification, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_snapshot_updates_commit_index() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::InstallSnapshot(InstallSnapshot {
            term: 1,
            leader_id: 1,
            last_included_index: 10,
            last_included_term: 1,
            data: b"snapshot".to_vec(),
        }));

    drain_messages(&mut outbound_rx, 1).await;
    drain_messages(&mut outbound_rx, 1).await;

    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 10,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 1,
                request: b"entry_11".to_vec(),
            }],
            leader_commit: 10,
        }));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive response")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntriesResponse(resp)) = event {
        assert!(resp.success);
        assert_eq!(resp.prev_log_index, 11);
    } else {
        panic!("Expected AppendEntriesResponse, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_snapshot_with_empty_log() {
    let num_nodes = 1;
    let quorum = 1;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateElection(InitiateElection));

    expect_leader_elected(1, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 0,
            data: b"empty_snapshot".to_vec(),
        }));

    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_multiple_snapshots_sequential() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 10 requests and commit them
    for i in 1..=10 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("entry_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 3,
            data: b"snapshot_1".to_vec(),
        }));

    // Drain StateUpdateResponse from the first snapshot
    drain_messages(&mut outbound_rx, 1).await;

    // Make 5 more requests (11-15) and commit them
    for i in 11..=15 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("entry_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 8,
            data: b"snapshot_2".to_vec(),
        }));

    // Drain StateUpdateResponse from the second snapshot
    drain_messages(&mut outbound_rx, 1).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive AppendEntries")
        .expect("Channel should not be closed");

    if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
        assert_eq!(ae.prev_log_index, 15);
        assert_eq!(ae.entries.len(), 0);
    } else {
        panic!("Expected AppendEntries, got: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_sends_install_snapshot_when_follower_far_behind() {
    let num_nodes = 3;
    let quorum = 3;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Make 10 requests and commit them
    for i in 1..=10 {
        make_request_and_commit(
            &nodes,
            1,
            num_nodes,
            format!("request_{}", i).as_bytes().to_vec(),
            &mut outbound_rx,
            i,
            1,
        )
        .await;
    }

    let snapshot_data = b"snapshot_1_to_8".to_vec();
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::StateUpdateRequest(StateUpdateRequest {
            included_index: 8,
            data: snapshot_data.clone(),
        }));

    // Drain StateUpdateResponse from the snapshot
    drain_messages(&mut outbound_rx, 1).await;

    for _ in 0..5 {
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
    }

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::InitiateHeartbeat(InitiateHeartbeat(2)));

    let event = tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("Should receive message")
        .expect("Channel should not be closed");

    match event {
        Outbound::MessageToPeer(_peer_id, Protocol::InstallSnapshot(snapshot)) => {
            assert_eq!(snapshot.term, 1);
            assert_eq!(snapshot.leader_id, 1);
            assert_eq!(snapshot.last_included_index, 8);
            assert_eq!(snapshot.last_included_term, 1);
            assert_eq!(snapshot.data, snapshot_data);
        }
        _ => panic!("Expected InstallSnapshot, got: {:?}", event),
    }

    shutdown_cluster(nodes).await;
}
