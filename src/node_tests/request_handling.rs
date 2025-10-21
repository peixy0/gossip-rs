// Request handling tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::Inbound;

#[tokio::test]
async fn test_follower_forwards_request_to_leader() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Manually send AppendEntries to follower 2 to ensure it knows the leader
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));
    // Drain the response
    let _ = outbound_rx.recv().await;

    // Send a request to follower (node 2)
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".to_string(),
        }));

    // Verify the request is forwarded to the leader (node 1)
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 1);
    if let Outbound::MakeRequest(req) = event {
        assert_eq!(req.request, "test");
    } else {
        panic!("unexpected event, expected MakeRequest: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_handles_request_directly() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Send request to leader
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "leader_request".to_string(),
        }));

    // Leader should replicate to followers, not forward
    let mut found_append_entries = 0;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(ae) = event {
                if ae.entries.len() == 1 && ae.entries[0].request == "leader_request" {
                    found_append_entries += 1;
                }
            }
        }
    }
    assert_eq!(found_append_entries, 2); // Should send to both followers

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_without_known_leader_drops_request() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Send request to follower without establishing a leader
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "lost_request".to_string(),
        }));

    // Should not forward (no known leader) and should timeout
    assert!(
        tokio::time::timeout(tokio::time::Duration::from_millis(100), outbound_rx.recv())
            .await
            .is_err()
    );

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_multiple_concurrent_requests_to_leader() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Send multiple requests rapidly
    for i in 1..=5 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: format!("request_{}", i),
            }));
    }

    // Collect all AppendEntries to verify each request was processed
    let mut requests_found = std::collections::HashSet::new();
    for _ in 0..(5 * (num_nodes - 1)) {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(ae) = event {
                for entry in ae.entries {
                    requests_found.insert(entry.request);
                }
            }
        }
    }

    // Verify all requests were processed
    for i in 1..=5 {
        assert!(
            requests_found.contains(&format!("request_{}", i)),
            "Missing request_{}",
            i
        );
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_request_during_leader_transition() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Elect leader 1
    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Inform follower 2 about leader
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));
    drain_messages(&mut outbound_rx, 1).await;

    // Send request to follower
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "during_transition".to_string(),
        }));

    // Should forward to current leader
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 1);
    if let Outbound::MakeRequest(_) = event {
        // Success
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_request_with_empty_payload() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Send request with empty string
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "".to_string(),
        }));

    // Should still replicate even with empty payload
    let mut found = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(ae) = event {
                if ae.entries.len() == 1 && ae.entries[0].request.is_empty() {
                    found = true;
                }
            }
        }
    }
    assert!(found, "Empty request should still be replicated");

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_request_forwarding_chain() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Inform follower 3 about leader via follower 2's forwarding
    // First, follower 2 needs to know the leader
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::AppendEntries(AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }));
    drain_messages(&mut outbound_rx, 1).await;

    // Now send request to follower 2, which should forward to leader 1
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "chain_request".to_string(),
        }));

    // Verify forwarded to leader
    let (peer_id, event) =
        tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(peer_id, 1);
    if let Outbound::MakeRequest(req) = event {
        assert_eq!(req.request, "chain_request");
    } else {
        panic!("unexpected event: {:?}", event);
    }

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_large_request_payload() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, 1, num_nodes, &mut outbound_rx).await;

    // Send request with large payload
    let large_payload = "x".repeat(10000);
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: large_payload.clone(),
        }));

    // Verify large payload is replicated correctly
    let mut found = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some((_, event))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::AppendEntries(ae) = event {
                if ae.entries.len() == 1 && ae.entries[0].request == large_payload {
                    found = true;
                }
            }
        }
    }
    assert!(found, "Large payload should be replicated correctly");

    shutdown_cluster(nodes).await;
}
