// Request handling tests following TESTING.md guidelines
use super::common::*;
use crate::event::*;
use crate::node::Inbound;

#[tokio::test]
async fn test_follower_drops_client_request() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    // Make follower aware of leader
    make_follower_aware_of_leader(&nodes, 2, 1, 1, &mut outbound_rx).await;

    // Follower receives request - should drop it
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "test".as_bytes().to_vec(),
        }));

    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;
    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_handles_request_directly() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "leader_request".as_bytes().to_vec(),
        }));

    let mut found_append_entries = 0;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some(event)) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
                if ae.entries.len() == 1
                    && ae.entries[0].request == "leader_request".as_bytes().to_vec()
                {
                    found_append_entries += 1;
                }
            }
        }
    }
    assert_eq!(found_append_entries, 2);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_follower_without_known_leader_drops_request() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    // Follower has no leader - should drop request
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "lost_request".as_bytes().to_vec(),
        }));

    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_multiple_concurrent_requests_to_leader() {
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
                request: format!("request_{}", i).as_bytes().to_vec(),
            }));
    }

    let mut requests_found = std::collections::HashSet::new();
    for _ in 0..(5 * (num_nodes - 1)) {
        if let Ok(Some(event)) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
                for entry in ae.entries {
                    requests_found.insert(entry.request);
                }
            }
        }
    }

    for i in 1..=5 {
        assert!(
            requests_found.contains(&format!("request_{}", i).as_bytes().to_vec()),
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

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    // Make follower aware of leader
    make_follower_aware_of_leader(&nodes, 2, 1, 1, &mut outbound_rx).await;

    // Request during transition - follower should drop it
    nodes
        .get(&2)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "during_transition".as_bytes().to_vec(),
        }));

    assert_no_message(&mut outbound_rx, tokio::time::Duration::from_millis(100)).await;

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_request_with_empty_payload() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: "".as_bytes().to_vec(),
        }));

    let mut found = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some(event)) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
                if ae.entries.len() == 1 && ae.entries[0].request.is_empty() {
                    found = true;
                }
            }
        }
    }
    assert!(found);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_large_request_payload() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    let large_payload = "x".repeat(10000);
    nodes
        .get(&1)
        .unwrap()
        .0
        .recv(Inbound::MakeRequest(MakeRequest {
            request: large_payload.clone().as_bytes().to_vec(),
        }));

    let mut found = false;
    for _ in 0..num_nodes - 1 {
        if let Ok(Some(event)) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), outbound_rx.recv()).await
        {
            if let Outbound::MessageToPeer(_peer_id, Protocol::AppendEntries(ae)) = event {
                if ae.entries.len() == 1
                    && ae.entries[0].request == large_payload.as_bytes().to_vec()
                {
                    found = true;
                }
            }
        }
    }
    assert!(found);

    shutdown_cluster(nodes).await;
}

#[tokio::test]
async fn test_leader_handles_duplicate_requests() {
    let num_nodes = 3;
    let quorum = 2;
    let (nodes, mut outbound_rx) = create_cluster(num_nodes, quorum);

    elect_leader(&nodes, num_nodes, &mut outbound_rx).await;

    let request_data = b"duplicate_request".to_vec();

    // Send the same request multiple times
    for _ in 0..3 {
        nodes
            .get(&1)
            .unwrap()
            .0
            .recv(Inbound::MakeRequest(MakeRequest {
                request: request_data.clone(),
            }));
    }

    // Collect all AppendEntries messages - may be batched or separate
    let mut total_entries = 0;
    for _ in 0..3 {
        let entries = collect_append_entries(&mut outbound_rx, (num_nodes - 1) as usize).await;
        assert_eq!(entries.len(), 2);
        for (_peer_id, ae) in entries {
            total_entries += ae.entries.len();
            // Each entry should contain the request data
            for entry in &ae.entries {
                assert_eq!(entry.request, request_data);
            }
        }
    }

    // Should have received 3 entries total (may be batched differently across followers)
    assert!(total_entries >= 6); // At least 3 entries per follower

    shutdown_cluster(nodes).await;
}
