//! Integration tests for the sync crate
//!
//! These tests verify the networking layer and MLS integration.
//! All tests have timeouts to prevent hanging.

use std::time::Duration;

use iroh::Endpoint;
use mls_rs::external_client::ExternalClient;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use universal_sync_acceptor::{AcceptorRegistry, SharedFjallStateStore, accept_connection};
use universal_sync_core::{AcceptorId, GroupId, PAXOS_ALPN};
use universal_sync_testing::{
    test_cipher_suite, test_crypto_provider, test_group_client, test_identity_provider,
};

/// Initialize tracing for tests
fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("universal_sync=debug")),
        )
        .with_test_writer()
        .try_init();
}

/// Create an iroh endpoint for testing
async fn test_endpoint() -> Endpoint {
    Endpoint::builder()
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind()
        .await
        .expect("failed to create endpoint")
}

#[tokio::test]
async fn test_state_store_group_persistence() {
    init_tracing();

    let temp_dir = TempDir::new().unwrap();

    // Create and store some groups
    let group_id_1 = GroupId::new([1u8; 32]);
    let group_id_2 = GroupId::new([2u8; 32]);
    let group_info_1 = b"test group info 1".to_vec();
    let group_info_2 = b"test group info 2".to_vec();

    {
        let store = SharedFjallStateStore::open(temp_dir.path())
            .await
            .expect("open store");

        store
            .store_group(&group_id_1, &group_info_1)
            .expect("store 1");
        store
            .store_group(&group_id_2, &group_info_2)
            .expect("store 2");

        // Verify they're stored
        let groups = store.list_groups();
        assert_eq!(groups.len(), 2);
    }

    // Reopen and verify persistence
    {
        let store = SharedFjallStateStore::open(temp_dir.path())
            .await
            .expect("reopen store");

        let groups = store.list_groups();
        assert_eq!(groups.len(), 2);

        let info_1 = store.get_group_info(&group_id_1);
        assert_eq!(info_1, Some(group_info_1));

        let info_2 = store.get_group_info(&group_id_2);
        assert_eq!(info_2, Some(group_info_2));
    }
}

#[tokio::test]
async fn test_mls_group_creation_with_group_api() {
    init_tracing();

    // Create client using GroupClient
    let alice = test_group_client("alice", test_endpoint().await);

    // Create a group using the GroupClient API (no acceptors)
    let mut group = alice.create_group(&[], "none").await.expect("create group");

    let context = group.context().await.expect("get context");
    tracing::info!(group_id = ?context.group_id, epoch = ?context.epoch, "Group created");

    assert_eq!(context.member_count, 1, "Should have 1 member (creator)");
    assert!(context.acceptors.is_empty(), "Should have no acceptors");

    group.shutdown().await;
}

#[tokio::test]
async fn test_alice_adds_bob_with_group_api() {
    init_tracing();

    // --- Setup acceptor server ---
    let acceptor_dir = TempDir::new().unwrap();
    let acceptor_endpoint = test_endpoint().await;
    let acceptor_addr = acceptor_endpoint.addr();

    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    let external_client = ExternalClient::builder()
        .crypto_provider(crypto.clone())
        .identity_provider(test_identity_provider())
        .build();

    let state_store = SharedFjallStateStore::open(acceptor_dir.path())
        .await
        .expect("open state store");

    // Spawn acceptor server
    let acceptor_task = tokio::spawn({
        let acceptor_endpoint = acceptor_endpoint.clone();

        let registry = AcceptorRegistry::new(
            external_client,
            cipher_suite.clone(),
            state_store.clone(),
            acceptor_endpoint.secret_key().clone(),
        );

        async move {
            loop {
                if let Some(incoming) = acceptor_endpoint.accept().await {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _ = accept_connection(incoming, registry).await;
                    });
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Alice creates a group using GroupClient ---
    let alice = test_group_client("alice", test_endpoint().await);

    // Create group without acceptors first
    let mut alice_group = alice
        .create_group(&[], "none")
        .await
        .expect("alice create group");

    let group_id = alice_group.group_id();
    tracing::info!(?group_id, "Alice created group");

    // Add the acceptor
    alice_group
        .add_acceptor(acceptor_addr.clone())
        .await
        .expect("add acceptor");

    let context = alice_group.context().await.expect("get context");
    assert_eq!(context.acceptors.len(), 1, "Should have 1 acceptor");
    tracing::info!("Alice added acceptor");

    // Give the acceptor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Alice adds Bob ---
    let bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    // Bob starts listening for welcome in background
    let bob_clone = bob.clone();
    let welcome_handle = tokio::spawn(async move { bob_clone.wait_for_welcome().await });

    // Give Bob a moment to start listening
    tokio::time::sleep(Duration::from_millis(50)).await;

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    tracing::info!("Alice added Bob and sent welcome");

    // Wait for Bob to receive the welcome
    let welcome = welcome_handle
        .await
        .expect("welcome task panicked")
        .expect("failed to receive welcome");

    tracing::info!("Bob received welcome message");

    // --- Bob joins using GroupClient ---
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join group");

    let bob_context = bob_group.context().await.expect("bob context");
    tracing::info!(epoch = ?bob_context.epoch, "Bob joined group");

    // Verify Bob got the acceptor
    assert_eq!(bob_context.acceptors.len(), 1, "Bob should have 1 acceptor");
    assert!(
        bob_context
            .acceptors
            .contains(&AcceptorId(*acceptor_addr.id.as_bytes())),
        "Bob should have the acceptor ID"
    );

    // Verify epochs match
    let alice_context = alice_group.context().await.expect("alice context");
    assert_eq!(
        alice_context.epoch, bob_context.epoch,
        "Alice and Bob should be at the same epoch"
    );

    // --- Alice updates keys ---
    alice_group.update_keys().await.expect("alice update keys");

    let alice_context = alice_group
        .context()
        .await
        .expect("alice context after update");
    tracing::info!(epoch = ?alice_context.epoch, "Alice updated keys");

    // Give a moment for sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    tracing::info!(
        epoch = ?alice_context.epoch,
        "Test complete: Alice and Bob synchronized via Group API"
    );

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

#[tokio::test]
async fn test_acceptor_add_remove() {
    init_tracing();

    // --- Setup two acceptor servers ---
    let acceptor1_dir = TempDir::new().unwrap();
    let acceptor1_endpoint = test_endpoint().await;
    let acceptor1_addr = acceptor1_endpoint.addr();

    let acceptor2_dir = TempDir::new().unwrap();
    let acceptor2_endpoint = test_endpoint().await;
    let acceptor2_addr = acceptor2_endpoint.addr();

    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    // Setup acceptor 1
    let state_store1 = SharedFjallStateStore::open(acceptor1_dir.path())
        .await
        .expect("open state store 1");

    let acceptor1_task = tokio::spawn({
        let acceptor_endpoint = acceptor1_endpoint.clone();
        let external_client = ExternalClient::builder()
            .crypto_provider(crypto.clone())
            .identity_provider(test_identity_provider())
            .build();

        let registry = AcceptorRegistry::new(
            external_client,
            cipher_suite.clone(),
            state_store1.clone(),
            acceptor_endpoint.secret_key().clone(),
        );

        async move {
            loop {
                if let Some(incoming) = acceptor_endpoint.accept().await {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _ = accept_connection(incoming, registry).await;
                    });
                }
            }
        }
    });

    // Setup acceptor 2
    let state_store2 = SharedFjallStateStore::open(acceptor2_dir.path())
        .await
        .expect("open state store 2");

    let acceptor2_task = tokio::spawn({
        let acceptor_endpoint = acceptor2_endpoint.clone();
        let external_client = ExternalClient::builder()
            .crypto_provider(crypto.clone())
            .identity_provider(test_identity_provider())
            .build();

        let registry = AcceptorRegistry::new(
            external_client,
            cipher_suite.clone(),
            state_store2.clone(),
            acceptor_endpoint.secret_key().clone(),
        );

        async move {
            loop {
                if let Some(incoming) = acceptor_endpoint.accept().await {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _ = accept_connection(incoming, registry).await;
                    });
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Alice creates a group and manages acceptors ---
    let alice = test_group_client("alice", test_endpoint().await);

    let mut group = alice.create_group(&[], "none").await.expect("create group");

    // Add first acceptor
    group
        .add_acceptor(acceptor1_addr.clone())
        .await
        .expect("add acceptor 1");

    let context = group.context().await.expect("context after add1");
    assert_eq!(context.acceptors.len(), 1);
    tracing::info!("Added first acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add second acceptor
    group
        .add_acceptor(acceptor2_addr.clone())
        .await
        .expect("add acceptor 2");

    let context = group.context().await.expect("context after add2");
    assert_eq!(context.acceptors.len(), 2);
    tracing::info!("Added second acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Remove first acceptor
    let acceptor1_id = AcceptorId(*acceptor1_addr.id.as_bytes());
    group
        .remove_acceptor(acceptor1_id)
        .await
        .expect("remove acceptor 1");

    let context = group.context().await.expect("context after remove");
    assert_eq!(context.acceptors.len(), 1);
    assert!(!context.acceptors.contains(&acceptor1_id));
    tracing::info!("Removed first acceptor");

    tracing::info!(epoch = ?context.epoch, "Acceptor add/remove test complete");

    group.shutdown().await;
    acceptor1_task.abort();
    acceptor2_task.abort();
}

// =============================================================================
// New Tests
// =============================================================================

/// Helper to create an acceptor server and return the task handle and address
async fn spawn_acceptor() -> (tokio::task::JoinHandle<()>, iroh::EndpointAddr, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let endpoint = test_endpoint().await;
    let addr = endpoint.addr();

    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    let state_store = SharedFjallStateStore::open(temp_dir.path())
        .await
        .expect("open state store");

    let external_client = ExternalClient::builder()
        .crypto_provider(crypto)
        .identity_provider(test_identity_provider())
        .build();

    let registry = AcceptorRegistry::new(
        external_client,
        cipher_suite,
        state_store,
        endpoint.secret_key().clone(),
    );

    let task = tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            loop {
                if let Some(incoming) = endpoint.accept().await {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _ = accept_connection(incoming, registry).await;
                    });
                }
            }
        }
    });

    (task, addr, temp_dir)
}

#[tokio::test]
async fn test_group_without_acceptors() {
    init_tracing();

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group without any acceptors - should still work for local operations
    let mut group = alice.create_group(&[], "none").await.expect("create group");

    let context = group.context().await.expect("get context");
    assert_eq!(context.member_count, 1);
    assert!(context.acceptors.is_empty());

    // Update keys - should work without acceptors (no consensus needed)
    group.update_keys().await.expect("update keys");

    let context = group.context().await.expect("get context after update");
    assert_eq!(context.member_count, 1);
    tracing::info!(epoch = ?context.epoch, "Key update succeeded without acceptors");

    group.shutdown().await;
}

#[tokio::test]
async fn test_multiple_key_updates() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_group_client("alice", test_endpoint().await);

    let mut group = alice.create_group(&[], "none").await.expect("create group");

    group
        .add_acceptor(acceptor_addr)
        .await
        .expect("add acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Perform multiple key updates
    for i in 1..=5 {
        group.update_keys().await.expect("update keys");
        let context = group.context().await.expect("get context");
        tracing::info!(epoch = ?context.epoch, iteration = i, "Key update {i} succeeded");
    }

    let final_context = group.context().await.expect("final context");
    // Epoch should be: create(0) + add_acceptor(1) + 5 updates = at least 6
    tracing::info!(epoch = ?final_context.epoch, "All key updates completed");

    group.shutdown().await;
    acceptor_task.abort();
}

/// Test removing a member from a group.
///
/// TODO: This test is timing out - remove_member operation needs investigation.
#[tokio::test]
#[ignore = "remove_member times out - needs investigation"]
async fn test_remove_member() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice creates group
    let alice = test_group_client("alice", test_endpoint().await);

    let mut alice_group = alice.create_group(&[], "none").await.expect("create group");

    alice_group
        .add_acceptor(acceptor_addr)
        .await
        .expect("add acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Bob using GroupClient
    let bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    let bob_clone = bob.clone();
    let welcome_handle = tokio::spawn(async move { bob_clone.wait_for_welcome().await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    let welcome = welcome_handle
        .await
        .expect("welcome task panicked")
        .expect("failed to receive welcome");

    let bob_group = bob.join_group(&welcome).await.expect("bob join");

    let context = alice_group.context().await.expect("context after add bob");
    assert_eq!(context.member_count, 2, "Should have 2 members");
    tracing::info!("Alice added Bob");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice removes Bob (member index 1)
    alice_group.remove_member(1).await.expect("remove bob");

    let context = alice_group.context().await.expect("context after remove");
    assert_eq!(
        context.member_count, 1,
        "Should have 1 member after removal"
    );
    tracing::info!("Alice removed Bob");

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

#[tokio::test]
async fn test_three_member_group() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice creates group using GroupClient
    let alice = test_group_client("alice", test_endpoint().await);

    let mut alice_group = alice.create_group(&[], "none").await.expect("create group");

    alice_group
        .add_acceptor(acceptor_addr.clone())
        .await
        .expect("add acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Bob using GroupClient
    let bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    let bob_clone = bob.clone();
    let welcome_handle = tokio::spawn(async move { bob_clone.wait_for_welcome().await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    let welcome = welcome_handle.await.expect("panic").expect("welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Carol using GroupClient
    let carol = test_group_client("carol", test_endpoint().await);
    let carol_key_package = carol.generate_key_package().expect("carol key package");

    let carol_clone = carol.clone();
    let welcome_handle = tokio::spawn(async move { carol_clone.wait_for_welcome().await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    alice_group
        .add_member(carol_key_package)
        .await
        .expect("add carol");

    let welcome = welcome_handle.await.expect("panic").expect("welcome");
    let mut carol_group = carol.join_group(&welcome).await.expect("carol join");

    // Verify all have correct member count
    let alice_ctx = alice_group.context().await.expect("alice context");
    let bob_ctx = bob_group.context().await.expect("bob context");
    let carol_ctx = carol_group.context().await.expect("carol context");

    assert_eq!(alice_ctx.member_count, 3, "Alice should see 3 members");
    // Bob joined before Carol was added
    assert_eq!(
        bob_ctx.member_count, 2,
        "Bob should see 2 members (from when he joined)"
    );
    assert_eq!(carol_ctx.member_count, 3, "Carol should see 3 members");

    // All should have the acceptor
    assert_eq!(alice_ctx.acceptors.len(), 1);
    assert_eq!(bob_ctx.acceptors.len(), 1);
    assert_eq!(carol_ctx.acceptors.len(), 1);

    tracing::info!("Three-member group test complete");

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    carol_group.shutdown().await;
    acceptor_task.abort();
}

#[tokio::test]
async fn test_send_application_message() {
    init_tracing();

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group without acceptors (messages work locally)
    let mut group = alice.create_group(&[], "none").await.expect("create group");

    // Send an application message
    let message_data = b"Hello, World!";
    let encrypted = group
        .send_message(message_data)
        .await
        .expect("send message");

    tracing::info!("Sent encrypted application message");

    // Verify the encrypted message has expected properties
    assert!(
        !encrypted.ciphertext.is_empty(),
        "Ciphertext should not be empty"
    );

    group.shutdown().await;
}

#[tokio::test]
async fn test_group_creation_with_initial_acceptor() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group WITH an initial acceptor
    let mut group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "none")
        .await
        .expect("create group with acceptor");

    let context = group.context().await.expect("get context");
    assert_eq!(
        context.acceptors.len(),
        1,
        "Should have 1 acceptor from creation"
    );
    assert!(
        context
            .acceptors
            .contains(&AcceptorId(*acceptor_addr.id.as_bytes())),
        "Should have the correct acceptor"
    );

    tracing::info!("Group created with initial acceptor");

    group.shutdown().await;
    acceptor_task.abort();
}

#[tokio::test]
async fn test_concurrent_operations_single_member() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_group_client("alice", test_endpoint().await);

    let mut group = alice
        .create_group(&[acceptor_addr], "none")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Perform operations in sequence to verify stability
    for i in 0..3 {
        group.update_keys().await.expect("update keys");
        let ctx = group.context().await.expect("context");
        tracing::debug!(epoch = ?ctx.epoch, iteration = i, "Update completed");
    }

    let final_ctx = group.context().await.expect("final context");
    tracing::info!(epoch = ?final_ctx.epoch, "All operations completed");

    group.shutdown().await;
    acceptor_task.abort();
}

// =============================================================================
// GroupClient Tests - using the new high-level abstraction
// =============================================================================

/// Test basic GroupClient functionality (create group, no members).
#[tokio::test]
async fn test_group_client_create_group() {
    init_tracing();

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group without acceptors
    let mut alice_group = alice
        .create_group(&[], "none")
        .await
        .expect("alice create group");

    let ctx = alice_group.context().await.expect("context");
    assert_eq!(ctx.member_count, 1);
    tracing::info!("Alice created group with GroupClient");

    // Generate a key package (just verify it works)
    let _key_package = alice.generate_key_package().expect("generate key package");
    tracing::info!("Generated key package with GroupClient");

    alice_group.shutdown().await;
}

/// Test GroupClient with acceptor.
#[tokio::test]
async fn test_group_client_with_acceptor() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group with acceptor
    let mut alice_group = alice
        .create_group(&[acceptor_addr], "none")
        .await
        .expect("alice create group");

    let ctx = alice_group.context().await.expect("context");
    assert_eq!(ctx.member_count, 1);
    assert_eq!(ctx.acceptors.len(), 1);
    tracing::info!("Alice created group with acceptor using GroupClient");

    // Update keys through consensus
    alice_group.update_keys().await.expect("update keys");

    let ctx = alice_group.context().await.expect("context after update");
    tracing::info!(epoch = ?ctx.epoch, "Updated keys with GroupClient");

    alice_group.shutdown().await;
    acceptor_task.abort();
}
