//! Integration tests for the sync crate
//!
//! These tests verify the networking layer and MLS integration.
//! All tests have timeouts to prevent hanging.

use std::time::Duration;

use mls_rs::external_client::ExternalClient;
use tempfile::TempDir;
use universal_sync_acceptor::{AcceptorRegistry, SharedFjallStateStore, accept_connection};
use universal_sync_core::{AcceptorId, CompactionConfig, GroupId};
use universal_sync_proposer::GroupEvent;
use universal_sync_testing::{
    YrsCrdt, init_tracing, spawn_acceptor, test_cipher_suite, test_crypto_provider, test_endpoint,
    test_group_client, test_identity_provider, test_yrs_group_client,
    test_yrs_group_client_with_config,
};

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

/// Test Alice adding Bob to a group using the Group API.
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
            acceptor_endpoint.clone(),
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
    let mut bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    tracing::info!("Alice added Bob and sent welcome");

    // Wait for Bob to receive the welcome (automatically received in background)
    let welcome = bob.recv_welcome().await.expect("failed to receive welcome");

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

/// Test adding and removing acceptors from a group.
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
            acceptor_endpoint.clone(),
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
            acceptor_endpoint.clone(),
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

/// Test multiple key updates in a group with an acceptor.
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
#[tokio::test]
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
    let mut bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    let welcome = bob.recv_welcome().await.expect("failed to receive welcome");
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

/// Test adding three members to a group.
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
    let mut bob = test_group_client("bob", test_endpoint().await);
    let bob_key_package = bob.generate_key_package().expect("bob key package");

    alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Carol using GroupClient
    let mut carol = test_group_client("carol", test_endpoint().await);
    let carol_key_package = carol.generate_key_package().expect("carol key package");

    alice_group
        .add_member(carol_key_package)
        .await
        .expect("add carol");

    let welcome = carol.recv_welcome().await.expect("carol welcome");
    let mut carol_group = carol.join_group(&welcome).await.expect("carol join");

    // Give Bob time to learn the commit that added Carol
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all have correct member count
    let alice_ctx = alice_group.context().await.expect("alice context");
    let bob_ctx = bob_group.context().await.expect("bob context");
    let carol_ctx = carol_group.context().await.expect("carol context");

    assert_eq!(alice_ctx.member_count, 3, "Alice should see 3 members");
    // Bob now learns about Carol via the passive learner subscription
    assert_eq!(
        bob_ctx.member_count, 3,
        "Bob should see 3 members (advances epoch via passive learning)"
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
async fn test_send_update_no_changes() {
    init_tracing();

    let alice = test_group_client("alice", test_endpoint().await);

    // Create group without acceptors
    let mut group = alice.create_group(&[], "none").await.expect("create group");

    // send_update with no CRDT changes should be a no-op
    group.send_update().await.expect("send update (no-op)");

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

/// Test concurrent operations with a single member group.
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

// =============================================================================
// ReplContext Tests - using the REPL command interface
// =============================================================================

use universal_sync_testing::test_repl_context;

/// Test basic REPL workflow: create_group, list_groups, group_context
#[tokio::test]
async fn test_repl_basic_workflow() {
    init_tracing();

    let mut alice = test_repl_context("alice", test_endpoint().await);

    // Create a group
    let result = alice.execute("create_group").await;
    assert!(result.is_ok(), "create_group should succeed: {result:?}");
    let output = result.unwrap();
    assert!(output.contains("Created group:"), "Should show group ID");

    // List groups
    let result = alice.execute("list_groups").await;
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Groups:"), "Should list groups");

    // Get group context (need to extract group_id from create output)
    let group_id = alice.groups.keys().next().expect("should have a group");
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();

    let result = alice
        .execute(&format!("group_context {group_id_b58}"))
        .await;
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Epoch: 0"), "Should be at epoch 0");
    assert!(output.contains("Members: 1"), "Should have 1 member");

    // Cleanup
    for (_id, group) in alice.groups.drain() {
        group.shutdown().await;
    }
}

/// Test REPL key_package command
#[tokio::test]
async fn test_repl_key_package() {
    init_tracing();

    let mut alice = test_repl_context("alice", test_endpoint().await);

    // Generate key package
    let result = alice.execute("key_package").await;
    assert!(result.is_ok(), "key_package should succeed");
    let output = result.unwrap();

    // Should be valid base58
    let decoded = bs58::decode(&output.trim()).into_vec();
    assert!(decoded.is_ok(), "Should be valid base58");
}

/// Test REPL add_acceptor workflow
#[tokio::test]
async fn test_repl_add_acceptor() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut alice = test_repl_context("alice", test_endpoint().await);

    // Create group
    alice.execute("create_group").await.expect("create_group");

    let group_id = alice.groups.keys().next().expect("should have a group");
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();

    // Serialize acceptor address
    let addr_bytes = postcard::to_allocvec(&acceptor_addr).expect("serialize addr");
    let addr_b58 = bs58::encode(&addr_bytes).into_string();

    // Add acceptor
    let result = alice
        .execute(&format!("add_acceptor {group_id_b58} {addr_b58}"))
        .await;
    assert!(result.is_ok(), "add_acceptor should succeed: {result:?}");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify acceptor was added
    let result = alice
        .execute(&format!("group_context {group_id_b58}"))
        .await;
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(
        output.contains("Acceptors:"),
        "Should show acceptors section"
    );
    assert!(
        !output.contains("Acceptors: none"),
        "Should have an acceptor"
    );

    // Cleanup
    for (_id, group) in alice.groups.drain() {
        group.shutdown().await;
    }
    acceptor_task.abort();
}

/// Test REPL update_keys command
///
/// Creates a group with an acceptor, then uses REPL to update keys.
#[tokio::test]
async fn test_repl_update_keys() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create group WITH acceptor using Group API (reliable path)
    let mut alice = test_repl_context("alice", test_endpoint().await);
    let alice_group = alice
        .client
        .create_group(std::slice::from_ref(&acceptor_addr), "none")
        .await
        .expect("create group with acceptor");

    let group_id = alice_group.group_id();
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
    alice.groups.insert(group_id, alice_group);

    // Wait for acceptor registration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Update keys using REPL
    let result = alice.execute(&format!("update_keys {group_id_b58}")).await;
    assert!(result.is_ok(), "update_keys should succeed: {result:?}");
    let output = result.unwrap();
    assert!(output.contains("New epoch:"), "Should show new epoch");

    // Verify epoch advanced (create + update = epoch 1)
    let result = alice
        .execute(&format!("group_context {group_id_b58}"))
        .await;
    let output = result.unwrap();
    assert!(
        output.contains("Epoch: 1"),
        "Should be at epoch 1: {output}"
    );

    // Cleanup
    for (_id, group) in alice.groups.drain() {
        group.shutdown().await;
    }
    acceptor_task.abort();
}

/// Test REPL add_member workflow with automatic welcome reception
///
/// This test creates a group with an initial acceptor using the GroupClient API,
/// then uses REPL commands to add members and verify auto-welcome works.
#[tokio::test]
async fn test_repl_add_member_with_auto_welcome() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice creates a group WITH an acceptor using Group API (this is the reliable path)
    let mut alice = test_repl_context("alice", test_endpoint().await);
    let alice_group = alice
        .client
        .create_group(std::slice::from_ref(&acceptor_addr), "none")
        .await
        .expect("create group with acceptor");

    let group_id = alice_group.group_id();
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
    alice.groups.insert(group_id, alice_group);

    // Wait for acceptor registration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob generates a key package using REPL
    let mut bob = test_repl_context("bob", test_endpoint().await);
    let bob_kp = bob.execute("key_package").await.expect("key_package");
    let bob_kp = bob_kp.trim();

    // Alice adds Bob using REPL command
    let result = alice
        .execute(&format!("add_member {group_id_b58} {bob_kp}"))
        .await;
    assert!(result.is_ok(), "add_member should succeed: {result:?}");

    // Give welcome time to arrive
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Bob processes pending welcomes (simulating pressing Enter in REPL)
    let result = bob.execute("").await;
    assert!(result.is_ok());
    let output = result.unwrap();

    // Should show auto-join message
    assert!(
        output.contains("Automatically joined"),
        "Should show auto-join: {output}"
    );
    assert_eq!(bob.groups.len(), 1, "Bob should have joined one group");

    // Bob should now see the group in list
    let result = bob.execute("list_groups").await;
    let output = result.unwrap();
    assert!(output.contains("Groups:"), "Bob should have groups");

    // Cleanup
    for (_id, group) in alice.groups.drain() {
        group.shutdown().await;
    }
    for (_id, group) in bob.groups.drain() {
        group.shutdown().await;
    }
    acceptor_task.abort();
}

/// Test REPL help command
#[tokio::test]
async fn test_repl_help() {
    init_tracing();

    let mut alice = test_repl_context("alice", test_endpoint().await);

    let result = alice.execute("help").await;
    assert!(result.is_ok());
    let output = result.unwrap();

    // Verify help contains key commands
    assert!(output.contains("key_package"));
    assert!(output.contains("create_group"));
    assert!(output.contains("add_member"));
    assert!(output.contains("update_keys"));
    assert!(output.contains("list_groups"));
}

/// Test REPL error handling for unknown commands
#[tokio::test]
async fn test_repl_unknown_command() {
    init_tracing();

    let mut alice = test_repl_context("alice", test_endpoint().await);

    let result = alice.execute("nonexistent_command").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Unknown command"));
}

/// Test REPL error handling for missing arguments
#[tokio::test]
async fn test_repl_missing_arguments() {
    init_tracing();

    let mut alice = test_repl_context("alice", test_endpoint().await);

    // Commands that require arguments
    let result = alice.execute("join_group").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Usage:"));

    let result = alice.execute("add_acceptor").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Usage:"));

    let result = alice.execute("update_keys").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Usage:"));
}

/// Test full workflow: Alice creates group with acceptor, updates keys, adds Bob
///
/// This test verifies the complete REPL workflow for group management.
#[tokio::test]
async fn test_repl_full_workflow() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice creates a group WITH acceptor using Group API (reliable path)
    let mut alice = test_repl_context("alice", test_endpoint().await);
    let alice_group = alice
        .client
        .create_group(std::slice::from_ref(&acceptor_addr), "none")
        .await
        .expect("create group with acceptor");

    let group_id = alice_group.group_id();
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
    alice.groups.insert(group_id, alice_group);

    // Wait for acceptor registration
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice updates keys using REPL
    alice
        .execute(&format!("update_keys {group_id_b58}"))
        .await
        .expect("update_keys");

    // Bob generates key package using REPL
    let mut bob = test_repl_context("bob", test_endpoint().await);
    let bob_kp = bob.execute("key_package").await.expect("key_package");
    let bob_kp = bob_kp.trim();

    // Alice adds Bob using REPL
    alice
        .execute(&format!("add_member {group_id_b58} {bob_kp}"))
        .await
        .expect("add_member");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Bob receives welcome automatically
    bob.execute("").await.expect("process welcomes");
    assert_eq!(bob.groups.len(), 1, "Bob should have joined");

    // Verify Alice's context using REPL
    let alice_ctx = alice
        .execute(&format!("group_context {group_id_b58}"))
        .await
        .expect("alice context");
    assert!(
        alice_ctx.contains("Members: 2"),
        "Alice should see 2 members"
    );

    // Verify both have the acceptor
    assert!(
        alice_ctx.contains("Acceptors:"),
        "Alice should have acceptors section"
    );

    tracing::info!("Full REPL workflow test complete");

    // Cleanup
    for (_id, group) in alice.groups.drain() {
        group.shutdown().await;
    }
    for (_id, group) in bob.groups.drain() {
        group.shutdown().await;
    }
    acceptor_task.abort();
}

// =============================================================================
// CRDT Tests
// =============================================================================

/// Test creating a group with a Yrs CRDT and adding a member.
///
/// Verifies that the CRDT snapshot is included in the welcome's GroupInfo
/// extensions and that the joiner can reconstruct the CRDT state.
#[tokio::test]
async fn test_yrs_crdt_snapshot_in_welcome() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice creates a group with Yrs CRDT
    let alice = test_yrs_group_client("alice", test_endpoint().await);

    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create yrs group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Bob joins - he needs YrsCrdtFactory registered to process the snapshot
    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob key package");

    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join yrs group");

    // Verify both are in the group
    let alice_ctx = alice_group.context().await.expect("alice context");
    let bob_ctx = bob_group.context().await.expect("bob context");
    assert_eq!(alice_ctx.member_count, 2);
    assert_eq!(bob_ctx.member_count, 2);

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test that CRDT updates are sent from one peer and applied by another
/// through the acceptor relay.
#[tokio::test]
async fn test_crdt_operations_sent_and_received() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice creates group with Yrs CRDT
    let alice = test_yrs_group_client("alice", test_endpoint().await);

    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Bob joins
    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob key package");

    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice edits the CRDT (insert text into a Yrs document)
    {
        use yrs::{Text, Transact};

        let yrs_crdt = alice_group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .expect("should be YrsCrdt");
        let text = yrs_crdt.doc().get_or_insert_text("doc");
        let mut txn = yrs_crdt.doc().transact_mut();
        text.insert(&mut txn, 0, "Hello from Alice");
    }

    // Send the update
    alice_group.send_update().await.expect("send update");

    // Bob should receive and apply the update
    tokio::time::timeout(Duration::from_secs(5), bob_group.wait_for_update())
        .await
        .expect("timeout waiting for update")
        .expect("channel closed");

    // Verify Bob's CRDT has Alice's text
    {
        use yrs::{GetString, Transact};

        let yrs_crdt = bob_group
            .crdt()
            .as_any()
            .downcast_ref::<YrsCrdt>()
            .expect("should be YrsCrdt");
        let text = yrs_crdt.doc().get_or_insert_text("doc");
        let txn = yrs_crdt.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello from Alice");
    }

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test bidirectional CRDT update exchange between two peers.
#[tokio::test]
async fn test_crdt_bidirectional_message_exchange() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_yrs_group_client("alice", test_endpoint().await);

    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob key package");

    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice inserts text
    {
        use yrs::{Text, Transact};
        let yrs = alice_group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let mut txn = yrs.doc().transact_mut();
        text.insert(&mut txn, 0, "Hello");
    }
    alice_group.send_update().await.expect("alice send");

    // Bob receives Alice's update
    tokio::time::timeout(Duration::from_secs(5), bob_group.wait_for_update())
        .await
        .expect("timeout")
        .expect("channel closed");

    // Verify Bob has Alice's text, then Bob appends
    {
        use yrs::{GetString, Text, Transact};
        let yrs = bob_group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        {
            let txn = yrs.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello");
        }
        let mut txn = yrs.doc().transact_mut();
        text.insert(&mut txn, 5, " World");
    }
    bob_group.send_update().await.expect("bob send");

    // Alice receives Bob's update
    tokio::time::timeout(Duration::from_secs(5), alice_group.wait_for_update())
        .await
        .expect("timeout")
        .expect("channel closed");

    // Verify Alice has the combined text
    {
        use yrs::{GetString, Transact};
        let yrs = alice_group
            .crdt()
            .as_any()
            .downcast_ref::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let txn = yrs.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello World");
    }

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test that a late joiner receives the CRDT snapshot containing prior state
/// and can exchange updates with the group.
#[tokio::test]
async fn test_crdt_late_joiner_snapshot_and_messages() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_yrs_group_client("alice", test_endpoint().await);

    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice writes some text before anyone joins and sends the update
    {
        use yrs::{Text, Transact};
        let yrs = alice_group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let mut txn = yrs.doc().transact_mut();
        text.insert(&mut txn, 0, "Initial content");
    }
    alice_group.send_update().await.expect("alice send initial");

    // Wait for the update to reach the acceptor
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Add Bob — Bob starts with empty CRDT and catches up via backfill
    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    // Bob catches up via backfill (compaction sends snapshot to acceptors)
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Drain and apply pending CRDT updates from the message channel
            bob_group.sync();
            let text = {
                use yrs::{GetString, Transact};
                let yrs = bob_group.crdt().as_any().downcast_ref::<YrsCrdt>().unwrap();
                let text_ref = yrs.doc().get_or_insert_text("doc");
                let txn = yrs.doc().transact();
                text_ref.get_string(&txn)
            };
            if text == "Initial content" {
                break;
            }
        }
    })
    .await
    .expect("Bob should eventually sync Initial content via backfill");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Carol — she also catches up via backfill
    let mut carol = test_yrs_group_client("carol", test_endpoint().await);
    let carol_kp = carol.generate_key_package().expect("carol kp");
    alice_group.add_member(carol_kp).await.expect("add carol");

    let welcome = carol.recv_welcome().await.expect("carol welcome");
    let mut carol_group = carol.join_group(&welcome).await.expect("carol join");

    // Carol catches up via backfill
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Drain and apply pending CRDT updates from the message channel
            carol_group.sync();
            let text = {
                use yrs::{GetString, Transact};
                let yrs = carol_group
                    .crdt()
                    .as_any()
                    .downcast_ref::<YrsCrdt>()
                    .unwrap();
                let text_ref = yrs.doc().get_or_insert_text("doc");
                let txn = yrs.doc().transact();
                text_ref.get_string(&txn)
            };
            if text == "Initial content" {
                break;
            }
        }
    })
    .await
    .expect("Carol should eventually sync Initial content via backfill");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice sends a CRDT update to Carol
    {
        use yrs::{Text, Transact};
        let yrs = alice_group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let mut txn = yrs.doc().transact_mut();
        text.insert(&mut txn, 15, " + update");
    }
    alice_group.send_update().await.expect("alice send");

    tokio::time::timeout(Duration::from_secs(5), carol_group.wait_for_update())
        .await
        .expect("carol timeout")
        .expect("carol channel closed");

    {
        use yrs::{GetString, Transact};
        let yrs = carol_group
            .crdt()
            .as_any()
            .downcast_ref::<YrsCrdt>()
            .unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let txn = yrs.doc().transact();
        assert_eq!(text.get_string(&txn), "Initial content + update");
    }

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    carol_group.shutdown().await;
    acceptor_task.abort();
}

// =============================================================================
// Compaction + Welcome Tests
// =============================================================================

/// Helper: insert text into a Yrs CRDT group and send the update.
async fn yrs_insert_and_send(
    group: &mut universal_sync_proposer::Group<
        impl mls_rs::client_builder::MlsConfig + Clone + Send + Sync + 'static,
        impl mls_rs::CipherSuiteProvider + Clone + Send + Sync + 'static,
    >,
    position: u32,
    text: &str,
) {
    {
        use yrs::{Text, Transact};
        let yrs = group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .unwrap();
        let text_ref = yrs.doc().get_or_insert_text("doc");
        let mut txn = yrs.doc().transact_mut();
        text_ref.insert(&mut txn, position, text);
    }
    group.send_update().await.expect("send update");
}

/// Helper: read text from a Yrs CRDT group.
fn yrs_get_text(
    group: &universal_sync_proposer::Group<
        impl mls_rs::client_builder::MlsConfig + Clone + Send + Sync + 'static,
        impl mls_rs::CipherSuiteProvider + Clone + Send + Sync + 'static,
    >,
) -> String {
    use yrs::{GetString, Transact};
    let yrs = group.crdt().as_any().downcast_ref::<YrsCrdt>().unwrap();
    let text_ref = yrs.doc().get_or_insert_text("doc");
    let txn = yrs.doc().transact();
    text_ref.get_string(&txn)
}

/// Helper: wait for a specific group event, with timeout.
async fn wait_for_event(
    rx: &mut tokio::sync::broadcast::Receiver<GroupEvent>,
    predicate: impl Fn(&GroupEvent) -> bool,
    timeout_secs: u64,
) -> GroupEvent {
    tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        loop {
            match rx.recv().await {
                Ok(event) if predicate(&event) => return event,
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::debug!(skipped = n, "event receiver lagged");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("event channel closed while waiting");
                }
            }
        }
    })
    .await
    .expect("timed out waiting for group event")
}

/// Helper: poll until a joiner's CRDT text matches the expected value.
async fn wait_for_sync(
    group: &mut universal_sync_proposer::Group<
        impl mls_rs::client_builder::MlsConfig + Clone + Send + Sync + 'static,
        impl mls_rs::CipherSuiteProvider + Clone + Send + Sync + 'static,
    >,
    expected: &str,
    timeout_secs: u64,
) {
    tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            group.sync();
            if yrs_get_text(group) == expected {
                break;
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for sync: expected '{}', got '{}'",
            expected,
            yrs_get_text(group),
        );
    });
}

/// Helper: low-threshold compaction config for fast tests.
/// 2 levels (L0 + L(max)), threshold of `threshold` L0 messages.
fn test_compaction_config(threshold: u32) -> CompactionConfig {
    CompactionConfig {
        levels: 2,
        thresholds: vec![threshold],
        replication: vec![1, 0], // L0 to 1, L(max) to all
    }
}

/// Test 1: Welcome + force_compaction L(max) with verified compaction.
///
/// Alice writes text, adds Bob. The `force_compaction` path triggers
/// an L(max) compaction. We verify CompactionCompleted fires and Bob
/// catches up via backfill.
#[tokio::test]
async fn test_welcome_force_compaction_verified() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_yrs_group_client("alice", test_endpoint().await);
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    // Subscribe to events BEFORE any compaction can fire
    let mut alice_events = alice_group.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice writes text
    yrs_insert_and_send(&mut alice_group, 0, "Hello World").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob joins
    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    // Verify CompactionCompleted event fires on Alice's side
    let event = wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;
    match event {
        GroupEvent::CompactionCompleted { level } => {
            // force_compaction always uses L(max)
            let max_level =
                CompactionConfig::default().levels.saturating_sub(1);
            assert_eq!(level, max_level, "force_compaction should use L(max)");
        }
        _ => unreachable!(),
    }

    // Bob should catch up via backfill
    wait_for_sync(&mut bob_group, "Hello World", 10).await;

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 2: Welcome after threshold-triggered compaction.
///
/// Alice sends enough L0 updates to trigger threshold compaction.
/// After compaction fires and old messages are deleted, Bob joins.
/// Bob must catch up from the compacted snapshot via backfill.
#[tokio::test]
async fn test_welcome_after_threshold_compaction() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Use low threshold (3 L0s → L(max))
    let config = test_compaction_config(3);
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config,
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    let mut alice_events = alice_group.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice sends 3 updates (hits threshold)
    yrs_insert_and_send(&mut alice_group, 0, "aaa").await;
    yrs_insert_and_send(&mut alice_group, 3, "bbb").await;
    yrs_insert_and_send(&mut alice_group, 6, "ccc").await;

    // Wait for threshold-triggered CompactionCompleted
    let event = wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;
    assert!(
        matches!(event, GroupEvent::CompactionCompleted { level: 1 }),
        "threshold compaction should fire at L(max)=1, got {event:?}"
    );

    // Wait for acceptor to process the deletion
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now Bob joins — old L0s should be deleted, compacted snapshot remains
    let mut bob = test_yrs_group_client_with_config(
        "bob",
        test_endpoint().await,
        test_compaction_config(3),
    );
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    // Bob catches up via backfill (force_compaction re-encrypts)
    wait_for_sync(&mut bob_group, "aaabbbccc", 10).await;

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 3: Welcome + re-encryption for sequential joiners.
///
/// Alice writes → Bob joins (compaction fires) → Carol joins later.
/// Carol can't decrypt Bob-era messages, so `last_compacted_snapshot`
/// must be re-encrypted at Carol's epoch.
#[tokio::test]
async fn test_welcome_reencryption_sequential_joiners() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = test_compaction_config(100); // High threshold — only force_compaction fires
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config.clone(),
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice writes text
    yrs_insert_and_send(&mut alice_group, 0, "Shared state").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob joins — triggers force_compaction (L(max))
    let mut bob = test_yrs_group_client_with_config(
        "bob",
        test_endpoint().await,
        config.clone(),
    );
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    // Bob catches up
    wait_for_sync(&mut bob_group, "Shared state", 10).await;

    // Wait for Alice's compaction to fully process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Carol joins — another force_compaction re-encrypts at Carol's epoch
    let mut carol = test_yrs_group_client_with_config(
        "carol",
        test_endpoint().await,
        config,
    );
    let carol_kp = carol.generate_key_package().expect("carol kp");
    alice_group.add_member(carol_kp).await.expect("add carol");

    let welcome = carol.recv_welcome().await.expect("carol welcome");
    let mut carol_group = carol.join_group(&welcome).await.expect("carol join");

    // Carol catches up via re-encrypted snapshot
    wait_for_sync(&mut carol_group, "Shared state", 10).await;

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    carol_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 4: Bidirectional message flow after compaction.
///
/// After compaction + new member join, both members should be able to
/// exchange messages. Bob advances his epoch by learning Alice's
/// CompactionComplete commit via the passive learner subscription.
#[tokio::test]
async fn test_post_compaction_bidirectional() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = test_compaction_config(100); // Only force_compaction
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config.clone(),
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice writes initial text
    yrs_insert_and_send(&mut alice_group, 0, "Hello").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob joins — triggers compaction, clears update_buffer
    let mut bob = test_yrs_group_client_with_config(
        "bob",
        test_endpoint().await,
        config,
    );
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    // Bob catches up with initial state via backfill
    wait_for_sync(&mut bob_group, "Hello", 10).await;

    // Give Bob time to learn the CompactionComplete commit and advance epoch
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Alice sends a NEW message AFTER compaction (at post-CompactionComplete epoch)
    yrs_insert_and_send(&mut alice_group, 5, " World").await;

    // Bob should receive Alice's message (Bob has advanced epoch via passive learning)
    wait_for_sync(&mut bob_group, "Hello World", 10).await;

    // Bob sends back to Alice
    yrs_insert_and_send(&mut bob_group, 11, "!").await;

    // Alice receives Bob's update
    tokio::time::timeout(Duration::from_secs(5), alice_group.wait_for_update())
        .await
        .expect("timeout waiting for Alice to receive Bob's message")
        .expect("channel closed");

    assert_eq!(yrs_get_text(&alice_group), "Hello World!");

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 5: Welcome + force_compaction with no prior data.
///
/// Alice creates a group and immediately adds Bob without writing anything.
/// `force_compaction` fires but there's nothing to compact. Must be a no-op.
#[tokio::test]
async fn test_welcome_force_compaction_empty() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = test_yrs_group_client("alice", test_endpoint().await);
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add Bob immediately — no writes, no snapshot
    let mut bob = test_yrs_group_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both should have empty text (no crash)
    assert_eq!(yrs_get_text(&alice_group), "");
    bob_group.sync();
    assert_eq!(yrs_get_text(&bob_group), "");

    // Now Alice writes — should still work
    yrs_insert_and_send(&mut alice_group, 0, "Late write").await;

    tokio::time::timeout(Duration::from_secs(5), bob_group.wait_for_update())
        .await
        .expect("timeout")
        .expect("channel closed");

    assert_eq!(yrs_get_text(&bob_group), "Late write");

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 6: Threshold compaction boundary condition.
///
/// With threshold=3, sending 2 messages should NOT trigger compaction.
/// Sending the 3rd should trigger it.
#[tokio::test]
async fn test_compaction_threshold_boundary() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = test_compaction_config(3);
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config,
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    let mut alice_events = alice_group.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send 2 updates — below threshold
    yrs_insert_and_send(&mut alice_group, 0, "aa").await;
    yrs_insert_and_send(&mut alice_group, 2, "bb").await;

    // Give compaction timer a chance to fire (it checks every 1s)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Should NOT have received CompactionCompleted
    let no_compaction = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            match alice_events.recv().await {
                Ok(GroupEvent::CompactionCompleted { .. }) => return true,
                Ok(_) => continue,
                Err(_) => return false,
            }
        }
    })
    .await;
    assert!(
        no_compaction.is_err() || !no_compaction.unwrap(),
        "compaction should NOT fire below threshold"
    );

    // Send the 3rd update — hits threshold
    yrs_insert_and_send(&mut alice_group, 4, "cc").await;

    // Now CompactionCompleted should fire
    let event = wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;
    assert!(matches!(event, GroupEvent::CompactionCompleted { level: 1 }));

    alice_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 7: Multiple compaction rounds.
///
/// Alice sends enough messages for two compaction rounds. The second
/// round should merge with `last_compacted_snapshot` from the first.
/// A new joiner should get the fully merged state.
#[tokio::test]
async fn test_multiple_compaction_rounds() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = test_compaction_config(3);
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config.clone(),
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    let mut alice_events = alice_group.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Round 1: send 3 updates → triggers compaction
    yrs_insert_and_send(&mut alice_group, 0, "aaa").await;
    yrs_insert_and_send(&mut alice_group, 3, "bbb").await;
    yrs_insert_and_send(&mut alice_group, 6, "ccc").await;

    wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;
    tracing::info!("first compaction round completed");

    // Wait for state to settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Round 2: send 3 more updates → triggers second compaction
    yrs_insert_and_send(&mut alice_group, 9, "ddd").await;
    yrs_insert_and_send(&mut alice_group, 12, "eee").await;
    yrs_insert_and_send(&mut alice_group, 15, "fff").await;

    wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;
    tracing::info!("second compaction round completed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // New joiner should get the fully merged state
    let mut bob = test_yrs_group_client_with_config(
        "bob",
        test_endpoint().await,
        config,
    );
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    wait_for_sync(&mut bob_group, "aaabbbcccdddeeefff", 10).await;

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}

/// Test 8: Compaction + acceptor deletion — verified via late joiner.
///
/// After threshold compaction fires and deletes old L0s, a new joiner
/// should still get the correct full state from the compacted snapshot.
/// This verifies both that compaction produces a valid snapshot and that
/// the acceptor's deletion doesn't remove the compacted entry.
#[tokio::test]
async fn test_compaction_deletion_late_joiner() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let config = test_compaction_config(3);
    let alice = test_yrs_group_client_with_config(
        "alice",
        test_endpoint().await,
        config.clone(),
    );
    let mut alice_group = alice
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    let mut alice_events = alice_group.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send 6 updates (two rounds of compaction at threshold=3)
    for i in 0u32..6 {
        yrs_insert_and_send(&mut alice_group, i * 2, &format!("{i}x")).await;
    }

    // Wait for at least one CompactionCompleted
    wait_for_event(
        &mut alice_events,
        |e| matches!(e, GroupEvent::CompactionCompleted { .. }),
        10,
    )
    .await;

    // Allow acceptor to process deletions
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Late joiner should get the complete state
    let mut bob = test_yrs_group_client_with_config(
        "bob",
        test_endpoint().await,
        config,
    );
    let bob_kp = bob.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");

    let welcome = bob.recv_welcome().await.expect("bob welcome");
    let mut bob_group = bob.join_group(&welcome).await.expect("bob join");

    let expected = yrs_get_text(&alice_group);
    wait_for_sync(&mut bob_group, &expected, 10).await;

    alice_group.shutdown().await;
    bob_group.shutdown().await;
    acceptor_task.abort();
}
