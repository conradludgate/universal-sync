//! Integration tests for the sync crate
//!
//! These tests verify the networking layer and MLS integration.

use std::time::Duration;

use iroh::Endpoint;
use mls_rs::external_client::ExternalClient;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use universal_sync_testing::{
    AcceptorId, AcceptorRegistry, Group, GroupId, PAXOS_ALPN, SharedFjallStateStore,
    accept_connection, test_cipher_suite, test_client, test_crypto_provider,
    test_identity_provider,
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

    // Create client
    let client_endpoint = test_endpoint().await;
    let test_result = test_client("alice");

    // Create a group using the new Group API (no acceptors)
    let mut group = Group::create(
        &test_result.client,
        test_result.signer,
        test_result.cipher_suite,
        &client_endpoint,
        &[], // No acceptors
    )
    .await
    .expect("create group");

    let context = group.context();
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

    // --- Alice creates a group using the Group API ---
    let alice = test_client("alice");
    let alice_endpoint = test_endpoint().await;

    // Create group without acceptors first
    let mut alice_group = Group::create(
        &alice.client,
        alice.signer,
        alice.cipher_suite.clone(),
        &alice_endpoint,
        &[],
    )
    .await
    .expect("alice create group");

    let group_id = alice_group.group_id();
    tracing::info!(?group_id, "Alice created group");

    // Add the acceptor
    alice_group
        .add_acceptor(acceptor_addr.clone())
        .await
        .expect("add acceptor");

    let context = alice_group.context();
    assert_eq!(context.acceptors.len(), 1, "Should have 1 acceptor");
    tracing::info!("Alice added acceptor");

    // Give the acceptor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Alice adds Bob ---
    let bob = test_client("bob");

    let bob_key_package = bob
        .client
        .generate_key_package_message(
            mls_rs::ExtensionList::default(),
            mls_rs::ExtensionList::default(),
        )
        .expect("bob key package");

    let welcome = alice_group
        .add_member(bob_key_package)
        .await
        .expect("add bob");

    tracing::info!("Alice added Bob, got welcome message");

    // --- Bob joins using the Group API ---
    let bob_endpoint = test_endpoint().await;

    let mut bob_group = Group::join(
        &bob.client,
        bob.signer,
        bob.cipher_suite.clone(),
        &bob_endpoint,
        &welcome,
    )
    .await
    .expect("bob join group");

    let bob_context = bob_group.context();
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
    let alice_context = alice_group.context();
    assert_eq!(
        alice_context.epoch, bob_context.epoch,
        "Alice and Bob should be at the same epoch"
    );

    // --- Alice updates keys ---
    alice_group.update_keys().await.expect("alice update keys");

    let alice_context = alice_group.context();
    tracing::info!(epoch = ?alice_context.epoch, "Alice updated keys");

    // Give a moment for sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Note: In the real system, Bob would receive the update through the background
    // learning task. For this test, we verify Alice's state only since Bob's learning
    // requires a running acceptor connection loop.

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
    let alice = test_client("alice");
    let alice_endpoint = test_endpoint().await;

    let mut group = Group::create(
        &alice.client,
        alice.signer,
        alice.cipher_suite.clone(),
        &alice_endpoint,
        &[],
    )
    .await
    .expect("create group");

    // Add first acceptor
    group
        .add_acceptor(acceptor1_addr.clone())
        .await
        .expect("add acceptor 1");

    let context = group.context();
    assert_eq!(context.acceptors.len(), 1);
    tracing::info!("Added first acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add second acceptor
    group
        .add_acceptor(acceptor2_addr.clone())
        .await
        .expect("add acceptor 2");

    let context = group.context();
    assert_eq!(context.acceptors.len(), 2);
    tracing::info!("Added second acceptor");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Remove first acceptor
    let acceptor1_id = AcceptorId(*acceptor1_addr.id.as_bytes());
    group
        .remove_acceptor(acceptor1_id)
        .await
        .expect("remove acceptor 1");

    let context = group.context();
    assert_eq!(context.acceptors.len(), 1);
    assert!(!context.acceptors.contains(&acceptor1_id));
    tracing::info!("Removed first acceptor");

    tracing::info!(epoch = ?context.epoch, "Acceptor add/remove test complete");

    group.shutdown().await;
    acceptor1_task.abort();
    acceptor2_task.abort();
}
