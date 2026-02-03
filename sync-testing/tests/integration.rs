//! Integration tests for the sync crate
//!
//! These tests verify the networking layer and MLS integration.

use std::time::Duration;

use iroh::Endpoint;
use mls_rs::external_client::ExternalClient;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use universal_sync_paxos::Learner;
use universal_sync_testing::{
    AcceptorId, AcceptorRegistry, GroupId, GroupMessage, IrohConnector, PAXOS_ALPN,
    SharedFjallStateStore, accept_connection, acceptors_extension, create_group, join_group,
    register_group_with_addr, test_cipher_suite, test_client, test_crypto_provider,
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
async fn test_mls_group_registration() {
    init_tracing();

    // Setup directories
    let acceptor_dir = TempDir::new().unwrap();

    // Create acceptor endpoint and registry
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

    let registry = AcceptorRegistry::new(
        external_client,
        cipher_suite.clone(),
        state_store.clone(),
        acceptor_endpoint.secret_key().clone(),
    );

    // Spawn acceptor server
    let acceptor_task = tokio::spawn({
        let acceptor_endpoint = acceptor_endpoint.clone();
        let registry = registry.clone();
        async move {
            if let Some(incoming) = acceptor_endpoint.accept().await {
                let _ = accept_connection(incoming, registry).await;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Create client
    let client_endpoint = test_endpoint().await;
    let test_result = test_client("alice");

    // Create MLS group
    let group = test_result
        .client
        .create_group_with_id(vec![1; 32], Default::default(), Default::default())
        .expect("create group");

    // Generate GroupInfo
    let group_info_msg = group.group_info_message(true).expect("group info");
    let group_info_bytes = group_info_msg.to_bytes().expect("serialize");

    // Register with acceptor
    let result = register_group_with_addr(&client_endpoint, acceptor_addr, &group_info_bytes).await;
    assert!(result.is_ok(), "registration failed: {:?}", result);

    // Verify group was persisted
    let mls_group_id = group.context().group_id.clone();
    let group_id = GroupId::from_slice(&mls_group_id);

    // Give acceptor time to persist
    tokio::time::sleep(Duration::from_millis(100)).await;

    let stored = state_store.get_group_info(&group_id);
    assert!(stored.is_some(), "group should be persisted");

    acceptor_task.abort();
}

#[tokio::test]
async fn test_alice_adds_bob_with_paxos() {
    use universal_sync_paxos::config::ProposerConfig;
    use universal_sync_paxos::proposer::Proposer;

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

    // --- Alice creates a group using the flow helper ---
    let alice = test_client("alice");
    let alice_endpoint = test_endpoint().await;

    // let acceptor_id = AcceptorId::from_bytes(*acceptor_addr.id.as_bytes());
    let acceptors = [acceptor_addr.clone()];

    let mut created = create_group(
        &alice.client,
        alice.signer,
        alice.cipher_suite.clone(),
        &alice_endpoint,
        &acceptors,
    )
    .await
    .expect("alice create group");

    tracing::info!(group_id = ?created.group_id, "Alice created group");

    // Give the acceptor time to process the registration
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Setup Paxos proposer for Alice ---
    let connector = IrohConnector::new(alice_endpoint.clone(), created.group_id);
    let mut proposer = Proposer::new(
        created.learner.node_id(),
        connector,
        ProposerConfig::default(),
    );
    proposer.sync_actors(created.learner.acceptor_ids());

    // --- Alice adds Bob ---
    let bob = test_client("bob");

    let bob_key_package = bob
        .client
        .generate_key_package_message(
            mls_rs::ExtensionList::default(),
            mls_rs::ExtensionList::default(),
        )
        .expect("bob key package");

    // Build the commit with acceptors in GroupInfo extensions
    let acceptors_ext = acceptors_extension(created.learner.acceptors().values().cloned());

    let commit_output = created
        .learner
        .group_mut()
        .commit_builder()
        .add_member(bob_key_package)
        .expect("add member")
        .set_group_info_ext(acceptors_ext)
        .build()
        .expect("build commit");

    // Wrap the commit message for Paxos
    let commit_message = GroupMessage::new(commit_output.commit_message.clone());

    // Use Paxos to get consensus on the commit
    let (proposal, message) = proposer
        .propose(&created.learner, commit_message)
        .await
        .expect("paxos consensus");

    tracing::info!(?proposal, "Paxos consensus reached for adding Bob");

    // Apply the learned message to Alice's learner
    created
        .learner
        .apply(proposal, message)
        .await
        .expect("apply to alice");

    // Get the Welcome message for Bob
    let welcome = commit_output
        .welcome_messages
        .first()
        .expect("should have welcome");
    let welcome_bytes = welcome.to_bytes().expect("serialize welcome");

    // --- Bob joins using the flow helper ---
    let joined = join_group(
        &bob.client,
        bob.signer,
        bob.cipher_suite.clone(),
        &welcome_bytes,
    )
    .await
    .expect("bob join group");

    tracing::info!(group_id = ?joined.group_id, "Bob joined group");

    // Verify Bob got the acceptors from the GroupInfo
    assert_eq!(
        joined.learner.acceptors().len(),
        1,
        "Bob should have 1 acceptor"
    );
    assert!(
        joined
            .learner
            .acceptors()
            .contains_key(&AcceptorId(*acceptor_addr.id.as_bytes())),
        "Bob should have the acceptor ID"
    );

    // Verify both Alice and Bob are at the same epoch
    let epoch_after_join = created.learner.mls_epoch();
    assert_eq!(
        epoch_after_join,
        joined.learner.mls_epoch(),
        "Alice and Bob should be at the same epoch"
    );

    // --- Alice makes an UpdateKeys commit ---
    tracing::info!("Alice creating UpdateKeys commit");

    // Build an empty commit (still produces a valid MLS commit that advances epoch)
    let update_commit = created
        .learner
        .group_mut()
        .commit_builder()
        .build()
        .expect("build update commit");

    // Wrap the commit for Paxos
    let update_message = GroupMessage::new(update_commit.commit_message.clone());

    // Use Paxos to get consensus
    let (update_proposal, learned_message) = proposer
        .propose(&created.learner, update_message)
        .await
        .expect("paxos consensus for update");

    tracing::info!(?update_proposal, "Paxos consensus reached for UpdateKeys");

    // Apply to Alice
    created
        .learner
        .apply(update_proposal.clone(), learned_message)
        .await
        .expect("apply update to alice");

    let epoch_after_update = created.learner.mls_epoch();
    assert!(
        epoch_after_update > epoch_after_join,
        "Epoch should have increased after update"
    );

    // --- Bob connects to acceptor and learns the update ---
    tracing::info!("Bob connecting to acceptor to learn updates");

    // Give a moment for the acceptor to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bob_endpoint = test_endpoint().await;
    // Bob needs address hints since iroh discovery isn't available in tests
    let bob_connector = IrohConnector::new(bob_endpoint.clone(), joined.group_id);
    let mut bob_proposer = Proposer::new(
        joined.learner.node_id(),
        bob_connector,
        ProposerConfig::default(),
    );
    bob_proposer.sync_actors(joined.learner.acceptor_ids());

    // Start sync to receive historical values
    bob_proposer.start_sync(&joined.learner);

    // Learn the update from the acceptor
    let mut joined = joined; // Make mutable for apply
    let (learned_proposal, learned_msg) = bob_proposer
        .learn_one(&joined.learner)
        .await
        .expect("bob should learn the update");

    tracing::info!(?learned_proposal, "Bob learned update from acceptor");

    // Apply the learned message to Bob (it's not Bob's proposal, it's Alice's)
    joined
        .learner
        .apply(learned_proposal, learned_msg)
        .await
        .expect("apply update to bob");

    // Verify Bob is now at the same epoch as Alice
    assert_eq!(
        created.learner.mls_epoch(),
        joined.learner.mls_epoch(),
        "Alice and Bob should be at the same epoch after update"
    );

    tracing::info!(
        epoch = ?created.learner.mls_epoch(),
        "Test complete: Alice and Bob synchronized via Paxos"
    );

    acceptor_task.abort();
}
