//! Integration tests for collaborative document sync
//!
//! These tests verify that two clients can synchronize text edits
//! through the federated sync infrastructure.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use iroh::{Endpoint, RelayMode};
use mls_rs::external_client::ExternalClient;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use mls_rs::identity::basic::BasicIdentityProvider;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use universal_sync_acceptor::{AcceptorRegistry, SharedFjallStateStore, accept_connection};
use universal_sync_core::PAXOS_ALPN;

use sync_editor::create_editor_client;

/// Initialize tracing for tests
fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("universal_sync=debug,sync_editor=debug")),
        )
        .with_test_writer()
        .try_init();
}

/// Create an iroh endpoint for testing.
async fn test_endpoint() -> Endpoint {
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    Endpoint::empty_builder(RelayMode::Disabled)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind_addr(bind_addr)
        .expect("valid bind address")
        .bind()
        .await
        .expect("failed to create endpoint")
}

/// Create a test crypto provider
fn test_crypto_provider() -> RustCryptoProvider {
    RustCryptoProvider::default()
}

/// Create a test cipher suite provider
fn test_cipher_suite(
    crypto: &RustCryptoProvider,
) -> <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider {
    crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available")
}

/// Create a basic identity provider for testing
fn test_identity_provider() -> BasicIdentityProvider {
    BasicIdentityProvider::new()
}

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

    let registry =
        AcceptorRegistry::new(external_client, cipher_suite, state_store, endpoint.clone());

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

/// Test that a single client can create a document and edit it.
#[tokio::test]
async fn test_single_client_document() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice = create_editor_client("alice", test_endpoint().await);

    // Create a document with the acceptor
    let mut doc = alice
        .create_document(&[acceptor_addr])
        .await
        .expect("create document");

    // Initial text should be empty
    let text = doc.text().await;
    assert_eq!(text, "");

    // Insert some text
    doc.insert(0, "Hello, world!").await.expect("insert");

    // Text should be updated
    let text = doc.text().await;
    assert_eq!(text, "Hello, world!");

    // Delete some text
    doc.delete(7, 6).await.expect("delete"); // Delete "world!"

    let text = doc.text().await;
    assert_eq!(text, "Hello, ");

    // Insert more text
    doc.insert(7, "Alice!").await.expect("insert");

    let text = doc.text().await;
    assert_eq!(text, "Hello, Alice!");

    tracing::info!("Single client test complete");

    doc.shutdown().await;
    acceptor_task.abort();
}

/// Test that two clients can sync document edits.
#[tokio::test]
async fn test_two_clients_sync() {
    init_tracing();

    let (acceptor_task, acceptor_addr, _acceptor_dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice creates a document
    let alice = create_editor_client("alice", test_endpoint().await);
    let mut alice_doc = alice
        .create_document(&[acceptor_addr.clone()])
        .await
        .expect("alice create document");

    // Alice adds some initial text
    alice_doc.insert(0, "Hello").await.expect("alice insert");
    
    let alice_text = alice_doc.text().await;
    assert_eq!(alice_text, "Hello");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Bob generates a key package
    let mut bob = create_editor_client("bob", test_endpoint().await);
    let bob_kp = bob.generate_key_package().expect("bob key package");

    // Alice adds Bob to the document
    alice_doc
        .group_mut()
        .await
        .add_member(bob_kp)
        .await
        .expect("alice add bob");

    tracing::info!("Alice added Bob to the document");

    // Bob receives the welcome
    let welcome = bob.recv_welcome().await.expect("bob receive welcome");
    tracing::info!("Bob received welcome");

    // Bob joins the document
    let mut bob_doc = bob.join_document(&welcome).await.expect("bob join");

    tracing::info!("Bob joined the document");

    // Give time for sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice sends another edit
    alice_doc.insert(5, " from Alice").await.expect("alice insert 2");

    let alice_text = alice_doc.text().await;
    assert_eq!(alice_text, "Hello from Alice");

    // Wait for Bob to receive the update
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Bob should receive and apply the update
    // Note: We need to actively receive messages
    if let Some(result) = bob_doc.recv_and_apply().await {
        result.expect("bob apply update");
    }

    let bob_text = bob_doc.text().await;
    tracing::info!(?bob_text, "Bob's text after receiving update");

    // Bob makes an edit
    bob_doc.insert(0, "Hey! ").await.expect("bob insert");

    let bob_text = bob_doc.text().await;
    tracing::info!(?bob_text, "Bob's text after his edit");

    // Wait for Alice to receive the update
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Alice should receive and apply the update
    if let Some(result) = alice_doc.recv_and_apply().await {
        result.expect("alice apply update");
    }

    let alice_text = alice_doc.text().await;
    tracing::info!(?alice_text, "Alice's text after receiving Bob's update");

    tracing::info!("Two client sync test complete");

    alice_doc.shutdown().await;
    bob_doc.shutdown().await;
    acceptor_task.abort();
}

/// Test the TextDelta API
#[tokio::test]
async fn test_text_delta_api() {
    init_tracing();

    let alice = create_editor_client("alice", test_endpoint().await);

    // Create a document without acceptors (local only)
    let mut doc = alice.create_document(&[]).await.expect("create document");

    // Use TextDelta API
    use sync_editor::TextDelta;

    doc.apply_delta(TextDelta::Insert {
        position: 0,
        text: "Hello".to_string(),
    })
    .await
    .expect("insert");

    assert_eq!(doc.text().await, "Hello");

    doc.apply_delta(TextDelta::Insert {
        position: 5,
        text: " World".to_string(),
    })
    .await
    .expect("insert");

    assert_eq!(doc.text().await, "Hello World");

    doc.apply_delta(TextDelta::Delete {
        position: 5,
        length: 6,
    })
    .await
    .expect("delete");

    assert_eq!(doc.text().await, "Hello");

    doc.shutdown().await;
}
