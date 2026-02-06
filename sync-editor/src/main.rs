//! Sync Editor — a collaborative text editor built on Universal Sync.
//!
//! This is a Tauri v2 app with an actor-based Rust backend:
//!
//! - [`CoordinatorActor`](actor::CoordinatorActor) — owns the `GroupClient`,
//!   routes requests, and manages document actor lifecycle.
//! - [`DocumentActor`](document::DocumentActor) — one per open document,
//!   owns the `Group` and its CRDT, handles edits and remote sync.
//!
//! No mutexes. All shared state is mediated by `mpsc` + `oneshot` channels.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod actor;
mod commands;
mod document;
mod types;

use iroh::{Endpoint, SecretKey};
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tokio::sync::mpsc;
use tracing::info;
use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    CRDT_REGISTRATION_EXTENSION_TYPE, CRDT_SNAPSHOT_EXTENSION_TYPE, MEMBER_ADDR_EXTENSION_TYPE,
    NoCrdtFactory, PAXOS_ALPN, SUPPORTED_CRDTS_EXTENSION_TYPE,
};
use universal_sync_proposer::GroupClient;
use universal_sync_testing::YrsCrdtFactory;

use crate::actor::CoordinatorActor;
use crate::types::{AppState, CoordinatorRequest};

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    // Create the coordinator channel eagerly so AppState is available immediately.
    let (coordinator_tx, coordinator_rx) = mpsc::channel::<CoordinatorRequest>(64);

    tauri::Builder::default()
        .manage(AppState { coordinator_tx })
        .setup(move |app| {
            let handle = app.handle().clone();
            // Spawn the async setup on the Tauri runtime.
            tauri::async_runtime::spawn(async move {
                if let Err(e) = setup_coordinator(coordinator_rx, handle).await {
                    tracing::error!(?e, "coordinator setup failed");
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::create_document,
            commands::get_key_package,
            commands::recv_welcome,
            commands::join_document_bytes,
            commands::apply_delta,
            commands::get_document_text,
            commands::add_member,
            commands::add_acceptor,
            commands::list_acceptors,
            commands::list_peers,
            commands::add_peer,
            commands::remove_member,
            commands::remove_acceptor,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

/// Async setup: create the MLS client, iroh endpoint, and coordinator actor.
async fn setup_coordinator(
    coordinator_rx: mpsc::Receiver<CoordinatorRequest>,
    app_handle: tauri::AppHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    // Generate an ephemeral iroh key
    let iroh_key = SecretKey::generate(&mut rand::rng());
    info!(public_key = %iroh_key.public(), "iroh key generated");

    // Create crypto provider and cipher suite
    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available");

    // Generate MLS signing key
    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    // Create identity — must be unique per instance so multiple peers can join the same group.
    // Use the iroh public key as the identity since it's already unique per instance.
    let credential = BasicCredential::new(iroh_key.public().as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    // Create MLS client with all extension types
    let client = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .signing_identity(
            signing_identity,
            secret_key.clone(),
            CipherSuite::CURVE25519_AES128,
        )
        .extension_type(ACCEPTORS_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_ADD_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_REMOVE_EXTENSION_TYPE)
        .extension_type(MEMBER_ADDR_EXTENSION_TYPE)
        .extension_type(SUPPORTED_CRDTS_EXTENSION_TYPE)
        .extension_type(CRDT_REGISTRATION_EXTENSION_TYPE)
        .extension_type(CRDT_SNAPSHOT_EXTENSION_TYPE)
        .build();

    info!("MLS client created");

    // Create iroh endpoint
    let endpoint = Endpoint::builder()
        .secret_key(iroh_key)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind()
        .await?;

    info!(addr = ?endpoint.addr(), "iroh endpoint ready");

    // Create GroupClient with CRDT factories
    let mut group_client = GroupClient::new(client, secret_key, cipher_suite, endpoint);
    group_client.register_crdt_factory(NoCrdtFactory);
    group_client.register_crdt_factory(YrsCrdtFactory::new());

    // Create and run the coordinator actor (blocks until shutdown)
    let coordinator = CoordinatorActor::new(group_client, coordinator_rx, app_handle);
    coordinator.run().await;

    Ok(())
}
