//! Filament Editor — a collaborative text editor built on Filament.
//! Actor-based Tauri v2 app: no mutexes, all state mediated by channels.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod actor;
mod commands;
mod document;
mod types;

use filament_core::{PAXOS_ALPN, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE};
use filament_weave::GroupClient;
use iroh::{Endpoint, SecretKey};
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tokio::sync::mpsc;
use tracing::info;

use crate::actor::CoordinatorActor;
use crate::types::{AppState, CoordinatorRequest};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let (coordinator_tx, coordinator_rx) = mpsc::channel::<CoordinatorRequest>(64);

    tauri::Builder::default()
        .manage(AppState { coordinator_tx })
        .setup(move |app| {
            let handle = app.handle().clone();
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
            commands::get_group_state,
            commands::update_keys,
            commands::update_cursor,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

async fn setup_coordinator(
    coordinator_rx: mpsc::Receiver<CoordinatorRequest>,
    app_handle: tauri::AppHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let iroh_key = SecretKey::generate(&mut rand::rng());
    info!(public_key = %iroh_key.public(), "iroh key generated");

    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available");

    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    // Use iroh public key as identity since it's already unique per instance
    let credential = BasicCredential::new(iroh_key.public().as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    let client = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .signing_identity(
            signing_identity,
            secret_key.clone(),
            CipherSuite::CURVE25519_AES128,
        )
        .extension_type(SYNC_EXTENSION_TYPE)
        .custom_proposal_type(SYNC_PROPOSAL_TYPE)
        .build();

    info!("MLS client created");

    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()))
        .build();

    let endpoint = Endpoint::builder()
        .transport_config(transport_config)
        .secret_key(iroh_key)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind()
        .await?;

    info!(addr = ?endpoint.addr(), "iroh endpoint ready");

    let group_client = GroupClient::new(client, secret_key, cipher_suite, endpoint);

    let coordinator = CoordinatorActor::new(group_client, coordinator_rx, app_handle);
    coordinator.run().await;

    Ok(())
}
