//! Tauri application setup and command registration
//!
//! This module provides the concrete types and initialization needed
//! to run the sync-editor as a Tauri application.

#![allow(clippy::missing_errors_doc)]

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use iroh::{Endpoint, RelayMode};
use mls_rs::client_builder::{WithCryptoProvider, WithIdentityProvider};
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::identity::SigningIdentity;
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tokio::sync::RwLock;
use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    CRDT_REGISTRATION_EXTENSION_TYPE, MEMBER_ADDR_EXTENSION_TYPE, PAXOS_ALPN,
    SUPPORTED_CRDTS_EXTENSION_TYPE,
};
use universal_sync_proposer::GroupClient;

use serde_json::json;
use tauri::Emitter;

use crate::{AppState, EditorClient};

/// The cipher suite provider type
pub type AppCipherSuite = <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider;

/// The MLS config type used by the app
pub type AppMlsConfig = WithIdentityProvider<
    BasicIdentityProvider,
    WithCryptoProvider<RustCryptoProvider, mls_rs::client_builder::BaseConfig>,
>;

/// The concrete `EditorClient` type for this app
pub type AppEditorClient = EditorClient<AppMlsConfig, AppCipherSuite>;

/// The concrete `AppState` type for this app
pub type AppAppState = AppState<AppMlsConfig, AppCipherSuite>;

/// Shared app state for Tauri
pub type SharedAppState = Arc<RwLock<AppAppState>>;

/// Default cipher suite
const DEFAULT_CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

/// Create the iroh endpoint for the app
async fn create_endpoint() -> Endpoint {
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
    Endpoint::empty_builder(RelayMode::Default)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind_addr(bind_addr)
        .expect("valid bind address")
        .bind()
        .await
        .expect("failed to create endpoint")
}

/// Create the editor client for the app
fn create_app_client(name: &str, endpoint: Endpoint) -> AppEditorClient {
    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(DEFAULT_CIPHER_SUITE)
        .expect("cipher suite should be available");

    // Generate a signing key pair
    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    let signing_public_key = public_key.as_ref().to_vec();

    // Create a basic credential
    let credential = BasicCredential::new(name.as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    // Build the MLS client
    let mls_client: Client<AppMlsConfig> = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .signing_identity(signing_identity, secret_key.clone(), DEFAULT_CIPHER_SUITE)
        .extension_type(MEMBER_ADDR_EXTENSION_TYPE)
        .extension_type(ACCEPTORS_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_ADD_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_REMOVE_EXTENSION_TYPE)
        .extension_type(SUPPORTED_CRDTS_EXTENSION_TYPE)
        .extension_type(CRDT_REGISTRATION_EXTENSION_TYPE)
        .build();

    // Create the group client
    let group_client = GroupClient::new(mls_client, secret_key, cipher_suite, endpoint);

    EditorClient::new(group_client, signing_public_key)
}

/// Initialize the app state
pub async fn init_app_state() -> SharedAppState {
    let endpoint = create_endpoint().await;
    let addr = endpoint.addr();
    tracing::info!(?addr, "Created iroh endpoint");

    // Use a random name for now (could be configurable)
    let name = format!("user-{}", rand::random::<u32>());
    let client = create_app_client(&name, endpoint);

    Arc::new(RwLock::new(AppState::new(client)))
}

// ============================================================================
// Concrete Tauri Commands
// ============================================================================

use crate::commands::{DeltaCommand, DocumentInfo, parse_group_id};

/// Start the sync loop for a document and emit events to the frontend.
fn spawn_sync_emitter(
    app_handle: tauri::AppHandle,
    group_id_b58: String,
    mut rx: tokio::sync::mpsc::Receiver<crate::DocumentUpdateEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            tracing::debug!(group_id = %group_id_b58, text_len = event.text.len(), "emitting document-updated");
            
            // Emit to frontend
            if let Err(e) = app_handle.emit("document-updated", json!({
                "group_id": group_id_b58,
                "text": event.text,
                "sender": event.sender,
                "epoch": event.epoch,
            })) {
                tracing::warn!(?e, "failed to emit document-updated event");
            }
        }
        tracing::debug!(group_id = %group_id_b58, "sync emitter stopped");
    });
}

#[tauri::command]
pub async fn create_document(
    app_handle: tauri::AppHandle,
    state: tauri::State<'_, SharedAppState>,
) -> Result<DocumentInfo, String> {
    let mut app = state.write().await;
    let acceptors = app.acceptor_addrs();

    let mut doc = app
        .client
        .create_document(&acceptors)
        .await
        .map_err(|e| format!("failed to create document: {e:?}"))?;

    let group_id = doc.group_id().await;
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
    let text = doc.text().await;

    // Start sync loop and emit events
    let rx = doc.start_sync_loop();
    spawn_sync_emitter(app_handle, group_id_b58.clone(), rx);

    app.insert_document(group_id, doc);

    Ok(DocumentInfo {
        group_id: group_id_b58,
        text,
        member_count: 1,
    })
}

#[tauri::command]
pub async fn recv_welcome(state: tauri::State<'_, SharedAppState>) -> Result<Vec<u8>, String> {
    let mut app = state.write().await;

    let welcome = app
        .client
        .recv_welcome()
        .await
        .ok_or_else(|| "welcome channel closed".to_string())?;

    Ok(welcome)
}

#[tauri::command]
pub async fn join_document_bytes(
    app_handle: tauri::AppHandle,
    state: tauri::State<'_, SharedAppState>,
    welcome: Vec<u8>,
) -> Result<DocumentInfo, String> {
    let mut app = state.write().await;

    let mut doc = app
        .client
        .join_document(&welcome)
        .await
        .map_err(|e| format!("failed to join document: {e:?}"))?;

    let group_id = doc.group_id().await;
    let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
    let text = doc.text().await;

    // Start sync loop and emit events
    let rx = doc.start_sync_loop();
    spawn_sync_emitter(app_handle, group_id_b58.clone(), rx);

    app.insert_document(group_id, doc);

    Ok(DocumentInfo {
        group_id: group_id_b58,
        text,
        member_count: 1,
    })
}

#[tauri::command]
pub async fn get_document_text(
    state: tauri::State<'_, SharedAppState>,
    group_id: String,
) -> Result<String, String> {
    let gid = parse_group_id(&group_id)?;
    let app = state.read().await;

    let doc = app
        .get_document(&gid)
        .ok_or_else(|| format!("document not found: {group_id}"))?;

    Ok(doc.text().await)
}

#[tauri::command]
pub async fn apply_delta(
    state: tauri::State<'_, SharedAppState>,
    group_id: String,
    delta: DeltaCommand,
) -> Result<(), String> {
    let gid = parse_group_id(&group_id)?;
    let mut app = state.write().await;

    let doc = app
        .get_document_mut(&gid)
        .ok_or_else(|| format!("document not found: {group_id}"))?;

    doc.apply_delta(delta.into())
        .await
        .map_err(|e| format!("failed to apply delta: {e:?}"))?;

    Ok(())
}

#[tauri::command]
pub async fn get_key_package(state: tauri::State<'_, SharedAppState>) -> Result<String, String> {
    let app = state.read().await;

    let kp = app
        .client
        .generate_key_package()
        .map_err(|e| format!("failed to generate key package: {e:?}"))?;

    let bytes = kp
        .to_bytes()
        .map_err(|e| format!("failed to serialize key package: {e:?}"))?;

    Ok(bs58::encode(bytes).into_string())
}

#[tauri::command]
pub async fn add_member(
    state: tauri::State<'_, SharedAppState>,
    group_id: String,
    key_package_b58: String,
) -> Result<(), String> {
    use mls_rs::MlsMessage;

    let gid = parse_group_id(&group_id)?;
    let kp_bytes = bs58::decode(&key_package_b58)
        .into_vec()
        .map_err(|e| format!("invalid base58: {e}"))?;
    let key_package =
        MlsMessage::from_bytes(&kp_bytes).map_err(|e| format!("invalid key package: {e:?}"))?;

    let mut app = state.write().await;

    let doc = app
        .get_document_mut(&gid)
        .ok_or_else(|| format!("document not found: {group_id}"))?;

    doc.group_mut()
        .await
        .add_member(key_package)
        .await
        .map_err(|e| format!("failed to add member: {e:?}"))?;

    Ok(())
}

/// Add an acceptor to a document's group.
#[tauri::command]
pub async fn add_acceptor(
    state: tauri::State<'_, SharedAppState>,
    group_id: String,
    addr_b58: String,
) -> Result<(), String> {
    use iroh::EndpointAddr;

    let gid = parse_group_id(&group_id)?;
    let addr_bytes = bs58::decode(&addr_b58)
        .into_vec()
        .map_err(|e| format!("invalid base58: {e}"))?;
    let addr: EndpointAddr =
        postcard::from_bytes(&addr_bytes).map_err(|e| format!("invalid address: {e}"))?;

    let mut app = state.write().await;
    
    let doc = app
        .get_document_mut(&gid)
        .ok_or_else(|| format!("document not found: {group_id}"))?;

    doc.group_mut()
        .await
        .add_acceptor(addr)
        .await
        .map_err(|e| format!("failed to add acceptor: {e:?}"))?;

    Ok(())
}

#[tauri::command]
pub async fn list_documents(state: tauri::State<'_, SharedAppState>) -> Result<Vec<String>, String> {
    let app = state.read().await;
    let ids: Vec<String> = app
        .document_ids()
        .iter()
        .map(|id| bs58::encode(id.as_bytes()).into_string())
        .collect();
    Ok(ids)
}
