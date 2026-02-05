//! Tauri commands for the collaborative editor
//!
//! These commands provide the IPC interface between the frontend and backend.
//! They are only compiled when the `tauri` feature is enabled.

use serde::{Deserialize, Serialize};
use universal_sync_core::GroupId;

/// Information about a document returned to the frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentInfo {
    /// The group ID as base58
    pub group_id: String,
    /// Current text content
    pub text: String,
    /// Number of members in the group
    pub member_count: usize,
}

/// A text editing delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DeltaCommand {
    /// Insert text at position
    Insert {
        /// UTF-8 position
        position: u32,
        /// Text to insert
        text: String,
    },
    /// Delete text at position
    Delete {
        /// UTF-8 position
        position: u32,
        /// Number of characters to delete
        length: u32,
    },
}

impl From<DeltaCommand> for crate::TextDelta {
    fn from(cmd: DeltaCommand) -> Self {
        match cmd {
            DeltaCommand::Insert { position, text } => crate::TextDelta::Insert { position, text },
            DeltaCommand::Delete { position, length } => {
                crate::TextDelta::Delete { position, length }
            }
        }
    }
}

/// Parse a group ID from base58.
///
/// # Errors
/// Returns an error string if the base58 is invalid or too long.
pub fn parse_group_id(b58: &str) -> Result<GroupId, String> {
    let bytes = bs58::decode(b58)
        .into_vec()
        .map_err(|e| format!("invalid base58: {e}"))?;
    if bytes.len() > 32 {
        return Err("group ID too long (max 32 bytes)".to_string());
    }
    Ok(GroupId::from_slice(&bytes))
}

// ============================================================================
// Tauri Commands (only when tauri feature is enabled)
// ============================================================================

#[cfg(feature = "tauri")]
pub mod tauri_commands {
    //! Tauri command implementations.
    //!
    //! These are the actual `#[tauri::command]` functions that get registered
    //! with the Tauri app.

    use std::sync::Arc;

    use iroh::EndpointAddr;
    use mls_rs::client_builder::MlsConfig;
    use mls_rs::CipherSuiteProvider;
    use tauri::State;
    use tokio::sync::RwLock;

    use super::{DeltaCommand, DocumentInfo, parse_group_id};
    use crate::AppState;

    /// Type alias for Tauri state
    pub type TauriState<C, CS> = State<'static, Arc<RwLock<AppState<C, CS>>>>;

    /// Create a new collaborative document.
    #[tauri::command]
    pub async fn create_document<C, CS>(
        state: TauriState<C, CS>,
    ) -> Result<DocumentInfo, String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let mut app = state.write().await;
        let acceptors = app.acceptor_addrs();

        let doc = app
            .client
            .create_document(&acceptors)
            .await
            .map_err(|e| format!("failed to create document: {e:?}"))?;

        let group_id = doc.group().group_id();
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
        let text = doc.text().await;

        app.insert_document(group_id, doc);

        Ok(DocumentInfo {
            group_id: group_id_b58,
            text,
            member_count: 1,
        })
    }

    /// Join an existing document using a welcome message.
    #[tauri::command]
    pub async fn join_document<C, CS>(
        state: TauriState<C, CS>,
        welcome_b58: String,
    ) -> Result<DocumentInfo, String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let welcome_bytes = bs58::decode(&welcome_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;

        let mut app = state.write().await;

        let doc = app
            .client
            .join_document(&welcome_bytes)
            .await
            .map_err(|e| format!("failed to join document: {e:?}"))?;

        let group_id = doc.group().group_id();
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
        let text = doc.text().await;

        app.insert_document(group_id, doc);

        Ok(DocumentInfo {
            group_id: group_id_b58,
            text,
            member_count: 1, // Will be updated when we get context
        })
    }

    /// Get the current text content of a document.
    #[tauri::command]
    pub async fn get_document_text<C, CS>(
        state: TauriState<C, CS>,
        group_id: String,
    ) -> Result<String, String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let gid = parse_group_id(&group_id)?;
        let app = state.read().await;

        let doc = app
            .get_document(&gid)
            .ok_or_else(|| format!("document not found: {group_id}"))?;

        Ok(doc.text().await)
    }

    /// Apply a text delta to a document.
    #[tauri::command]
    pub async fn apply_delta<C, CS>(
        state: TauriState<C, CS>,
        group_id: String,
        delta: DeltaCommand,
    ) -> Result<(), String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
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

    /// Generate a key package for joining a document.
    #[tauri::command]
    pub async fn get_key_package<C, CS>(
        state: TauriState<C, CS>,
    ) -> Result<String, String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
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

    /// Add a member to a document.
    #[tauri::command]
    pub async fn add_member<C, CS>(
        state: TauriState<C, CS>,
        group_id: String,
        key_package_b58: String,
    ) -> Result<(), String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
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
            .add_member(key_package)
            .await
            .map_err(|e| format!("failed to add member: {e:?}"))?;

        Ok(())
    }

    /// Add an acceptor to the app state.
    #[tauri::command]
    pub async fn add_acceptor<C, CS>(
        state: TauriState<C, CS>,
        name: String,
        addr_b58: String,
    ) -> Result<(), String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let addr_bytes = bs58::decode(&addr_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let addr: EndpointAddr =
            postcard::from_bytes(&addr_bytes).map_err(|e| format!("invalid address: {e}"))?;

        let mut app = state.write().await;
        app.add_acceptor(name, addr);

        Ok(())
    }

    /// List all documents.
    #[tauri::command]
    pub async fn list_documents<C, CS>(
        state: TauriState<C, CS>,
    ) -> Result<Vec<String>, String>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let app = state.read().await;
        let ids: Vec<String> = app
            .document_ids()
            .iter()
            .map(|id| bs58::encode(id.as_bytes()).into_string())
            .collect();
        Ok(ids)
    }
}
