//! Application state management for the Tauri app
//!
//! This module provides [`AppState`], which holds all runtime state
//! for the collaborative editor application.

use std::collections::HashMap;
use std::sync::Arc;

use mls_rs::client_builder::MlsConfig;
use mls_rs::CipherSuiteProvider;
use tokio::sync::RwLock;
use universal_sync_core::GroupId;

use crate::{EditorClient, SyncedDocument};

/// Application state shared across all Tauri commands.
///
/// This is wrapped in `Arc<RwLock<_>>` for thread-safe access from
/// multiple async command handlers.
pub struct AppState<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// The editor client for creating/joining documents
    pub client: EditorClient<C, CS>,
    /// Active documents by group ID
    pub documents: HashMap<GroupId, SyncedDocument<C, CS>>,
}

impl<C, CS> AppState<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new app state with the given editor client.
    #[must_use]
    pub fn new(client: EditorClient<C, CS>) -> Self {
        Self {
            client,
            documents: HashMap::new(),
        }
    }

    /// Get a document by group ID.
    #[must_use]
    pub fn get_document(&self, group_id: &GroupId) -> Option<&SyncedDocument<C, CS>> {
        self.documents.get(group_id)
    }

    /// Get a mutable document by group ID.
    pub fn get_document_mut(&mut self, group_id: &GroupId) -> Option<&mut SyncedDocument<C, CS>> {
        self.documents.get_mut(group_id)
    }

    /// Insert a document.
    pub fn insert_document(&mut self, group_id: GroupId, doc: SyncedDocument<C, CS>) {
        self.documents.insert(group_id, doc);
    }

    /// Remove a document.
    pub fn remove_document(&mut self, group_id: &GroupId) -> Option<SyncedDocument<C, CS>> {
        self.documents.remove(group_id)
    }

    /// List all document group IDs.
    #[must_use]
    pub fn document_ids(&self) -> Vec<GroupId> {
        self.documents.keys().copied().collect()
    }
}

/// Thread-safe wrapper for app state.
pub type SharedAppState<C, CS> = Arc<RwLock<AppState<C, CS>>>;

/// Create a new shared app state.
#[must_use]
pub fn shared_state<C, CS>(client: EditorClient<C, CS>) -> SharedAppState<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    Arc::new(RwLock::new(AppState::new(client)))
}
