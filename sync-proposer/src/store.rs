//! Persistent storage for proposer state
//!
//! Stores acceptor address mappings using fjall.
//! MLS group state is not persisted (must be re-created each session).

use std::path::Path;
use std::sync::Arc;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use serde::{Deserialize, Serialize};
use universal_sync_core::{AcceptorId, GroupId};

/// Stored acceptor information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAcceptor {
    pub id: AcceptorId,
    /// Stored as iroh `EndpointAddr.to_string()`
    pub address: String,
}

/// Stored group metadata (acceptor mappings only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredGroupInfo {
    /// List of acceptors for this group
    pub acceptors: Vec<StoredAcceptor>,
}

/// Persistent store for proposer state
pub struct ProposerStore {
    db: Database,
    /// Keyspace for group info: `group_id` -> `StoredGroupInfo`
    groups: Keyspace,
}

impl ProposerStore {
    /// Open or create a store at the given path
    ///
    /// # Errors
    /// Returns an error if the database cannot be opened.
    ///
    /// # Panics
    /// Panics if `spawn_blocking` fails.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

    fn open_sync(path: &Path) -> Result<Self, fjall::Error> {
        let db = Database::builder(path).open()?;
        let groups = db.keyspace("groups", KeyspaceCreateOptions::default)?;

        Ok(Self { db, groups })
    }

    /// List all stored group IDs
    #[must_use]
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.groups
            .iter()
            .filter_map(|guard| {
                let (key, _) = guard.into_inner().ok()?;
                if key.len() == 32 {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&key);
                    Some(GroupId::new(bytes))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get stored group info
    #[must_use]
    pub fn get_group(&self, group_id: &GroupId) -> Option<StoredGroupInfo> {
        self.groups
            .get(group_id.as_bytes())
            .ok()
            .flatten()
            .and_then(|bytes| postcard::from_bytes(&bytes).ok())
    }

    /// Store group info
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    ///
    /// # Panics
    /// Panics if serialization fails.
    pub fn store_group(
        &self,
        group_id: &GroupId,
        info: &StoredGroupInfo,
    ) -> Result<(), fjall::Error> {
        let bytes = postcard::to_allocvec(info).expect("serialization should not fail");
        self.groups.insert(group_id.as_bytes(), &bytes)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Remove a group
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    pub fn remove_group(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.groups.remove(group_id.as_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
}

/// Wrapper to make `ProposerStore` shareable
#[derive(Clone)]
pub struct SharedProposerStore {
    inner: Arc<ProposerStore>,
}

impl SharedProposerStore {
    /// Open or create a store at the given path
    ///
    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let store = ProposerStore::open(path).await?;
        Ok(Self {
            inner: Arc::new(store),
        })
    }

    /// List all stored group IDs
    #[must_use]
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.inner.list_groups()
    }

    /// Get stored group info
    #[must_use]
    pub fn get_group(&self, group_id: &GroupId) -> Option<StoredGroupInfo> {
        self.inner.get_group(group_id)
    }

    /// Store group info
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    pub fn store_group(
        &self,
        group_id: &GroupId,
        info: &StoredGroupInfo,
    ) -> Result<(), fjall::Error> {
        self.inner.store_group(group_id, info)
    }

    /// Remove a group
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    pub fn remove_group(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.inner.remove_group(group_id)
    }
}
