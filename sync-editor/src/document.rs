//! Synchronized document using yrs CRDT
//!
//! This module provides [`SyncedDocument`], which wraps a yrs document
//! and synchronizes changes through a universal-sync Group.

use std::sync::Arc;

use error_stack::Report;
use mls_rs::client_builder::MlsConfig;
use mls_rs::CipherSuiteProvider;
use tokio::sync::Mutex;
use universal_sync_proposer::{Group, GroupError, ReceivedAppMessage};
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Text, Transact, Update};

/// A text editing operation (delta).
#[derive(Debug, Clone)]
pub enum TextDelta {
    /// Insert text at the given position
    Insert {
        /// UTF-8 position to insert at
        position: u32,
        /// Text to insert
        text: String,
    },
    /// Delete characters starting at the given position
    Delete {
        /// UTF-8 position to delete from
        position: u32,
        /// Number of characters to delete
        length: u32,
    },
}

/// Error type for document operations
#[derive(Debug)]
pub struct DocumentError;

impl std::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "document error")
    }
}

impl std::error::Error for DocumentError {}

/// A synchronized text document.
///
/// Wraps a yrs document and a universal-sync Group to provide
/// E2EE collaborative text editing.
pub struct SyncedDocument<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// The yrs document
    doc: Arc<Mutex<Doc>>,
    /// The underlying sync group
    group: Group<C, CS>,
    /// Name of the text field in the yrs document
    text_name: &'static str,
}

impl<C, CS> SyncedDocument<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new synced document from a group and yrs document.
    ///
    /// The document uses a text field named "content" by default.
    #[must_use]
    pub fn new(group: Group<C, CS>, doc: Doc) -> Self {
        Self {
            doc: Arc::new(Mutex::new(doc)),
            group,
            text_name: "content",
        }
    }

    /// Get the current text content.
    pub async fn text(&self) -> String {
        let doc = self.doc.lock().await;
        let text = doc.get_or_insert_text(self.text_name);
        let txn = doc.transact();
        text.get_string(&txn)
    }

    /// Insert text at the given position.
    ///
    /// The change is applied locally and broadcast to other group members.
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn insert(&mut self, position: u32, content: &str) -> Result<(), Report<GroupError>> {
        let update = {
            let doc = self.doc.lock().await;
            let text = doc.get_or_insert_text(self.text_name);
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, position, content);
            txn.encode_update_v1()
        };

        // Send the update to the group
        self.group.send_message(&update).await?;

        Ok(())
    }

    /// Delete characters starting at the given position.
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn delete(&mut self, position: u32, length: u32) -> Result<(), Report<GroupError>> {
        let update = {
            let doc = self.doc.lock().await;
            let text = doc.get_or_insert_text(self.text_name);
            let mut txn = doc.transact_mut();
            text.remove_range(&mut txn, position, length);
            txn.encode_update_v1()
        };

        // Send the update to the group
        self.group.send_message(&update).await?;

        Ok(())
    }

    /// Apply a text delta (insert or delete).
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn apply_delta(&mut self, delta: TextDelta) -> Result<(), Report<GroupError>> {
        match delta {
            TextDelta::Insert { position, text } => self.insert(position, &text).await,
            TextDelta::Delete { position, length } => self.delete(position, length).await,
        }
    }

    /// Apply a remote update received from another group member.
    ///
    /// # Errors
    /// Returns an error if the update is malformed.
    pub async fn apply_remote_update(&self, update_bytes: &[u8]) -> Result<(), Report<DocumentError>> {
        let update = Update::decode_v1(update_bytes)
            .map_err(|e| Report::new(DocumentError).attach(format!("decode error: {e}")))?;

        let doc = self.doc.lock().await;
        doc.transact_mut()
            .apply_update(update)
            .map_err(|e| Report::new(DocumentError).attach(format!("apply error: {e}")))?;

        Ok(())
    }

    /// Receive the next message from the group and apply it.
    ///
    /// Returns `None` if the channel is closed.
    ///
    /// # Errors
    /// Returns an error if the update is malformed.
    pub async fn recv_and_apply(&mut self) -> Option<Result<ReceivedAppMessage, Report<DocumentError>>> {
        let msg = self.group.recv_message().await?;
        
        // Apply the update
        if let Err(e) = self.apply_remote_update(&msg.data).await {
            return Some(Err(e));
        }

        Some(Ok(msg))
    }

    /// Get a mutable reference to the underlying group.
    pub fn group_mut(&mut self) -> &mut Group<C, CS> {
        &mut self.group
    }

    /// Get a reference to the underlying group.
    #[must_use]
    pub fn group(&self) -> &Group<C, CS> {
        &self.group
    }

    /// Get a clone of the document Arc for observation.
    #[must_use]
    pub fn doc_handle(&self) -> Arc<Mutex<Doc>> {
        Arc::clone(&self.doc)
    }

    /// Shutdown the document and its underlying group.
    pub async fn shutdown(self) {
        self.group.shutdown().await;
    }
}
