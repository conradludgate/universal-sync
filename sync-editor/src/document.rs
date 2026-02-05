//! Synchronized document using yrs CRDT
//!
//! This module provides [`SyncedDocument`], which uses the Group's internal
//! yrs CRDT to provide E2EE collaborative text editing.

use std::sync::Arc;

use error_stack::Report;
use mls_rs::client_builder::MlsConfig;
use mls_rs::CipherSuiteProvider;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_proposer::{Group, GroupError};
use universal_sync_testing::YrsCrdt;
use yrs::{GetString, Text, Transact};

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

/// Event emitted when the document is updated by a remote peer.
#[derive(Debug, Clone)]
pub struct DocumentUpdateEvent {
    /// The sender's member ID
    pub sender: u32,
    /// The epoch when the message was sent
    pub epoch: u64,
    /// The new text content after applying the update
    pub text: String,
}

/// Handle to a running sync loop.
///
/// Dropping this handle will cancel the sync loop.
pub struct SyncHandle {
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl SyncHandle {
    /// Cancel the sync loop and wait for it to finish.
    pub async fn cancel(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for SyncHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
        // Task will be aborted when JoinHandle is dropped
    }
}

/// Name of the text field in the yrs document
const TEXT_NAME: &str = "content";

/// A synchronized text document.
///
/// Uses the Group's internal yrs CRDT to provide E2EE collaborative text editing.
/// The CRDT is automatically kept in sync when messages are sent/received.
pub struct SyncedDocument<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// The underlying sync group (wrapped for sharing with sync loop)
    group: Arc<Mutex<Group<C, CS>>>,
    /// Handle to the sync loop (if running)
    sync_handle: Option<SyncHandle>,
}

impl<C, CS> SyncedDocument<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new synced document from a group.
    ///
    /// The document uses the Group's internal CRDT.
    #[must_use]
    pub fn new(group: Group<C, CS>) -> Self {
        Self {
            group: Arc::new(Mutex::new(group)),
            sync_handle: None,
        }
    }

    /// Get the current text content.
    pub async fn text(&self) -> String {
        let group = self.group.lock().await;
        let crdt = group.crdt();
        let crdt_guard = crdt.lock().unwrap();
        
        if let Some(yrs_crdt) = crdt_guard.as_any().downcast_ref::<YrsCrdt>() {
            let doc = yrs_crdt.doc();
            let text = doc.get_or_insert_text(TEXT_NAME);
            let txn = doc.transact();
            text.get_string(&txn)
        } else {
            String::new()
        }
    }

    /// Insert text at the given position.
    ///
    /// The change is applied locally and broadcast to other group members.
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn insert(&mut self, position: u32, content: &str) -> Result<(), Report<GroupError>> {
        let mut group = self.group.lock().await;
        
        // Create the update by modifying the CRDT
        let update = {
            let crdt = group.crdt();
            let mut crdt_guard = crdt.lock().unwrap();
            
            if let Some(yrs_crdt) = crdt_guard.as_any_mut().downcast_mut::<YrsCrdt>() {
                let doc = yrs_crdt.doc_mut();
                let client_id = doc.client_id();
                let text = doc.get_or_insert_text(TEXT_NAME);
                let mut txn = doc.transact_mut();
                text.insert(&mut txn, position, content);
                let update = txn.encode_update_v1();
                tracing::debug!(
                    update_len = update.len(),
                    position = position,
                    inserted_text = %content,
                    client_id = client_id,
                    "sending insert update to group"
                );
                update
            } else {
                return Ok(()); // No yrs CRDT, nothing to do
            }
        };
        group.send_message(&update).await?;

        Ok(())
    }

    /// Delete characters starting at the given position.
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn delete(&mut self, position: u32, length: u32) -> Result<(), Report<GroupError>> {
        let mut group = self.group.lock().await;
        
        // Create the update by modifying the CRDT
        let update = {
            let crdt = group.crdt();
            let mut crdt_guard = crdt.lock().unwrap();
            
            if let Some(yrs_crdt) = crdt_guard.as_any_mut().downcast_mut::<YrsCrdt>() {
                let doc = yrs_crdt.doc_mut();
                let text = doc.get_or_insert_text(TEXT_NAME);
                let mut txn = doc.transact_mut();
                text.remove_range(&mut txn, position, length);
                txn.encode_update_v1()
            } else {
                return Ok(());
            }
        };

        tracing::debug!(update_len = update.len(), "sending delete update to group");
        group.send_message(&update).await?;

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

    /// Start a background sync loop that receives and applies remote updates.
    ///
    /// Returns a channel receiver for update events. The sync loop runs until
    /// the document is shut down or the returned receiver is dropped.
    ///
    /// Only one sync loop can be active at a time. Calling this again will
    /// cancel the previous loop.
    pub fn start_sync_loop(&mut self) -> mpsc::Receiver<DocumentUpdateEvent> {
        // Cancel any existing sync loop
        if let Some(handle) = self.sync_handle.take() {
            handle.cancel.cancel();
        }

        let (tx, rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let group = Arc::clone(&self.group);

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    () = cancel_clone.cancelled() => {
                        tracing::debug!("sync loop cancelled");
                        break;
                    }

                    // Note: We lock, try to receive, and release quickly
                    // This allows other operations (like add_acceptor) to acquire the lock
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                        let msg = {
                            let mut g = group.lock().await;
                            // Use a short timeout to avoid holding lock forever
                            match tokio::time::timeout(
                                tokio::time::Duration::from_millis(10),
                                g.recv_message()
                            ).await {
                                Ok(msg) => msg,
                                Err(_) => continue, // Timeout, try again
                            }
                        };
                        
                        let Some(msg) = msg else {
                            tracing::debug!("sync loop: group channel closed");
                            break;
                        };
                        
                        tracing::debug!(
                            sender = msg.sender.0,
                            epoch = msg.epoch.0,
                            data_len = msg.data.len(),
                            "sync loop: received message from group"
                        );

                        // The Group's internal CRDT is already updated by recv_message,
                        // so we just need to read the current text.
                        let text = {
                            let g = group.lock().await;
                            let crdt = g.crdt();
                            let crdt_guard = crdt.lock().unwrap();
                            
                            // Try to downcast to YrsCrdt
                            let crdt_type = crdt_guard.type_id();
                            tracing::debug!(crdt_type, "sync loop: checking CRDT type");
                            
                            if let Some(yrs_crdt) = crdt_guard.as_any().downcast_ref::<YrsCrdt>() {
                                let doc = yrs_crdt.doc();
                                let t = doc.get_or_insert_text(TEXT_NAME);
                                let txn = doc.transact();
                                t.get_string(&txn)
                            } else {
                                tracing::warn!("sync loop: failed to downcast CRDT to YrsCrdt");
                                continue;
                            }
                        };

                        tracing::debug!(
                            text_len = text.len(),
                            "sync loop: got updated text"
                        );

                        let event = DocumentUpdateEvent {
                            sender: msg.sender.0,
                            epoch: msg.epoch.0,
                            text,
                        };

                        // Send the event (ignore if receiver is dropped)
                        if tx.send(event).await.is_err() {
                            tracing::debug!("sync loop: event receiver dropped");
                            break;
                        }
                    }
                }
            }
        });

        self.sync_handle = Some(SyncHandle { cancel, task: Some(task) });
        rx
    }

    /// Check if a sync loop is currently running.
    #[must_use]
    pub fn is_syncing(&self) -> bool {
        self.sync_handle.is_some()
    }

    /// Get a clone of the group Arc for direct access.
    #[must_use]
    pub fn group_handle(&self) -> Arc<Mutex<Group<C, CS>>> {
        Arc::clone(&self.group)
    }

    /// Get a mutable reference to the underlying group.
    ///
    /// Note: This locks the group mutex. For concurrent access,
    /// use `group_handle()` instead.
    pub async fn group_mut(&mut self) -> tokio::sync::MutexGuard<'_, Group<C, CS>> {
        self.group.lock().await
    }

    /// Get the group ID.
    pub async fn group_id(&self) -> universal_sync_core::GroupId {
        let group = self.group.lock().await;
        group.group_id()
    }

    /// Shutdown the document and its underlying group.
    ///
    /// # Panics
    ///
    /// Panics if there are outstanding references to the group handle
    /// (e.g., from `group_handle()`). Ensure all clones are dropped before
    /// calling shutdown.
    pub async fn shutdown(mut self) {
        // Cancel sync loop if running
        if let Some(handle) = self.sync_handle.take() {
            handle.cancel().await;
        }

        // Shutdown the group
        let group = Arc::try_unwrap(self.group)
            .unwrap_or_else(|arc| {
                // If there are other references, we can't take ownership
                // Just cancel and let them clean up
                tracing::warn!("document has other references, cannot fully shutdown");
                // Block to get ownership
                futures::executor::block_on(async {
                    let g = arc.lock().await;
                    // We can't call shutdown without ownership, so just drop
                    std::mem::drop(g);
                });
                panic!("cannot shutdown with outstanding references");
            })
            .into_inner();

        group.shutdown().await;
    }
}
