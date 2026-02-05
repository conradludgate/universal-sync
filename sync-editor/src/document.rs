//! Synchronized document using yrs CRDT
//!
//! This module provides [`SyncedDocument`], which wraps a yrs document
//! and synchronizes changes through a universal-sync Group.

use std::sync::Arc;

use error_stack::Report;
use mls_rs::client_builder::MlsConfig;
use mls_rs::CipherSuiteProvider;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
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
    /// The underlying sync group (wrapped for sharing with sync loop)
    group: Arc<Mutex<Group<C, CS>>>,
    /// Name of the text field in the yrs document
    text_name: &'static str,
    /// Handle to the sync loop (if running)
    sync_handle: Option<SyncHandle>,
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
            group: Arc::new(Mutex::new(group)),
            text_name: "content",
            sync_handle: None,
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
        let mut group = self.group.lock().await;
        group.send_message(&update).await?;

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
        let mut group = self.group.lock().await;
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
        let msg = {
            let mut group = self.group.lock().await;
            group.recv_message().await?
        };

        // Apply the update
        if let Err(e) = self.apply_remote_update(&msg.data).await {
            return Some(Err(e));
        }

        Some(Ok(msg))
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

        let doc = Arc::clone(&self.doc);
        let group = Arc::clone(&self.group);
        let text_name = self.text_name;

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    () = cancel_clone.cancelled() => {
                        tracing::debug!("sync loop cancelled");
                        break;
                    }

                    msg = async {
                        let mut g = group.lock().await;
                        g.recv_message().await
                    } => {
                        let Some(msg) = msg else {
                            tracing::debug!("sync loop: group channel closed");
                            break;
                        };

                        // Apply the update and get new text in a single lock scope
                        // (yrs::Update is not Send, so we can't hold it across await)
                        let result: Option<(u32, u64, String)> = {
                            let d = doc.lock().await;

                            let update = match Update::decode_v1(&msg.data) {
                                Ok(u) => u,
                                Err(e) => {
                                    tracing::warn!(?e, "sync loop: failed to decode update");
                                    continue;
                                }
                            };

                            if let Err(e) = d.transact_mut().apply_update(update) {
                                tracing::warn!(?e, "sync loop: failed to apply update");
                                continue;
                            }

                            // Get the new text content
                            let t = d.get_or_insert_text(text_name);
                            let txn = d.transact();
                            let text = t.get_string(&txn);

                            Some((msg.sender.0, msg.epoch.0, text))
                        };

                        let Some((sender, epoch, text)) = result else {
                            continue;
                        };

                        let event = DocumentUpdateEvent {
                            sender,
                            epoch,
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

    /// Get a clone of the document Arc for observation.
    #[must_use]
    pub fn doc_handle(&self) -> Arc<Mutex<Doc>> {
        Arc::clone(&self.doc)
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
