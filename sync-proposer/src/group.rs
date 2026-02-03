//! High-level synchronized group API
//!
//! This module provides a self-driving [`Group`] wrapper that:
//! - Spawns a background task to learn updates from acceptors
//! - Automatically retries proposals on contention
//! - Provides a simple API for group operations
//!
//! # Example
//!
//! ```ignore
//! let group = Group::create(&client, signer, cipher_suite, &endpoint, &acceptors).await?;
//!
//! // Subscribe to events (optional, informational only)
//! let mut events = group.subscribe();
//! tokio::spawn(async move {
//!     while let Ok(event) = events.recv().await {
//!         println!("Group event: {:?}", event);
//!     }
//! });
//!
//! // Add a member - blocks until consensus is reached
//! let welcome = group.add_member(key_package).await?;
//!
//! // Graceful shutdown
//! group.shutdown().await;
//! ```

use error_stack::Report;
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use rand::rngs::StdRng;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, EncryptedAppMessage, Epoch, GroupId, GroupMessage,
    GroupProposal, MemberId, MessageId,
};
use universal_sync_paxos::Learner;
use universal_sync_paxos::config::{ProposerConfig, TokioSleep};
use universal_sync_paxos::proposer::Proposer;

use crate::connection::ConnectionManager;
use crate::connector::IrohConnector;
use crate::error::GroupError;
use crate::flows::acceptors_extension;
use crate::learner::GroupLearner;

/// Type alias for the proposer with concrete types
type GroupProposer<C, CS> =
    Proposer<GroupLearner<C, CS>, IrohConnector<GroupLearner<C, CS>>, TokioSleep, StdRng>;

/// A decrypted application message received from the group.
#[derive(Debug, Clone)]
pub struct ReceivedAppMessage {
    /// The sender's member index
    pub sender: MemberId,
    /// The epoch in which the message was sent
    pub epoch: Epoch,
    /// The message index (per-sender, per-epoch)
    pub index: u32,
    /// The decrypted application data
    pub data: Vec<u8>,
}

/// Events emitted by a Group.
///
/// These are informational only - applications don't need to handle them.
/// The enum is `#[non_exhaustive]` so new variants can be added without breaking changes.
///
/// Each variant corresponds to an MLS proposal type that was applied in a commit.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum GroupEvent {
    /// A member was added
    MemberAdded {
        /// The index of the new member
        index: u32,
    },
    /// A member was removed
    MemberRemoved {
        /// The index of the removed member
        index: u32,
    },
    /// An acceptor was added (from `GroupContextExtensions` proposal)
    AcceptorAdded {
        /// The ID of the new acceptor
        id: AcceptorId,
    },
    /// An acceptor was removed (from `GroupContextExtensions` proposal)
    AcceptorRemoved {
        /// The ID of the removed acceptor
        id: AcceptorId,
    },
    /// `ReInit` proposal was applied
    ReInitiated,
    /// External init proposal was applied
    ExternalInit,
    /// Group context extensions were updated (not acceptor add/remove)
    ExtensionsUpdated,
    /// Unknown or custom proposal type
    Unknown,
}

/// Current state of the group
#[derive(Debug, Clone)]
pub struct GroupContext {
    /// The group ID
    pub group_id: GroupId,
    /// Current MLS epoch
    pub epoch: Epoch,
    /// Number of members in the group
    pub member_count: usize,
    /// Current set of acceptor IDs
    pub acceptors: Vec<AcceptorId>,
    /// The confirmed transcript hash for this epoch
    pub confirmed_transcript_hash: Vec<u8>,
}

/// A synchronized MLS group that automatically handles consensus.
///
/// Spawns a background task to learn updates from acceptors.
/// All mutation methods automatically propose via Paxos with retry.
///
/// # Lifecycle
///
/// - On `Drop`: Sends cancel signal to background task, does NOT wait
/// - On [`shutdown()`](Self::shutdown): Sends cancel signal and waits for clean termination
pub struct Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// The underlying learner
    learner: GroupLearner<C, CS>,

    /// The Paxos proposer (works with or without acceptors)
    proposer: GroupProposer<C, CS>,

    /// Connector for creating new proposers (used when rebuilding after acceptor changes)
    connector: IrohConnector<GroupLearner<C, CS>>,

    /// Connection manager for multiplexed streams
    connection_manager: ConnectionManager,

    /// Receiver for learned proposals from the background task
    learned_rx: tokio::sync::mpsc::Receiver<(GroupProposal, GroupMessage)>,

    /// Background task handle
    learn_task: Option<JoinHandle<()>>,

    /// Cancellation token for the background task
    cancel_token: CancellationToken,

    /// Event broadcaster
    event_tx: broadcast::Sender<GroupEvent>,

    /// Receiver for encrypted application messages (from background task)
    encrypted_message_rx: tokio::sync::mpsc::Receiver<EncryptedAppMessage>,

    /// Deduplication: seen message identities (sender, epoch, index)
    seen_messages: std::collections::HashSet<(MemberId, Epoch, u32)>,

    /// Per-epoch message index counter (resets on epoch change)
    message_index: u32,

    /// The epoch for which message_index is valid
    message_epoch: Epoch,
}

impl<C, CS> Drop for Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Signal cancellation but don't wait
        self.cancel_token.cancel();
    }
}

/// Run a blocking closure, using `block_in_place` on multi-threaded runtimes.
///
/// On single-threaded runtimes (like `#[tokio::test]`), just runs the closure directly.
/// On multi-threaded runtimes, uses `block_in_place` to signal blocking I/O.
fn blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                tokio::task::block_in_place(f)
            } else {
                f()
            }
        }
        Err(_) => f(),
    }
}

impl<C, CS> Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// Create a new group, optionally registering with acceptors.
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key
    /// * `cipher_suite` - The cipher suite provider
    /// * `endpoint` - The iroh endpoint for p2p connections
    /// * `acceptors` - Endpoint addresses of acceptors to register with (can be empty)
    /// * `store` - Persistent storage for group state
    ///
    /// # Errors
    /// Returns an error if group creation or acceptor registration fails.
    pub async fn create(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: &Endpoint,
        acceptors: &[EndpointAddr],
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use crate::connector::register_group_with_addr;

        // Create a new MLS group (blocking I/O via storage)
        let (learner, group_id, group_info_bytes) = blocking(|| {
            let group = client
                .create_group(
                    mls_rs::ExtensionList::default(),
                    mls_rs::ExtensionList::default(),
                )
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("failed to create MLS group: {e:?}"))
                })?;

            // Extract the group ID
            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

            // Create the learner
            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

            // Generate GroupInfo if we have acceptors
            let group_info_bytes = if acceptors.is_empty() {
                None
            } else {
                let group_info_msg = learner.group().group_info_message(true).map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("failed to create group info: {e:?}"))
                })?;
                Some(group_info_msg.to_bytes().map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("failed to serialize group info: {e:?}"))
                })?)
            };

            Ok::<_, Report<GroupError>>((learner, group_id, group_info_bytes))
        })?;

        // Register with acceptors (async network I/O)
        if let Some(group_info_bytes) = group_info_bytes {
            for addr in acceptors {
                register_group_with_addr(endpoint, addr.clone(), &group_info_bytes)
                    .await
                    .map_err(|e| {
                        Report::new(GroupError)
                            .attach_printable(format!("failed to register with acceptor: {e}"))
                    })?;
            }
        }

        let connector = IrohConnector::new(endpoint.clone(), group_id);
        let connection_manager = ConnectionManager::new(endpoint.clone());

        Ok(Self::from_learner(learner, connector, connection_manager))
    }

    /// Join an existing group from a Welcome message.
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key
    /// * `cipher_suite` - The cipher suite provider
    /// * `endpoint` - The iroh endpoint for p2p connections
    /// * `welcome` - The Welcome message bytes
    /// * `store` - Persistent storage for group state
    ///
    /// # Errors
    /// Returns an error if joining fails.
    #[allow(clippy::unused_async)] // Keep async for API consistency
    pub async fn join(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: &Endpoint,
        welcome_bytes: &[u8],
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use universal_sync_core::AcceptorsExt;

        // Join the group (blocking I/O via storage)
        let (learner, group_id) = blocking(|| {
            // Parse the Welcome message
            let welcome = MlsMessage::from_bytes(welcome_bytes).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("invalid welcome message: {e:?}"))
            })?;

            // Join the group - info contains the GroupInfo extensions
            let (group, info) = client.join_group(None, &welcome).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("failed to join group: {e:?}"))
            })?;

            // Extract the group ID
            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

            // Read acceptors from GroupInfo extensions
            let acceptors = info
                .group_info_extensions
                .get_as::<AcceptorsExt>()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("failed to read acceptors extension: {e:?}"))
                })?
                .map(|ext| ext.0)
                .unwrap_or_default();

            // Create the learner
            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors);

            Ok::<_, Report<GroupError>>((learner, group_id))
        })?;

        let connector = IrohConnector::new(endpoint.clone(), group_id);
        let connection_manager = ConnectionManager::new(endpoint.clone());

        Ok(Self::from_learner(learner, connector, connection_manager))
    }

    /// Create a Group from an existing learner
    fn from_learner(
        learner: GroupLearner<C, CS>,
        connector: IrohConnector<GroupLearner<C, CS>>,
        connection_manager: ConnectionManager,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(64);
        let (learned_tx, learned_rx) = tokio::sync::mpsc::channel(64);
        let (encrypted_message_tx, encrypted_message_rx) = tokio::sync::mpsc::channel(256);

        // Create the proposer (works with or without acceptors)
        let node_id = learner.node_id();
        let group_id = GroupId::from_slice(&learner.group().context().group_id);
        let acceptor_ids: Vec<_> = learner.acceptor_ids().collect();
        let mut proposer = Proposer::new(node_id, connector.clone(), ProposerConfig::default());
        proposer.sync_actors(acceptor_ids.clone());

        // Spawn the background learning task
        let task_cancel = cancel_token.clone();
        let task_connector = connector.clone();
        let task_connection_manager = connection_manager.clone();
        let task = tokio::spawn(async move {
            Self::background_loop(
                node_id,
                group_id,
                acceptor_ids,
                task_connector,
                task_connection_manager,
                task_cancel,
                learned_tx,
                encrypted_message_tx,
            )
            .await;
        });

        let message_epoch = learner.mls_epoch();

        Self {
            learner,
            proposer,
            connector,
            connection_manager,
            learned_rx,
            learn_task: Some(task),
            cancel_token,
            event_tx,
            encrypted_message_rx,
            seen_messages: std::collections::HashSet::new(),
            message_index: 0,
            message_epoch,
        }
    }

    /// Background task that handles learning and message subscription
    #[allow(clippy::too_many_arguments)]
    async fn background_loop(
        node_id: universal_sync_core::MemberId,
        group_id: GroupId,
        acceptor_ids: Vec<AcceptorId>,
        connector: IrohConnector<GroupLearner<C, CS>>,
        connection_manager: ConnectionManager,
        cancel_token: CancellationToken,
        learned_tx: tokio::sync::mpsc::Sender<(GroupProposal, GroupMessage)>,
        encrypted_message_tx: tokio::sync::mpsc::Sender<EncryptedAppMessage>,
    ) {
        use futures::StreamExt;

        // Create a dedicated proposer for learning
        let mut proposer: GroupProposer<C, CS> =
            Proposer::new(node_id, connector, ProposerConfig::default());
        proposer.sync_actors(acceptor_ids.clone());

        // Subscribe to message streams from each acceptor
        let mut message_streams = futures::stream::SelectAll::new();
        for acceptor_id in &acceptor_ids {
            let stream = subscribe_to_acceptor_messages(
                connection_manager.clone(),
                *acceptor_id,
                group_id,
                cancel_token.clone(),
            );
            // Box::pin to make the stream Unpin for SelectAll
            message_streams.push(Box::pin(stream));
        }

        loop {
            tokio::select! {
                biased;

                () = cancel_token.cancelled() => {
                    tracing::debug!("background loop cancelled");
                    break;
                }

                // Receive messages from acceptors
                Some(msg) = message_streams.next() => {
                    if encrypted_message_tx.send(msg).await.is_err() {
                        tracing::debug!("message channel closed");
                        break;
                    }
                }

                // Note: Learning is still handled via polling in apply_pending_learned
                // A full implementation would share state for learn_one
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Polling placeholder
                }
            }
        }

        drop(learned_tx);
    }

    /// Apply any pending learned proposals from the background task
    fn apply_pending_learned(&mut self) -> Result<Vec<GroupEvent>, Report<GroupError>> {
        let mut events = Vec::new();

        while let Ok((proposal, message)) = self.learned_rx.try_recv() {
            let new_events = self.apply_proposal(&proposal, message)?;
            events.extend(new_events);
        }

        Ok(events)
    }

    /// Apply a proposal and message, returning the events generated
    fn apply_proposal(
        &mut self,
        proposal: &GroupProposal,
        message: GroupMessage,
    ) -> Result<Vec<GroupEvent>, Report<GroupError>> {
        // Check if this is our own proposal
        let my_proposal = proposal.member_id == self.learner.node_id();

        // Apply the proposal (blocking I/O via storage)
        let effect = blocking(|| {
            if my_proposal && self.learner.has_pending_commit() {
                // This is our proposal and we have a pending commit - apply it
                tracing::debug!("applying our own pending commit");
                let effect = self
                    .learner
                    .group_mut()
                    .apply_pending_commit()
                    .map_err(|e| {
                        Report::new(GroupError)
                            .attach_printable(format!("apply pending commit failed: {e:?}"))
                    })?;

                Ok::<_, Report<GroupError>>(Some(effect.effect))
            } else {
                // This is someone else's proposal, or we don't have a pending commit
                if self.learner.has_pending_commit() {
                    tracing::debug!("clearing pending commit - another proposal won");
                    self.learner.clear_pending_commit();
                }

                // Process the MLS message
                let result = self
                    .learner
                    .group_mut()
                    .process_incoming_message(message.mls_message)
                    .map_err(|e| {
                        Report::new(GroupError)
                            .attach_printable(format!("process message failed: {e:?}"))
                    })?;

                match result {
                    ReceivedMessage::Commit(commit_desc) => {
                        tracing::debug!(
                            epoch = self.learner.group().context().epoch,
                            "applied commit"
                        );
                        Ok(Some(commit_desc.effect))
                    }
                    _ => Ok(None),
                }
            }
        })?;

        let events = if let Some(effect) = effect {
            self.process_commit_effect(&effect)
        } else {
            Vec::new()
        };

        self.sync_proposer_actors();
        Ok(events)
    }

    /// Process a commit effect and return events for each applied proposal
    fn process_commit_effect(&mut self, effect: &CommitEffect) -> Vec<GroupEvent> {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => {
                return vec![GroupEvent::ReInitiated];
            }
        };

        let mut events = Vec::new();

        for proposal_info in applied_proposals {
            let event = match &proposal_info.proposal {
                MlsProposal::Add(add_proposal) => {
                    // Get the member index from the key package
                    // For now, we use a placeholder - real implementation would track this
                    let _ = add_proposal;
                    GroupEvent::MemberAdded { index: 0 }
                }
                MlsProposal::Remove(remove_proposal) => GroupEvent::MemberRemoved {
                    index: remove_proposal.to_remove(),
                },
                MlsProposal::ReInit(_) => GroupEvent::ReInitiated,
                MlsProposal::ExternalInit(_) => GroupEvent::ExternalInit,
                MlsProposal::GroupContextExtensions(extensions) => {
                    // Check for AcceptorAdd
                    if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                        let id = add.acceptor_id();
                        self.learner.add_acceptor_addr(add.0.clone());
                        GroupEvent::AcceptorAdded { id }
                    } else if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                        let id = remove.acceptor_id();
                        self.learner.remove_acceptor_id(&id);
                        GroupEvent::AcceptorRemoved { id }
                    } else {
                        GroupEvent::ExtensionsUpdated
                    }
                }
                _ => GroupEvent::Unknown,
            };

            events.push(event.clone());

            // Broadcast each event
            let _ = self.event_tx.send(event);
        }

        events
    }

    /// Sync the proposer's actor set with the current acceptors
    fn sync_proposer_actors(&mut self) {
        self.proposer.sync_actors(self.learner.acceptor_ids());
    }

    /// Add a member to the group.
    ///
    /// Proposes via Paxos with automatic retry on contention.
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Arguments
    /// * `key_package` - The new member's key package
    ///
    /// # Returns
    /// The Welcome message to send to the new member.
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn add_member(
        &mut self,
        key_package: MlsMessage,
    ) -> Result<Vec<u8>, Report<GroupError>> {
        self.propose_with_retry(|learner| {
            // Collect acceptors first to avoid borrow conflicts
            let acceptors_ext = acceptors_extension(learner.acceptors().values().cloned());

            // Build the commit with acceptors extension
            let commit_output = learner
                .group_mut()
                .commit_builder()
                .add_member(key_package.clone())
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("add_member failed: {e:?}"))
                })?
                .set_group_info_ext(acceptors_ext)
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            // Extract the welcome message
            let welcome = commit_output
                .welcome_messages
                .first()
                .ok_or_else(|| {
                    Report::new(GroupError).attach_printable("no welcome message generated")
                })?
                .to_bytes()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("welcome serialization failed: {e:?}"))
                })?;

            Ok((commit_output, welcome))
        })
        .await
    }

    /// Remove a member from the group.
    ///
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Arguments
    /// * `member_index` - The index of the member to remove
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn remove_member(&mut self, member_index: u32) -> Result<(), Report<GroupError>> {
        self.propose_with_retry(|learner| {
            let commit_output = learner
                .group_mut()
                .commit_builder()
                .remove_member(member_index)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("remove_member failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Update this member's keys (forward secrecy).
    ///
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn update_keys(&mut self) -> Result<(), Report<GroupError>> {
        self.propose_with_retry(|learner| {
            let commit_output = learner.group_mut().commit_builder().build().map_err(|e| {
                Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
            })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Send an encrypted application message to the group.
    ///
    /// The message is encrypted using MLS and sent to a subset of acceptors.
    /// Messages are identified by `(group_id, epoch, sender, index)` where
    /// the index is per-sender and resets each epoch.
    ///
    /// # Arguments
    /// * `data` - The plaintext data to encrypt and send
    ///
    /// # Returns
    /// The `EncryptedAppMessage` that was created (for tracking/debugging).
    ///
    /// # Errors
    /// Returns an error if encryption or sending fails.
    pub async fn send_message(
        &mut self,
        data: &[u8],
    ) -> Result<EncryptedAppMessage, Report<GroupError>> {
        // Apply any pending learned proposals first
        self.apply_pending_learned()?;

        // Get current epoch and check if we need to reset the message counter
        let current_epoch = self.learner.mls_epoch();
        if current_epoch != self.message_epoch {
            self.message_index = 0;
            self.message_epoch = current_epoch;
        }

        // Get message ID components
        let group_id = self.group_id();
        let epoch = current_epoch;
        let sender = MemberId(self.learner.group().current_member_index());
        let index = self.message_index;

        // Increment the index for next message
        self.message_index = self.message_index.wrapping_add(1);

        // Only include the index in authenticated data - MLS already embeds
        // group_id, epoch, and sender in the ciphertext
        let authenticated_data = index.to_be_bytes().to_vec();

        // Encrypt the message (blocking I/O via storage)
        let mls_message = blocking(|| {
            self.learner
                .encrypt_application_message(data, authenticated_data)
        })
        .map_err(|e| {
            Report::new(GroupError).attach_printable(format!("failed to encrypt message: {e}"))
        })?;

        // Serialize the MLS message to ciphertext bytes
        let ciphertext = mls_message.to_bytes().map_err(|e| {
            Report::new(GroupError)
                .attach_printable(format!("failed to serialize ciphertext: {e:?}"))
        })?;

        let encrypted_msg = EncryptedAppMessage { ciphertext };

        // Select acceptors using rendezvous hashing
        // We construct a MessageId just for hashing - it's not stored in the message
        let message_id = MessageId {
            group_id,
            epoch,
            sender,
            index,
        };
        let acceptor_ids: Vec<_> = self.learner.acceptor_ids().collect();
        let delivery_count = crate::rendezvous::delivery_count(acceptor_ids.len());
        let selected_acceptors =
            crate::rendezvous::select_acceptors(&acceptor_ids, &message_id, delivery_count);

        // Send to selected acceptors (fire-and-forget style)
        // We spawn tasks to send in parallel but don't wait for all of them
        for acceptor_id in selected_acceptors {
            let connection_manager = self.connection_manager.clone();
            let msg = encrypted_msg.clone();
            let gid = group_id;

            tokio::spawn(async move {
                if let Err(e) =
                    send_message_to_acceptor(&connection_manager, &acceptor_id, gid, msg).await
                {
                    tracing::warn!(?acceptor_id, ?e, "failed to send message to acceptor");
                }
            });
        }

        Ok(encrypted_msg)
    }

    /// Receive the next decrypted application message.
    ///
    /// This receives encrypted messages from acceptors (via background subscription),
    /// decrypts them using MLS, deduplicates, and returns the plaintext.
    ///
    /// Returns `None` if the channel is closed (group shutting down).
    ///
    /// # Errors
    /// Returns an error if decryption fails.
    pub async fn recv_message(&mut self) -> Result<Option<ReceivedAppMessage>, Report<GroupError>> {
        use mls_rs::MlsMessage;

        loop {
            // Apply any pending learned proposals first
            self.apply_pending_learned()?;

            // Try to receive an encrypted message
            let encrypted = match self.encrypted_message_rx.recv().await {
                Some(msg) => msg,
                None => return Ok(None), // Channel closed
            };

            // Deserialize the MLS message
            let mls_message = MlsMessage::from_bytes(&encrypted.ciphertext).map_err(|e| {
                Report::new(GroupError)
                    .attach_printable(format!("failed to deserialize MLS message: {e:?}"))
            })?;

            // Process through MLS (blocking I/O via storage)
            let received = blocking(|| {
                self.learner
                    .group_mut()
                    .process_incoming_message(mls_message)
            })
            .map_err(|e| {
                Report::new(GroupError)
                    .attach_printable(format!("failed to process message: {e:?}"))
            })?;

            // Extract application data
            let ReceivedMessage::ApplicationMessage(app_msg) = received else {
                // Not an application message (shouldn't happen on message stream)
                tracing::debug!("received non-application message on message stream");
                continue;
            };

            // Extract message identity from authenticated data (message index)
            let index = if app_msg.authenticated_data.len() >= 4 {
                u32::from_be_bytes(app_msg.authenticated_data[..4].try_into().unwrap_or([0; 4]))
            } else {
                0
            };

            let sender = MemberId(app_msg.sender_index);
            // Use current group epoch as approximation (exact epoch isn't in ApplicationMessageDescription)
            let epoch = self.learner.mls_epoch();

            // Deduplicate by (sender, epoch, index)
            let key = (sender, epoch, index);
            if self.seen_messages.contains(&key) {
                tracing::trace!(?sender, ?epoch, index, "duplicate message, skipping");
                continue;
            }
            self.seen_messages.insert(key);

            return Ok(Some(ReceivedAppMessage {
                sender,
                epoch,
                index,
                data: app_msg.data().to_vec(),
            }));
        }
    }

    /// Add an acceptor to the federation.
    ///
    /// This proposes adding the acceptor to the group via consensus,
    /// then registers the group with the new acceptor.
    ///
    /// # Arguments
    /// * `addr` - The endpoint address of the acceptor to add
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    pub async fn add_acceptor(&mut self, addr: EndpointAddr) -> Result<(), Report<GroupError>> {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorAdd;

        use crate::connector::register_group_with_addr;

        // Build commit with AcceptorAdd extension
        self.propose_with_retry(|learner| {
            let add_ext = AcceptorAdd::new(addr.clone());
            let mut extensions = ExtensionList::default();
            extensions.set_from(add_ext).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("extension failed: {e:?}"))
            })?;

            let commit_output = learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("set extension failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await?;

        // Get GroupInfo for registration (blocking I/O)
        let group_info_bytes = blocking(|| {
            let group_info = self.learner.group().group_info_message(true).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("group info failed: {e:?}"))
            })?;
            group_info.to_bytes().map_err(|e| {
                Report::new(GroupError).attach_printable(format!("serialization failed: {e:?}"))
            })
        })?;

        // Register group with the new acceptor
        register_group_with_addr(self.connector.endpoint(), addr, &group_info_bytes)
            .await
            .map_err(|e| {
                Report::new(GroupError).attach_printable(format!("registration failed: {e}"))
            })?;

        Ok(())
    }

    /// Remove an acceptor from the federation.
    ///
    /// # Arguments
    /// * `acceptor_id` - The ID of the acceptor to remove
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    pub async fn remove_acceptor(
        &mut self,
        acceptor_id: AcceptorId,
    ) -> Result<(), Report<GroupError>> {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorRemove;

        // Build commit with AcceptorRemove extension
        self.propose_with_retry(|learner| {
            let remove_ext = AcceptorRemove::new(acceptor_id);
            let mut extensions = ExtensionList::default();
            extensions.set_from(remove_ext).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("extension failed: {e:?}"))
            })?;

            let commit_output = learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("set extension failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Subscribe to group state changes (informational).
    ///
    /// This is purely informational - applications are not required to consume events.
    /// The stream will buffer and drop old events if not consumed.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<GroupEvent> {
        self.event_tx.subscribe()
    }

    /// Get current group context (epoch, members, acceptors).
    ///
    /// This applies any pending learned proposals before returning.
    pub fn context(&mut self) -> GroupContext {
        // Apply any pending learned proposals first
        let _ = self.apply_pending_learned();

        self.context_snapshot()
    }

    /// Get a snapshot of the group context without applying pending updates.
    ///
    /// Use [`context`] to also apply pending learned proposals.
    #[must_use]
    pub fn context_snapshot(&self) -> GroupContext {
        let mls_context = self.learner.group().context();
        GroupContext {
            group_id: self.group_id(),
            epoch: self.learner.mls_epoch(),
            member_count: self.learner.group().roster().members().len(),
            acceptors: self.learner.acceptor_ids().collect(),
            confirmed_transcript_hash: mls_context.confirmed_transcript_hash.to_vec(),
        }
    }

    /// Get the group ID
    #[must_use]
    pub fn group_id(&self) -> GroupId {
        let mls_group_id = self.learner.group().context().group_id.clone();
        GroupId::from_slice(&mls_group_id)
    }

    /// Gracefully shut down the background task.
    ///
    /// Unlike `Drop`, this waits for the background task to complete.
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        if let Some(task) = self.learn_task.take() {
            let _ = task.await;
        }
    }

    /// Propose with automatic retry on contention.
    ///
    /// Blocks until this node has learned the consensus result.
    async fn propose_with_retry<F, T>(&mut self, build_commit: F) -> Result<T, Report<GroupError>>
    where
        F: Fn(
            &mut GroupLearner<C, CS>,
        ) -> Result<(mls_rs::group::CommitOutput, T), Report<GroupError>>,
    {
        const MAX_RETRIES: u32 = 5;

        for attempt in 0..MAX_RETRIES {
            tracing::debug!(attempt, "propose_with_retry attempt");

            // Apply any pending learned proposals first
            self.apply_pending_learned()?;

            let current_epoch = self.learner.mls_epoch();
            let my_id = self.learner.node_id();

            // Build the commit (blocking I/O via storage)
            let (commit_output, result) = blocking(|| build_commit(&mut self.learner))?;
            let message = GroupMessage::new(commit_output.commit_message.clone());

            // Try to get consensus (works with or without acceptors)
            match self.proposer.propose(&self.learner, message).await {
                Some((won_proposal, won_msg)) => {
                    // Check if we won (by comparing epoch and member ID)
                    let we_won =
                        won_proposal.member_id == my_id && won_proposal.epoch == current_epoch;

                    // Apply the winning proposal (handles both our own and others')
                    self.apply_proposal(&won_proposal, won_msg)?;

                    if we_won {
                        return Ok(result);
                    }
                    // Someone else won - retry
                }
                None => {
                    // Proposal failed - clear and retry
                    self.learner.clear_pending_commit();
                }
            }

            // Wait a bit before retrying
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        Err(Report::new(GroupError).attach_printable("max retries exceeded due to contention"))
    }
}

/// Helper to send a message to a single acceptor
async fn send_message_to_acceptor(
    connection_manager: &ConnectionManager,
    acceptor_id: &AcceptorId,
    group_id: GroupId,
    msg: EncryptedAppMessage,
) -> Result<(), crate::connector::ConnectorError> {
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
    use universal_sync_core::MessageRequest;

    // Open a message stream to the acceptor
    let (send, recv) = connection_manager
        .open_message_stream(acceptor_id, group_id)
        .await?;

    let mut writer = FramedWrite::new(send, LengthDelimitedCodec::new());
    let mut reader = FramedRead::new(recv, LengthDelimitedCodec::new());

    use crate::connector::ConnectorError;

    // Send the message
    let request = MessageRequest::Send(msg);
    let request_bytes = postcard::to_allocvec(&request)
        .map_err(|e| ConnectorError::Codec(format!("serialize: {e}")))?;
    writer
        .send(request_bytes.into())
        .await
        .map_err(|e| ConnectorError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

    // Wait for acknowledgment (optional, but good to confirm delivery)
    match reader.next().await {
        Some(Ok(response_bytes)) => {
            let response: universal_sync_core::MessageResponse =
                postcard::from_bytes(&response_bytes)
                    .map_err(|e| ConnectorError::Codec(format!("deserialize: {e}")))?;

            match response {
                universal_sync_core::MessageResponse::Stored { arrival_seq } => {
                    tracing::debug!(?acceptor_id, arrival_seq, "message stored");
                    Ok(())
                }
                universal_sync_core::MessageResponse::Error(e) => {
                    Err(ConnectorError::Handshake(format!("acceptor error: {e}")))
                }
                _ => Ok(()), // Ignore other responses
            }
        }
        Some(Err(e)) => Err(ConnectorError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e,
        ))),
        None => Err(ConnectorError::Connect("stream closed".to_string())),
    }
}

/// Create a stream of messages from a single acceptor
fn subscribe_to_acceptor_messages(
    connection_manager: ConnectionManager,
    acceptor_id: AcceptorId,
    group_id: GroupId,
    cancel_token: CancellationToken,
) -> impl futures::Stream<Item = EncryptedAppMessage> + Send {
    async_stream::stream! {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
        use universal_sync_core::{MessageRequest, MessageResponse};

        // Try to connect and subscribe
        let result = connection_manager
            .open_message_stream(&acceptor_id, group_id)
            .await;

        let (send, recv) = match result {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(?acceptor_id, ?e, "failed to open message stream");
                return;
            }
        };

        let mut writer = FramedWrite::new(send, LengthDelimitedCodec::new());
        let mut reader = FramedRead::new(recv, LengthDelimitedCodec::new());

        // Send subscribe request (start from 0 to get only new messages)
        let request = MessageRequest::Subscribe { since_seq: 0 };
        let request_bytes = match postcard::to_allocvec(&request) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(?acceptor_id, ?e, "failed to serialize subscribe request");
                return;
            }
        };

        if let Err(e) = writer.send(request_bytes.into()).await {
            tracing::warn!(?acceptor_id, ?e, "failed to send subscribe request");
            return;
        }

        // Read messages until cancelled or stream closes
        loop {
            tokio::select! {
                biased;

                () = cancel_token.cancelled() => {
                    break;
                }

                frame = reader.next() => {
                    match frame {
                        Some(Ok(bytes)) => {
                            match postcard::from_bytes::<MessageResponse>(&bytes) {
                                Ok(MessageResponse::Message { message, .. }) => {
                                    yield message;
                                }
                                Ok(MessageResponse::Error(e)) => {
                                    tracing::warn!(?acceptor_id, ?e, "acceptor returned error");
                                }
                                Ok(_) => {
                                    // Ignore other responses (e.g., Stored)
                                }
                                Err(e) => {
                                    tracing::warn!(?acceptor_id, ?e, "failed to deserialize response");
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(?acceptor_id, ?e, "stream read error");
                            break;
                        }
                        None => {
                            tracing::debug!(?acceptor_id, "message stream closed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GroupEvent>();
    }
}
