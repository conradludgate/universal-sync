//! High-level synchronized group API
//!
//! This module provides a self-driving [`Group`] wrapper that:
//! - Spawns a background actor to manage MLS group state
//! - Spawns per-acceptor actors for network I/O
//! - Provides a simple API for group operations via message passing
//!
//! # Architecture
//!
//! ```text
//! Group (handle)
//!   │
//!   ├─► GroupActor (owns GroupLearner, Proposer, QuorumTracker)
//!   │     │
//!   │     ├─► AcceptorActor[0] ──► iroh connection to acceptor 0
//!   │     ├─► AcceptorActor[1] ──► iroh connection to acceptor 1
//!   │     └─► AcceptorActor[n] ──► iroh connection to acceptor n
//!   │
//!   └─► app_message_rx (receives decrypted messages)
//! ```
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

use std::collections::{HashMap, HashSet};

use error_stack::{Report, ResultExt};
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, Crdt, CrdtFactory, CrdtRegistrationExt,
    CrdtSnapshotExt, EncryptedAppMessage, Epoch, GroupId, GroupMessage, GroupProposal, Handshake,
    MemberId, MessageId, OperationContext, PAXOS_ALPN,
};
use universal_sync_paxos::proposer::{ProposeResult, Proposer, QuorumTracker};
use universal_sync_paxos::{AcceptorMessage, Learner, Proposal};

use crate::connection::ConnectionManager;
use crate::connector::{ProposalRequest, ProposalResponse};
use crate::error::GroupError;
use crate::flows::welcome_group_info_extensions;
use crate::learner::GroupLearner;

// =============================================================================
// Actor Message Types
// =============================================================================

/// Request sent to the `GroupActor`
enum GroupRequest<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// Get current group context
    GetContext {
        reply: oneshot::Sender<GroupContext>,
    },
    /// Add a member to the group
    AddMember {
        key_package: Box<MlsMessage>,
        member_addr: EndpointAddr,
        crdt_snapshot: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Remove a member from the group
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Update this member's keys
    UpdateKeys {
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Send an encrypted application message (CRDT update bytes)
    SendMessage {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Add an acceptor to the federation
    AddAcceptor {
        addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Remove an acceptor from the federation
    RemoveAcceptor {
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    /// Shutdown the actor
    Shutdown,
    /// Marker for generic types
    #[allow(dead_code)]
    _Marker(std::marker::PhantomData<(C, CS)>),
}

/// Messages from `AcceptorActor`s to `GroupActor`
#[allow(clippy::large_enum_variant)]
enum AcceptorInbound {
    /// Paxos response from an acceptor
    ProposalResponse {
        acceptor_id: AcceptorId,
        response: ProposalResponse,
    },
    /// Encrypted application message received from an acceptor
    EncryptedMessage { msg: EncryptedAppMessage },
    /// Acceptor connection failed or closed
    Disconnected { acceptor_id: AcceptorId },
}

/// A pending encrypted message waiting for epoch advancement
struct PendingMessage {
    /// The encrypted message
    msg: EncryptedAppMessage,
    /// Number of processing attempts
    attempts: u32,
}

/// Maximum number of times to retry processing a pending message
const MAX_MESSAGE_ATTEMPTS: u32 = 10;

/// Messages from `GroupActor` to `AcceptorActor`s
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum AcceptorOutbound {
    /// Send a Paxos proposal request
    ProposalRequest { request: ProposalRequest },
    /// Send an encrypted application message
    AppMessage { msg: EncryptedAppMessage },
}

// =============================================================================
// Public Types
// =============================================================================

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

// =============================================================================
// Group Handle (public API)
// =============================================================================

/// A synchronized MLS group that automatically handles consensus.
///
/// This is a handle to a background actor that manages the MLS group state.
/// All mutation methods send requests to the actor and await responses.
///
/// # Lifecycle
///
/// - On `Drop`: Sends cancel signal to background actors, does NOT wait
/// - On [`shutdown()`](Self::shutdown): Sends cancel signal and waits for clean termination
#[allow(clippy::struct_field_names)]
pub struct Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// Channel to send requests to the `GroupActor`
    request_tx: mpsc::Sender<GroupRequest<C, CS>>,

    /// Receiver for decrypted application messages (raw bytes)
    app_message_rx: mpsc::Receiver<Vec<u8>>,

    /// Event broadcaster
    event_tx: broadcast::Sender<GroupEvent>,

    /// Cancellation token for all actors
    cancel_token: CancellationToken,

    /// Handle to the group actor task
    actor_handle: Option<JoinHandle<()>>,

    /// Cached group ID (immutable after creation)
    group_id: GroupId,

    /// CRDT for application state synchronization
    crdt: Box<dyn Crdt>,
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
    /// * `connection_manager` - The connection manager for p2p connections
    /// * `acceptors` - Endpoint addresses of acceptors to register with (can be empty)
    /// * `crdt_factory` - CRDT factory to create the group's CRDT
    ///
    /// # Errors
    /// Returns an error if group creation or acceptor registration fails.
    pub(crate) async fn create(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        connection_manager: &ConnectionManager,
        acceptors: &[EndpointAddr],
        crdt_factory: &dyn CrdtFactory,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use crate::connector::register_group_with_addr;

        let crdt_type_id = crdt_factory.type_id().to_owned();

        // Create a new MLS group (blocking I/O via storage)
        let (learner, group_id, group_info_bytes) = blocking(|| {
            // Include CRDT type in group context extensions
            let mut group_context_extensions = mls_rs::ExtensionList::default();
            group_context_extensions
                .set_from(CrdtRegistrationExt::new(&crdt_type_id))
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach(OperationContext::CREATING_GROUP)
                        .attach(format!("failed to set CRDT extension: {e:?}"))
                })?;

            let group = client
                .create_group(
                    group_context_extensions,
                    mls_rs::ExtensionList::default(),
                    None,
                )
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach(OperationContext::CREATING_GROUP)
                        .attach(format!("MLS group creation failed: {e:?}"))
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
                        .attach(OperationContext::CREATING_GROUP)
                        .attach(format!("failed to create group info: {e:?}"))
                })?;
                Some(group_info_msg.to_bytes().map_err(|e| {
                    Report::new(GroupError)
                        .attach(OperationContext::CREATING_GROUP)
                        .attach(format!("failed to serialize group info: {e:?}"))
                })?)
            };

            Ok::<_, Report<GroupError>>((learner, group_id, group_info_bytes))
        })?;

        let endpoint = connection_manager.endpoint();

        // Register with acceptors (async network I/O) and add address hints
        if let Some(group_info_bytes) = group_info_bytes {
            for addr in acceptors {
                register_group_with_addr(endpoint, addr.clone(), &group_info_bytes)
                    .await
                    .map_err(|e| {
                        Report::new(GroupError)
                            .attach(format!("failed to register with acceptor: {e:?}"))
                    })?;
            }
        }

        // Add address hints for all acceptors to the connection manager
        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

        // Create CRDT from factory
        let crdt = crdt_factory.create();

        Ok(Self::spawn_actors(
            learner,
            group_id,
            endpoint.clone(),
            connection_manager.clone(),
            crdt,
        ))
    }

    /// Join an existing group from a Welcome message (or bundle).
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key
    /// * `cipher_suite` - The cipher suite provider
    /// * `connection_manager` - The connection manager for p2p connections
    /// * `welcome_bytes` - The serialized MLS Welcome message bytes
    /// * `crdt_factories` - Map of CRDT type IDs to factories for lookup
    ///
    /// # Errors
    /// Returns an error if joining fails or if the CRDT type is not supported.
    #[allow(clippy::unused_async)] // Keep async for API consistency
    pub(crate) async fn join(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        connection_manager: &ConnectionManager,
        welcome_bytes: &[u8],
        crdt_factories: &std::collections::HashMap<String, std::sync::Arc<dyn CrdtFactory>>,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use universal_sync_core::AcceptorsExt;

        // Join the group (blocking I/O via storage)
        let (learner, group_id, crdt_type_id, crdt_snapshot) = blocking(|| {
            // Parse the MLS Welcome message
            let welcome = MlsMessage::from_bytes(welcome_bytes).map_err(|e| {
                Report::new(GroupError)
                    .attach(OperationContext::JOINING_GROUP)
                    .attach(format!("invalid welcome message: {e:?}"))
            })?;

            // Join the group - info contains the GroupInfo extensions
            let (group, info) = client.join_group(None, &welcome, None).map_err(|e| {
                Report::new(GroupError)
                    .attach(OperationContext::JOINING_GROUP)
                    .attach(format!("MLS join failed: {e:?}"))
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
                        .attach(OperationContext::JOINING_GROUP)
                        .attach(format!("failed to read acceptors extension: {e:?}"))
                })?
                .map(|ext| ext.0)
                .unwrap_or_default();

            // Read CRDT snapshot from GroupInfo extensions
            let crdt_snapshot = info
                .group_info_extensions
                .get_as::<CrdtSnapshotExt>()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach(OperationContext::JOINING_GROUP)
                        .attach(format!("failed to read CRDT snapshot extension: {e:?}"))
                })?
                .ok_or_else(|| {
                    Report::new(GroupError)
                        .attach(OperationContext::JOINING_GROUP)
                        .attach("missing required CRDT snapshot extension in GroupInfo")
                })?;

            // Read CRDT type from group context extensions
            let crdt_type_id = group
                .context()
                .extensions
                .get_as::<CrdtRegistrationExt>()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach(OperationContext::JOINING_GROUP)
                        .attach(format!("failed to read CRDT extension: {e:?}"))
                })?
                .map_or_else(|| "none".to_owned(), |ext| ext.type_id);

            // Create the learner
            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors);

            Ok::<_, Report<GroupError>>((learner, group_id, crdt_type_id, crdt_snapshot))
        })?;

        // Look up the CRDT factory
        let crdt_factory = crdt_factories.get(&crdt_type_id).ok_or_else(|| {
            Report::new(GroupError).attach(format!(
                "CRDT type '{crdt_type_id}' not registered. Register a factory with register_crdt_factory()"
            ))
        })?;

        // Add address hints for all acceptors to the connection manager
        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

        // Create CRDT from the snapshot in the GroupInfo extensions
        let crdt = crdt_factory
            .from_snapshot(crdt_snapshot.snapshot())
            .map_err(|e| {
                Report::new(GroupError)
                    .attach(format!("failed to create CRDT from snapshot: {e:?}"))
            })?;

        Ok(Self::spawn_actors(
            learner,
            group_id,
            connection_manager.endpoint().clone(),
            connection_manager.clone(),
            crdt,
        ))
    }

    /// Spawn all actors and return a Group handle
    fn spawn_actors(
        learner: GroupLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        crdt: Box<dyn Crdt>,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(64);
        let (request_tx, request_rx) = mpsc::channel(64);
        let (app_message_tx, app_message_rx) = mpsc::channel(256);

        // Spawn the group actor
        let actor = GroupActor::new(
            learner,
            group_id,
            endpoint,
            connection_manager,
            request_rx,
            app_message_tx,
            event_tx.clone(),
            cancel_token.clone(),
        );

        let actor_handle = tokio::spawn(actor.run());

        Self {
            request_tx,
            app_message_rx,
            event_tx,
            cancel_token,
            actor_handle: Some(actor_handle),
            group_id,
            crdt,
        }
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
    /// The Welcome message is sent directly to the member's endpoint address,
    /// which is extracted from the key package's `MemberAddrExt` extension.
    ///
    /// # Errors
    /// Returns an error if the key package is missing the address extension,
    /// or if the operation fails after retries.
    pub async fn add_member(&mut self, key_package: MlsMessage) -> Result<(), Report<GroupError>> {
        use universal_sync_core::MemberAddrExt;

        // Extract the member address from the key package extensions
        let member_addr = key_package
            .as_key_package()
            .ok_or_else(|| Report::new(GroupError).attach("message is not a key package"))?
            .extensions
            .get_as::<MemberAddrExt>()
            .map_err(|e| {
                Report::new(GroupError)
                    .attach(format!("failed to read member address extension: {e:?}"))
            })?
            .ok_or_else(|| {
                Report::new(GroupError).attach(
                    "key package missing MemberAddrExt extension with member's endpoint address",
                )
            })?
            .0;

        // Capture CRDT snapshot from the Group-owned CRDT
        let crdt_snapshot = self.crdt.snapshot().map_err(|e| {
            Report::new(GroupError).attach(format!("failed to capture CRDT snapshot: {e:?}"))
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddMember {
                key_package: Box::new(key_package),
                member_addr,
                crdt_snapshot,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::RemoveMember {
                member_index,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
    }

    /// Update this member's keys (forward secrecy).
    ///
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn update_keys(&mut self) -> Result<(), Report<GroupError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::UpdateKeys { reply: reply_tx })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
    }

    /// Get a reference to the CRDT for reading state.
    #[must_use]
    pub fn crdt(&self) -> &dyn Crdt {
        &*self.crdt
    }

    /// Get a mutable reference to the CRDT for local mutations.
    ///
    /// After mutating, call [`send_update`](Self::send_update) to broadcast changes to peers.
    pub fn crdt_mut(&mut self) -> &mut dyn Crdt {
        &mut *self.crdt
    }

    /// Flush local CRDT changes and send the update to peers.
    ///
    /// No-op if there are no pending local changes.
    ///
    /// # Errors
    /// Returns an error if flushing or sending fails.
    pub async fn send_update(&mut self) -> Result<(), Report<GroupError>> {
        let update = self
            .crdt
            .flush_update()
            .map_err(|e| Report::new(GroupError).attach(format!("CRDT flush failed: {e:?}")))?;

        let Some(data) = update else {
            return Ok(());
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::SendMessage {
                data,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
    }

    /// Non-blocking: drain pending received updates and apply them to the CRDT.
    ///
    /// Returns `true` if any updates were applied.
    pub fn sync(&mut self) -> bool {
        let mut applied = false;
        while let Ok(data) = self.app_message_rx.try_recv() {
            if let Err(e) = self.crdt.apply(&data) {
                tracing::warn!(?e, "failed to apply CRDT update");
            } else {
                applied = true;
            }
        }
        applied
    }

    /// Async wait: blocks until at least one remote update arrives, then applies it.
    ///
    /// Returns `None` if the group is shutting down (channel closed).
    pub async fn wait_for_update(&mut self) -> Option<()> {
        let data = self.app_message_rx.recv().await?;
        if let Err(e) = self.crdt.apply(&data) {
            tracing::warn!(?e, "failed to apply CRDT update");
        }
        // Also drain any additional pending updates
        self.sync();
        Some(())
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddAcceptor {
                addr,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::RemoveAcceptor {
                acceptor_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
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
    /// This queries the group actor for the current state.
    ///
    /// # Errors
    ///
    /// Returns an error if the group actor has been closed.
    pub async fn context(&mut self) -> Result<GroupContext, Report<GroupError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::GetContext { reply: reply_tx })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))
    }

    /// Get the group ID
    #[must_use]
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Gracefully shut down the background actors.
    ///
    /// Unlike `Drop`, this waits for actors to complete.
    pub async fn shutdown(mut self) {
        // Send shutdown request
        let _ = self.request_tx.send(GroupRequest::Shutdown).await;
        self.cancel_token.cancel();
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
        }
    }
}

// =============================================================================
// GroupActor
// =============================================================================

/// The main actor that owns all MLS group state
struct GroupActor<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// The underlying MLS learner
    learner: GroupLearner<C, CS>,

    /// Paxos proposer
    proposer: Proposer<GroupLearner<C, CS>>,

    /// Quorum tracker for learning from acceptors
    quorum_tracker: QuorumTracker<GroupLearner<C, CS>>,

    /// Current attempt number for proposals
    attempt: universal_sync_core::Attempt,

    /// Group ID
    group_id: GroupId,

    /// Iroh endpoint for registering new acceptors
    #[allow(dead_code)]
    endpoint: Endpoint,

    /// Connection manager for opening streams
    connection_manager: ConnectionManager,

    /// Receiver for requests from the Group handle
    request_rx: mpsc::Receiver<GroupRequest<C, CS>>,

    /// Sender for decrypted application message data (raw CRDT update bytes)
    app_message_tx: mpsc::Sender<Vec<u8>>,

    /// Event broadcaster
    event_tx: broadcast::Sender<GroupEvent>,

    /// Cancellation token
    cancel_token: CancellationToken,

    /// Channels to acceptor actors (for sending outbound messages)
    acceptor_txs: HashMap<AcceptorId, mpsc::Sender<AcceptorOutbound>>,

    /// Receiver for inbound messages from acceptor actors
    acceptor_rx: mpsc::Receiver<AcceptorInbound>,

    /// Sender for acceptor inbound (cloned to acceptor actors)
    acceptor_inbound_tx: mpsc::Sender<AcceptorInbound>,

    /// Handles to acceptor actor tasks
    acceptor_handles: HashMap<AcceptorId, JoinHandle<()>>,

    /// Per-epoch message index counter (resets on epoch change)
    message_index: u32,

    /// The epoch for which `message_index` is valid
    message_epoch: Epoch,

    /// Deduplication: seen message identities (sender, epoch, index)
    seen_messages: HashSet<(MemberId, Epoch, u32)>,

    /// Pending encrypted messages for future epochs (buffered until we advance)
    pending_messages: Vec<PendingMessage>,

    /// The epoch when we joined the group (messages from earlier epochs cannot be decrypted)
    join_epoch: Epoch,

    /// Active proposal state (waiting for quorum)
    active_proposal: Option<ActiveProposal>,
}

/// State for an active proposal waiting for quorum
struct ActiveProposal {
    /// The proposal we're waiting for (stored for debugging)
    #[allow(dead_code)]
    proposal: GroupProposal,
    /// The message associated with the proposal (needed for retry)
    message: GroupMessage,
    /// The kind of reply to send when complete
    reply_kind: ProposalReplyKind,
    /// When this proposal started (for timeout)
    started_at: std::time::Instant,
    /// Number of retry attempts
    retries: u32,
}

/// Different kinds of replies for proposals
enum ProposalReplyKind {
    /// Simple success/failure reply
    Simple(oneshot::Sender<Result<(), Report<GroupError>>>),
    /// Reply with welcome message sent to new member
    WithWelcome {
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
}

/// Welcome message to send to a new member
struct WelcomeToSend {
    member_addr: EndpointAddr,
    welcome: Vec<u8>,
    reply: oneshot::Sender<Result<(), Report<GroupError>>>,
}

/// Maximum number of proposal retry attempts
const MAX_PROPOSAL_RETRIES: u32 = 3;
/// Timeout for proposal to reach quorum
const PROPOSAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl<C, CS> GroupActor<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        learner: GroupLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        request_rx: mpsc::Receiver<GroupRequest<C, CS>>,
        app_message_tx: mpsc::Sender<Vec<u8>>,
        event_tx: broadcast::Sender<GroupEvent>,
        cancel_token: CancellationToken,
    ) -> Self {
        let num_acceptors = learner.acceptor_ids().count();
        let message_epoch = learner.mls_epoch();
        let join_epoch = message_epoch; // Remember the epoch we joined at
        let (acceptor_inbound_tx, acceptor_rx) = mpsc::channel(256);

        Self {
            learner,
            proposer: Proposer::new(),
            quorum_tracker: QuorumTracker::new(num_acceptors),
            attempt: universal_sync_core::Attempt::default(),
            group_id,
            endpoint,
            connection_manager,
            request_rx,
            app_message_tx,
            event_tx,
            cancel_token,
            acceptor_txs: HashMap::new(),
            acceptor_rx,
            acceptor_inbound_tx,
            acceptor_handles: HashMap::new(),
            message_index: 0,
            message_epoch,
            seen_messages: HashSet::new(),
            pending_messages: Vec::new(),
            join_epoch,
            active_proposal: None,
        }
    }

    /// Run the actor's main loop
    async fn run(mut self) {
        // Spawn acceptor actors for all known acceptors
        self.spawn_acceptor_actors().await;

        // Interval for checking proposal timeouts
        let mut timeout_check = tokio::time::interval(std::time::Duration::from_secs(1));
        timeout_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                () = self.cancel_token.cancelled() => {
                    tracing::debug!("group actor cancelled");
                    break;
                }

                // Handle requests from the Group handle
                Some(request) = self.request_rx.recv() => {
                    if self.handle_request(request).await {
                        break; // Shutdown requested
                    }
                }

                // Handle messages from acceptor actors
                Some(inbound) = self.acceptor_rx.recv() => {
                    self.handle_acceptor_inbound(inbound).await;
                }

                // Check for proposal timeout
                _ = timeout_check.tick() => {
                    self.check_proposal_timeout();
                }
            }
        }

        // Cleanup: cancel all acceptor actors
        for (_, handle) in self.acceptor_handles.drain() {
            handle.abort();
        }
    }

    /// Check if the active proposal has timed out
    fn check_proposal_timeout(&mut self) {
        let timed_out = self
            .active_proposal
            .as_ref()
            .is_some_and(|a| a.started_at.elapsed() > PROPOSAL_TIMEOUT);

        if timed_out {
            tracing::warn!("proposal timed out, failing");
            self.learner.clear_pending_commit();
            if let Some(active) = self.active_proposal.take() {
                Self::complete_proposal_error(active, "proposal timed out");
            }
        }
    }

    /// Spawn acceptor actors for all known acceptors
    async fn spawn_acceptor_actors(&mut self) {
        let acceptors: Vec<_> = self.learner.acceptors().clone().into_iter().collect();
        for (acceptor_id, addr) in acceptors {
            self.spawn_acceptor_actor(acceptor_id, addr).await;
        }
    }

    /// Spawn a single acceptor actor
    async fn spawn_acceptor_actor(&mut self, acceptor_id: AcceptorId, addr: EndpointAddr) {
        self.spawn_acceptor_actor_inner(acceptor_id, addr, false)
            .await;
    }

    /// Spawn a single acceptor actor, optionally registering the group first
    async fn spawn_acceptor_actor_with_registration(
        &mut self,
        acceptor_id: AcceptorId,
        addr: EndpointAddr,
    ) {
        self.spawn_acceptor_actor_inner(acceptor_id, addr, true)
            .await;
    }

    /// Inner implementation for spawning acceptor actors
    #[allow(clippy::collapsible_if)]
    async fn spawn_acceptor_actor_inner(
        &mut self,
        acceptor_id: AcceptorId,
        addr: EndpointAddr,
        register: bool,
    ) {
        // Add address hint
        self.connection_manager
            .add_address_hint(acceptor_id, addr.clone())
            .await;

        // Register the group with the new acceptor if needed
        if register {
            if let Err(e) = self.register_group_with_acceptor(&addr).await {
                tracing::warn!(
                    ?acceptor_id,
                    ?e,
                    "failed to register group with new acceptor"
                );
                // Continue anyway - the acceptor actor will retry connections
            }
        }

        let (outbound_tx, outbound_rx) = mpsc::channel(64);
        self.acceptor_txs.insert(acceptor_id, outbound_tx);

        let actor = AcceptorActor {
            acceptor_id,
            group_id: self.group_id,
            connection_manager: self.connection_manager.clone(),
            outbound_rx,
            inbound_tx: self.acceptor_inbound_tx.clone(),
            cancel_token: self.cancel_token.clone(),
        };

        let handle = tokio::spawn(actor.run());
        self.acceptor_handles.insert(acceptor_id, handle);
    }

    /// Register this group with an acceptor
    async fn register_group_with_acceptor(
        &self,
        addr: &EndpointAddr,
    ) -> Result<(), Report<GroupError>> {
        use crate::connector::register_group_with_addr;

        // Get GroupInfo for registration (blocking I/O)
        let group_info_bytes = blocking(|| {
            let group_info =
                self.learner.group().group_info_message(true).map_err(|e| {
                    Report::new(GroupError).attach(format!("group info failed: {e:?}"))
                })?;
            group_info.to_bytes().change_context(GroupError)
        })?;

        // Register with the acceptor
        register_group_with_addr(
            self.connection_manager.endpoint(),
            addr.clone(),
            &group_info_bytes,
        )
        .await
        .change_context(GroupError)?;

        Ok(())
    }

    /// Handle a request from the Group handle. Returns true if shutdown was requested.
    async fn handle_request(&mut self, request: GroupRequest<C, CS>) -> bool {
        match request {
            GroupRequest::GetContext { reply } => {
                let context = self.get_context();
                let _ = reply.send(context);
            }
            GroupRequest::AddMember {
                key_package,
                member_addr,
                crdt_snapshot,
                reply,
            } => {
                self.handle_add_member(*key_package, member_addr, crdt_snapshot, reply)
                    .await;
            }
            GroupRequest::RemoveMember {
                member_index,
                reply,
            } => {
                self.handle_remove_member(member_index, reply).await;
            }
            GroupRequest::UpdateKeys { reply } => {
                self.handle_update_keys(reply).await;
            }
            GroupRequest::SendMessage { data, reply } => {
                self.handle_send_message(&data, reply);
            }
            GroupRequest::AddAcceptor { addr, reply } => {
                self.handle_add_acceptor(addr, reply).await;
            }
            GroupRequest::RemoveAcceptor { acceptor_id, reply } => {
                self.handle_remove_acceptor(acceptor_id, reply).await;
            }
            GroupRequest::Shutdown => {
                return true;
            }
            GroupRequest::_Marker(_) => unreachable!(),
        }
        false
    }

    /// Get the current group context
    fn get_context(&self) -> GroupContext {
        let mls_context = self.learner.group().context();
        GroupContext {
            group_id: self.group_id,
            epoch: self.learner.mls_epoch(),
            member_count: self.learner.group().roster().members().len(),
            acceptors: self.learner.acceptor_ids().collect(),
            confirmed_transcript_hash: mls_context.confirmed_transcript_hash.to_vec(),
        }
    }

    /// Handle add member request
    async fn handle_add_member(
        &mut self,
        key_package: MlsMessage,
        member_addr: EndpointAddr,
        crdt_snapshot: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        // Safety check: cannot add members without at least one acceptor
        let acceptor_count = self.learner.acceptor_ids().len();
        if acceptor_count == 0 {
            let _ = reply.send(Err(Report::new(GroupError).attach(
                "cannot add members to a group without acceptors: add an acceptor first",
            )));
            return;
        }

        // Build the commit with CRDT snapshot in GroupInfo extensions
        let result = blocking(|| {
            let group_info_ext = welcome_group_info_extensions(
                self.learner.acceptors().values().cloned(),
                crdt_snapshot,
            );

            let commit_output = self
                .learner
                .group_mut()
                .commit_builder()
                .add_member(key_package)
                .change_context(GroupError)?
                .set_group_info_ext(group_info_ext)
                .build()
                .change_context(GroupError)?;

            // Extract the welcome message bytes
            let welcome_bytes = commit_output
                .welcome_messages
                .first()
                .ok_or_else(|| Report::new(GroupError).attach("no welcome message generated"))?
                .to_bytes()
                .map_err(|e| {
                    Report::new(GroupError).attach(format!("welcome serialization failed: {e:?}"))
                })?;

            Ok::<_, Report<GroupError>>((commit_output, welcome_bytes))
        });

        match result {
            Ok((commit_output, welcome_bytes)) => {
                let message = GroupMessage::new(commit_output.commit_message);
                self.start_proposal_with_welcome(message, member_addr, welcome_bytes, reply)
                    .await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Handle remove member request
    async fn handle_remove_member(
        &mut self,
        member_index: u32,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .remove_member(member_index)
                .change_context(GroupError)?
                .build()
                .change_context(GroupError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                self.start_proposal(message, reply).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Handle update keys request
    async fn handle_update_keys(&mut self, reply: oneshot::Sender<Result<(), Report<GroupError>>>) {
        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .build()
                .change_context(GroupError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                self.start_proposal(message, reply).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Handle send message request
    fn handle_send_message(
        &mut self,
        data: &[u8],
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        // Get current epoch and check if we need to reset the message counter
        let current_epoch = self.learner.mls_epoch();
        if current_epoch != self.message_epoch {
            self.message_index = 0;
            self.message_epoch = current_epoch;
        }

        let sender = MemberId(self.learner.group().current_member_index());
        let index = self.message_index;
        self.message_index = self.message_index.wrapping_add(1);

        // Only include the index in authenticated data
        let authenticated_data = index.to_be_bytes().to_vec();

        // Encrypt the message
        let result = blocking(|| {
            self.learner
                .encrypt_application_message(data, authenticated_data)
        });

        let mls_message = match result {
            Ok(msg) => msg,
            Err(e) => {
                let _ = reply.send(Err(
                    Report::new(GroupError).attach(format!("encrypt failed: {e:?}"))
                ));
                return;
            }
        };

        let ciphertext = match mls_message.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                let _ = reply.send(Err(
                    Report::new(GroupError).attach(format!("serialize failed: {e:?}"))
                ));
                return;
            }
        };

        let encrypted_msg = EncryptedAppMessage {
            ciphertext: ciphertext.clone(),
        };

        // Select acceptors using rendezvous hashing
        let message_id = MessageId {
            group_id: self.group_id,
            epoch: current_epoch,
            sender,
            index,
        };
        let acceptor_ids: Vec<_> = self.learner.acceptor_ids().collect();
        let delivery_count = crate::rendezvous::delivery_count(acceptor_ids.len());
        let selected_acceptors =
            crate::rendezvous::select_acceptors(&acceptor_ids, &message_id, delivery_count);

        // Send to selected acceptors
        for acceptor_id in selected_acceptors {
            if let Some(tx) = self.acceptor_txs.get(&acceptor_id) {
                let _ = tx.try_send(AcceptorOutbound::AppMessage {
                    msg: encrypted_msg.clone(),
                });
            }
        }

        let _ = reply.send(Ok(()));
    }

    /// Handle add acceptor request
    async fn handle_add_acceptor(
        &mut self,
        addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorAdd;

        // Build commit with AcceptorAdd extension
        let result = blocking(|| {
            let add_ext = AcceptorAdd::new(addr.clone());
            let mut extensions = ExtensionList::default();
            extensions.set_from(add_ext).change_context(GroupError)?;

            self.learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .change_context(GroupError)?
                .build()
                .change_context(GroupError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                // Store the addr so we can register after consensus
                // For now, just start the proposal
                self.start_proposal(message, reply).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Handle remove acceptor request
    async fn handle_remove_acceptor(
        &mut self,
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorRemove;

        // Safety check: cannot remove the last acceptor if the group has multiple members
        let acceptor_count = self.learner.acceptor_ids().len();
        let member_count = self.learner.group().roster().members_iter().count();

        if acceptor_count <= 1 && member_count > 1 {
            let _ = reply.send(Err(Report::new(GroupError).attach(
                "cannot remove the last acceptor from a group with multiple members: \
                 remove other members first or add another acceptor",
            )));
            return;
        }

        let result = blocking(|| {
            let remove_ext = AcceptorRemove::new(acceptor_id);
            let mut extensions = ExtensionList::default();
            extensions.set_from(remove_ext).change_context(GroupError)?;

            self.learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .change_context(GroupError)?
                .build()
                .change_context(GroupError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                self.start_proposal(message, reply).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Start a proposal and wait for consensus
    async fn start_proposal(
        &mut self,
        message: GroupMessage,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                // Immediately learned (no acceptors)
                self.apply_proposal(&proposal, message).await;
                let _ = reply.send(Ok(()));
            }
            ProposeResult::Continue(messages) => {
                // Send prepare messages to acceptors
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::Simple(reply),
                    started_at: std::time::Instant::now(),
                    retries: 0,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                // Retry with higher attempt number
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                tracing::debug!(?self.attempt, "proposal rejected immediately, will retry");
                // Re-queue the proposal request
                let () = self.retry_proposal_simple(message, reply, 0).await;
            }
        }
    }

    /// Start a proposal that sends a welcome message to a new member
    async fn start_proposal_with_welcome(
        &mut self,
        message: GroupMessage,
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                // Immediately learned (no acceptors)
                self.apply_proposal(&proposal, message).await;
                self.send_welcome_to_member(member_addr, welcome, reply)
                    .await;
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::WithWelcome {
                        member_addr,
                        welcome,
                        reply,
                    },
                    started_at: std::time::Instant::now(),
                    retries: 0,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                // Retry with higher attempt number
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                tracing::debug!(?self.attempt, "proposal rejected immediately, will retry");
                let () = self
                    .retry_proposal_with_welcome(message, member_addr, welcome, reply, 0)
                    .await;
            }
        }
    }

    /// Retry a simple proposal
    async fn retry_proposal_simple(
        &mut self,
        message: GroupMessage,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
        retries: u32,
    ) {
        if retries >= MAX_PROPOSAL_RETRIES {
            let _ = reply.send(Err(
                Report::new(GroupError).attach("max proposal retries exceeded")
            ));
            return;
        }

        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                self.apply_proposal(&proposal, message).await;
                let _ = reply.send(Ok(()));
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::Simple(reply),
                    started_at: std::time::Instant::now(),
                    retries,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                // Recursive retry with backoff
                tokio::time::sleep(std::time::Duration::from_millis(
                    10 * (u64::from(retries) + 1),
                ))
                .await;
                Box::pin(self.retry_proposal_simple(message, reply, retries + 1)).await;
            }
        }
    }

    /// Retry a proposal with welcome message
    async fn retry_proposal_with_welcome(
        &mut self,
        message: GroupMessage,
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
        retries: u32,
    ) {
        if retries >= MAX_PROPOSAL_RETRIES {
            let _ = reply.send(Err(
                Report::new(GroupError).attach("max proposal retries exceeded")
            ));
            return;
        }

        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                self.apply_proposal(&proposal, message).await;
                self.send_welcome_to_member(member_addr, welcome, reply)
                    .await;
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::WithWelcome {
                        member_addr,
                        welcome,
                        reply,
                    },
                    started_at: std::time::Instant::now(),
                    retries,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                tokio::time::sleep(std::time::Duration::from_millis(
                    10 * (u64::from(retries) + 1),
                ))
                .await;
                Box::pin(self.retry_proposal_with_welcome(
                    message,
                    member_addr,
                    welcome,
                    reply,
                    retries + 1,
                ))
                .await;
            }
        }
    }

    /// Complete a proposal with success (returns Some if welcome needs to be sent)
    fn complete_proposal_success_sync(active: ActiveProposal) -> Option<WelcomeToSend> {
        match active.reply_kind {
            ProposalReplyKind::Simple(reply) => {
                let _ = reply.send(Ok(()));
                None
            }
            ProposalReplyKind::WithWelcome {
                member_addr,
                welcome,
                reply,
            } => Some(WelcomeToSend {
                member_addr,
                welcome,
                reply,
            }),
        }
    }

    /// Complete a proposal with error
    fn complete_proposal_error(active: ActiveProposal, message: &'static str) {
        let reply = match active.reply_kind {
            ProposalReplyKind::Simple(reply) | ProposalReplyKind::WithWelcome { reply, .. } => {
                reply
            }
        };
        let _ = reply.send(Err(Report::new(GroupError).attach(message)));
    }

    /// Complete a proposal with success, sending welcome if needed
    async fn complete_proposal_success(&mut self, active: ActiveProposal) {
        if let Some(welcome_to_send) = Self::complete_proposal_success_sync(active) {
            self.send_welcome_to_member(
                welcome_to_send.member_addr,
                welcome_to_send.welcome,
                welcome_to_send.reply,
            )
            .await;
        }
    }

    /// Send a welcome message to a new member
    async fn send_welcome_to_member(
        &self,
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let result = self.send_welcome_inner(&member_addr, &welcome).await;
        let _ = reply.send(result);
    }

    /// Inner function to send welcome message
    async fn send_welcome_inner(
        &self,
        member_addr: &EndpointAddr,
        welcome: &[u8],
    ) -> Result<(), Report<GroupError>> {
        use futures::SinkExt;
        use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

        tracing::debug!(?member_addr, "connecting to new member to send welcome");

        // Connect to the new member's endpoint
        let conn = self
            .endpoint
            .connect(member_addr.clone(), PAXOS_ALPN)
            .await
            .map_err(|e| {
                Report::new(GroupError).attach(format!("failed to connect to new member: {e:?}"))
            })?;

        tracing::debug!("connected to new member, opening stream");

        // Open a stream and send the welcome handshake
        let (send, _recv) = conn.open_bi().await.map_err(|e| {
            Report::new(GroupError).attach(format!("failed to open stream to new member: {e:?}"))
        })?;

        let mut framed = FramedWrite::new(send, LengthDelimitedCodec::new());

        // Send the welcome handshake
        let handshake = Handshake::SendWelcome(welcome.to_vec());
        let handshake_bytes = postcard::to_stdvec(&handshake).map_err(|e| {
            Report::new(GroupError).attach(format!("failed to serialize handshake: {e:?}"))
        })?;

        framed.send(handshake_bytes.into()).await.map_err(|e| {
            Report::new(GroupError).attach(format!("failed to send welcome: {e:?}"))
        })?;

        tracing::debug!("sent welcome handshake, finishing stream");

        // Finish the stream and wait for it to be acknowledged
        let mut send = framed.into_inner();
        send.finish().change_context(GroupError)?;

        // Wait for the stream to be fully closed (data acknowledged)
        send.stopped().await.change_context(GroupError)?;

        tracing::debug!("welcome sent successfully");

        Ok(())
    }

    /// Send a proposal request to an acceptor
    fn send_proposal_request(
        &self,
        acceptor_id: AcceptorId,
        request: universal_sync_paxos::AcceptorRequest<GroupLearner<C, CS>>,
    ) {
        let wire_request = match request {
            universal_sync_paxos::AcceptorRequest::Prepare(p) => ProposalRequest::Prepare(p),
            universal_sync_paxos::AcceptorRequest::Accept(p, m) => ProposalRequest::Accept(p, m),
        };

        if let Some(tx) = self.acceptor_txs.get(&acceptor_id) {
            if let Err(e) = tx.try_send(AcceptorOutbound::ProposalRequest {
                request: wire_request,
            }) {
                tracing::warn!(?acceptor_id, ?e, "failed to queue proposal request");
            }
        } else {
            tracing::warn!(?acceptor_id, "no acceptor actor found for proposal request");
        }
    }

    /// Handle an inbound message from an acceptor actor
    async fn handle_acceptor_inbound(&mut self, inbound: AcceptorInbound) {
        match inbound {
            AcceptorInbound::ProposalResponse {
                acceptor_id,
                response,
            } => {
                self.handle_proposal_response(acceptor_id, response).await;
            }
            AcceptorInbound::EncryptedMessage { msg } => {
                self.handle_encrypted_message(msg);
            }
            AcceptorInbound::Disconnected { acceptor_id } => {
                tracing::warn!(?acceptor_id, "acceptor disconnected");
                self.acceptor_txs.remove(&acceptor_id);
                if let Some(handle) = self.acceptor_handles.remove(&acceptor_id) {
                    handle.abort();
                }
            }
        }
    }

    /// Handle a proposal response from an acceptor
    async fn handle_proposal_response(
        &mut self,
        acceptor_id: AcceptorId,
        response: ProposalResponse,
    ) {
        // Convert wire format to AcceptorMessage
        let acceptor_msg: AcceptorMessage<GroupLearner<C, CS>> = AcceptorMessage {
            promised: response.promised,
            accepted: response.accepted,
        };

        // Track for learning (even if not our proposal)
        if let Some((proposal, message)) = acceptor_msg.accepted.clone() {
            self.quorum_tracker.track(proposal, message);

            // Check if we learned something for current epoch
            let current_epoch = self.learner.mls_epoch();
            if let Some((learned_p, learned_m)) = self.quorum_tracker.check_quorum(current_epoch) {
                let learned_p = learned_p.clone();
                let learned_m = learned_m.clone();

                // Check if this is our proposal
                let my_id = self.learner.node_id();
                let is_ours = learned_p.member_id == my_id;

                self.apply_proposal(&learned_p, learned_m).await;

                // Complete the active proposal if it was ours
                if is_ours {
                    if let Some(active) = self.active_proposal.take() {
                        self.complete_proposal_success(active).await;
                    }
                } else {
                    // Someone else won - fail our active proposal
                    if let Some(active) = self.active_proposal.take() {
                        self.learner.clear_pending_commit();
                        Self::complete_proposal_error(active, "another proposal won");
                    }
                }

                return;
            }
        }

        // If we have an active proposal, process through proposer
        if self.active_proposal.is_some() {
            let result = self
                .proposer
                .receive(&self.learner, acceptor_id, acceptor_msg);

            match result {
                ProposeResult::Continue(messages) => {
                    for (acc_id, request) in messages {
                        self.send_proposal_request(acc_id, request);
                    }
                }
                ProposeResult::Learned { proposal, message } => {
                    self.apply_proposal(&proposal, message).await;

                    if let Some(active) = self.active_proposal.take() {
                        self.complete_proposal_success(active).await;
                    }
                }
                ProposeResult::Rejected { superseded_by } => {
                    self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                    self.learner.clear_pending_commit();

                    // Retry the proposal if we haven't exceeded max retries
                    if let Some(active) = self.active_proposal.take() {
                        let retries = active.retries + 1;
                        if retries > MAX_PROPOSAL_RETRIES {
                            tracing::warn!(retries, "max proposal retries exceeded");
                            Self::complete_proposal_error(active, "max proposal retries exceeded");
                        } else {
                            tracing::debug!(retries, "proposal rejected, retrying");
                            // Small backoff before retry
                            tokio::time::sleep(std::time::Duration::from_millis(
                                10 * u64::from(retries),
                            ))
                            .await;
                            self.retry_active_proposal(active, retries).await;
                        }
                    }
                }
            }
        }
    }

    /// Retry an active proposal after rejection
    async fn retry_active_proposal(&mut self, active: ActiveProposal, retries: u32) {
        let result = self
            .proposer
            .propose(&self.learner, self.attempt, active.message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                self.apply_proposal(&proposal, message).await;
                self.complete_proposal_success(active).await;
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message: active.message,
                    reply_kind: active.reply_kind,
                    started_at: active.started_at, // Keep original start time
                    retries,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();

                if retries + 1 > MAX_PROPOSAL_RETRIES {
                    Self::complete_proposal_error(active, "max proposal retries exceeded");
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        10 * u64::from(retries + 1),
                    ))
                    .await;
                    Box::pin(self.retry_active_proposal(active, retries + 1)).await;
                }
            }
        }
    }

    /// Handle an encrypted message from an acceptor
    fn handle_encrypted_message(&mut self, msg: EncryptedAppMessage) {
        use mls_rs::MlsMessage;

        // Deserialize the MLS message
        let mls_message = match MlsMessage::from_bytes(&msg.ciphertext) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(?e, "failed to deserialize MLS message");
                return;
            }
        };

        // Try to process through MLS
        let received = blocking(|| {
            self.learner
                .group_mut()
                .process_incoming_message(mls_message)
        });

        let received = match received {
            Ok(msg) => msg,
            Err(e) => {
                // Could be from a future epoch - buffer it for later retry
                tracing::debug!(?e, "failed to process message, buffering for retry");
                self.pending_messages
                    .push(PendingMessage { msg, attempts: 1 });
                return;
            }
        };

        self.handle_decrypted_message(received);
    }

    /// Apply a proposal to the MLS group state
    async fn apply_proposal(&mut self, proposal: &GroupProposal, message: GroupMessage) {
        let my_id = self.learner.node_id();
        let my_proposal = proposal.member_id == my_id;

        let effect = blocking(|| {
            if my_proposal && self.learner.has_pending_commit() {
                tracing::debug!("applying our own pending commit");
                let effect = self.learner.group_mut().apply_pending_commit().ok()?;
                Some(effect.effect)
            } else {
                if self.learner.has_pending_commit() {
                    tracing::debug!("clearing pending commit - another proposal won");
                    self.learner.clear_pending_commit();
                }

                let result = self
                    .learner
                    .group_mut()
                    .process_incoming_message(message.mls_message)
                    .ok()?;

                match result {
                    ReceivedMessage::Commit(commit_desc) => Some(commit_desc.effect),
                    _ => None,
                }
            }
        });

        if let Some(effect) = effect {
            let (events, new_acceptors) = self.process_commit_effect(&effect);
            for event in events {
                let _ = self.event_tx.send(event);
            }

            // Register and spawn actors for newly added acceptors
            for (acceptor_id, addr) in new_acceptors {
                self.spawn_acceptor_actor_with_registration(acceptor_id, addr)
                    .await;
            }

            // Reinitialize quorum tracker with new acceptor count
            let num_acceptors = self.learner.acceptor_ids().count();
            self.quorum_tracker = QuorumTracker::new(num_acceptors);
        }

        // Reset attempt on successful apply
        self.attempt = universal_sync_core::Attempt::default();

        // Try to process any pending messages
        self.try_process_pending_messages();
    }

    /// Process a commit effect and return events + new acceptors to spawn
    fn process_commit_effect(
        &mut self,
        effect: &CommitEffect,
    ) -> (Vec<GroupEvent>, Vec<(AcceptorId, EndpointAddr)>) {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => {
                return (vec![GroupEvent::ReInitiated], vec![]);
            }
        };

        let mut events = Vec::new();
        let mut new_acceptors = Vec::new();

        for proposal_info in applied_proposals {
            let event = match &proposal_info.proposal {
                MlsProposal::Add(add_proposal) => {
                    let _ = add_proposal;
                    GroupEvent::MemberAdded { index: 0 }
                }
                MlsProposal::Remove(remove_proposal) => GroupEvent::MemberRemoved {
                    index: remove_proposal.to_remove(),
                },
                MlsProposal::ReInit(_) => GroupEvent::ReInitiated,
                MlsProposal::ExternalInit(_) => GroupEvent::ExternalInit,
                MlsProposal::GroupContextExtensions(extensions) => {
                    if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                        let id = add.acceptor_id();
                        let addr = add.0.clone();
                        self.learner.add_acceptor_addr(addr.clone());
                        // Queue for spawning after this function returns
                        new_acceptors.push((id, addr));
                        GroupEvent::AcceptorAdded { id }
                    } else if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                        let id = remove.acceptor_id();
                        self.learner.remove_acceptor_id(&id);
                        // Remove acceptor actor
                        if let Some(handle) = self.acceptor_handles.remove(&id) {
                            handle.abort();
                        }
                        self.acceptor_txs.remove(&id);
                        GroupEvent::AcceptorRemoved { id }
                    } else {
                        GroupEvent::ExtensionsUpdated
                    }
                }
                _ => GroupEvent::Unknown,
            };

            events.push(event);
        }

        (events, new_acceptors)
    }

    /// Try to process any pending messages that may now be decryptable
    fn try_process_pending_messages(&mut self) {
        use mls_rs::MlsMessage;

        let pending = std::mem::take(&mut self.pending_messages);
        let current_epoch = self.learner.mls_epoch();
        let mut still_pending = Vec::new();

        for mut pending_msg in pending {
            // Check if message has exceeded retry limit
            if pending_msg.attempts >= MAX_MESSAGE_ATTEMPTS {
                tracing::debug!(
                    attempts = pending_msg.attempts,
                    "dropping message after max retry attempts"
                );
                continue;
            }

            // Try to deserialize and process
            let Ok(mls_message) = MlsMessage::from_bytes(&pending_msg.msg.ciphertext) else {
                tracing::debug!("dropping malformed pending message");
                continue;
            };

            // Try to process
            let result = blocking(|| {
                self.learner
                    .group_mut()
                    .process_incoming_message(mls_message)
            });

            match result {
                Ok(received) => {
                    // Successfully processed - handle like normal
                    self.handle_decrypted_message(received);
                }
                Err(e) => {
                    // Still can't process - check if it's worth retrying
                    pending_msg.attempts += 1;

                    // Don't retry if we've advanced past this message's likely epoch
                    // (heuristic: if we're more than 5 epochs ahead and still failing, give up)
                    if current_epoch.0 > self.join_epoch.0 + 5 && pending_msg.attempts > 3 {
                        tracing::debug!(
                            ?e,
                            current_epoch = current_epoch.0,
                            join_epoch = self.join_epoch.0,
                            "dropping old pending message"
                        );
                        continue;
                    }

                    still_pending.push(pending_msg);
                }
            }
        }

        self.pending_messages = still_pending;
    }

    /// Handle a successfully decrypted message
    fn handle_decrypted_message(&mut self, received: ReceivedMessage) {
        let ReceivedMessage::ApplicationMessage(app_msg) = received else {
            tracing::debug!("received non-application message on message stream");
            return;
        };

        // Extract message identity from authenticated data
        let index = if app_msg.authenticated_data.len() >= 4 {
            u32::from_be_bytes(app_msg.authenticated_data[..4].try_into().unwrap_or([0; 4]))
        } else {
            0
        };

        let sender = MemberId(app_msg.sender_index);
        let epoch = self.learner.mls_epoch();

        // Deduplicate
        let key = (sender, epoch, index);
        if self.seen_messages.contains(&key) {
            return;
        }
        self.seen_messages.insert(key);

        // Forward raw CRDT update bytes to the Group handle
        let _ = self.app_message_tx.try_send(app_msg.data().to_vec());
    }
}

// =============================================================================
// AcceptorActor
// =============================================================================

/// Actor managing connection to a single acceptor
struct AcceptorActor {
    acceptor_id: AcceptorId,
    group_id: GroupId,
    connection_manager: ConnectionManager,
    outbound_rx: mpsc::Receiver<AcceptorOutbound>,
    inbound_tx: mpsc::Sender<AcceptorInbound>,
    cancel_token: CancellationToken,
}

/// Maximum reconnection attempts before giving up
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
/// Initial reconnection delay
const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
/// Maximum reconnection delay
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(10);

impl AcceptorActor {
    async fn run(mut self) {
        let mut reconnect_attempts = 0u32;
        let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

            // Try to connect and run
            match self.run_connection().await {
                ConnectionResult::Cancelled => break,
                ConnectionResult::Disconnected => {
                    reconnect_attempts += 1;
                    if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                        tracing::warn!(
                            acceptor_id = ?self.acceptor_id,
                            "max reconnect attempts reached, giving up"
                        );
                        break;
                    }

                    tracing::debug!(
                        acceptor_id = ?self.acceptor_id,
                        attempt = reconnect_attempts,
                        delay_ms = reconnect_delay.as_millis(),
                        "reconnecting to acceptor"
                    );

                    // Wait before reconnecting
                    tokio::select! {
                        () = tokio::time::sleep(reconnect_delay) => {}
                        () = self.cancel_token.cancelled() => break,
                    }

                    // Exponential backoff with cap
                    reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                }
            }
        }

        // Notify group actor we disconnected
        let _ = self
            .inbound_tx
            .send(AcceptorInbound::Disconnected {
                acceptor_id: self.acceptor_id,
            })
            .await;
    }

    /// Run a single connection session. Returns when disconnected or cancelled.
    #[allow(clippy::too_many_lines)]
    async fn run_connection(&mut self) -> ConnectionResult {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
        use universal_sync_core::{MessageRequest, MessageResponse};

        // Open proposal stream (required for Paxos)
        let proposal_streams = self
            .connection_manager
            .open_proposal_stream(&self.acceptor_id, self.group_id)
            .await;

        let (proposal_send, proposal_recv) = match proposal_streams {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to open proposal stream");
                return ConnectionResult::Disconnected;
            }
        };

        let (mut proposal_writer, mut proposal_reader) =
            crate::connector::make_proposal_streams(proposal_send, proposal_recv);

        // Try to open message stream (optional - for application messages)
        let message_streams = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            self.connection_manager
                .open_message_stream(&self.acceptor_id, self.group_id),
        )
        .await;

        let message_io: Option<(
            FramedWrite<iroh::endpoint::SendStream, LengthDelimitedCodec>,
            FramedRead<iroh::endpoint::RecvStream, LengthDelimitedCodec>,
        )> = match message_streams {
            Ok(Ok((message_send, message_recv))) => {
                let mut message_writer =
                    FramedWrite::new(message_send, LengthDelimitedCodec::new());
                let message_reader = FramedRead::new(message_recv, LengthDelimitedCodec::new());

                let subscribe_request = MessageRequest::Subscribe { since_seq: 0 };
                if let Ok(request_bytes) = postcard::to_allocvec(&subscribe_request) {
                    if message_writer.send(request_bytes.into()).await.is_ok() {
                        Some((message_writer, message_reader))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Ok(Err(_)) | Err(_) => None,
        };

        let (mut message_writer_opt, mut message_reader_opt) = match message_io {
            Some((w, r)) => (Some(w), Some(r)),
            None => (None, None),
        };

        // Connection established - run the main loop
        loop {
            tokio::select! {
                biased;

                () = self.cancel_token.cancelled() => {
                    return ConnectionResult::Cancelled;
                }

                Some(outbound) = self.outbound_rx.recv() => {
                    match outbound {
                        AcceptorOutbound::ProposalRequest { request } => {
                            if let Err(e) = proposal_writer.send(request).await {
                                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to send proposal");
                                return ConnectionResult::Disconnected;
                            }
                        }
                        AcceptorOutbound::AppMessage { msg } => {
                            if let Some(ref mut writer) = message_writer_opt {
                                let request = MessageRequest::Send(msg);
                                if let Ok(request_bytes) = postcard::to_allocvec(&request) {
                                    let _ = writer.send(request_bytes.into()).await;
                                }
                            }
                        }
                    }
                }

                Some(result) = proposal_reader.next() => {
                    match result {
                        Ok(response) => {
                            let _ = self.inbound_tx.send(AcceptorInbound::ProposalResponse {
                                acceptor_id: self.acceptor_id,
                                response,
                            }).await;
                        }
                        Err(e) => {
                            tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "proposal stream error");
                            return ConnectionResult::Disconnected;
                        }
                    }
                }

                Some(result) = async {
                    if let Some(ref mut reader) = message_reader_opt {
                        reader.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Ok(bytes) = result {
                        if let Ok(MessageResponse::Message { message, .. }) = postcard::from_bytes(&bytes) {
                            let _ = self.inbound_tx.send(AcceptorInbound::EncryptedMessage {
                                msg: message,
                            }).await;
                        }
                    } else {
                        // Don't disconnect for message stream errors
                        message_reader_opt = None;
                        message_writer_opt = None;
                    }
                }
            }
        }
    }
}

/// Result of a connection attempt
enum ConnectionResult {
    /// Connection was cancelled
    Cancelled,
    /// Connection disconnected (should reconnect)
    Disconnected,
}

/// Wait for an incoming welcome message on the endpoint.
///
/// This should be called before the group leader calls `add_member`.
/// Returns the welcome bytes that can be passed to [`Group::join`].
///
/// # Errors
/// Returns an error if no welcome is received or the connection fails.
pub(crate) async fn wait_for_welcome(endpoint: &Endpoint) -> Result<Vec<u8>, Report<GroupError>> {
    use futures::StreamExt;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    // Wait for an incoming connection
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| Report::new(GroupError).attach("endpoint closed"))?;

    let conn = incoming
        .accept()
        .change_context(GroupError)?
        .await
        .change_context(GroupError)?;

    // Accept a bidirectional stream
    let (_send, recv) = conn.accept_bi().await.change_context(GroupError)?;

    // Read the handshake
    let mut framed = FramedRead::new(recv, LengthDelimitedCodec::new());

    let handshake_bytes = framed
        .next()
        .await
        .ok_or_else(|| Report::new(GroupError).attach("no handshake received"))?
        .change_context(GroupError)?;

    let handshake: Handshake = postcard::from_bytes(&handshake_bytes).change_context(GroupError)?;

    match handshake {
        Handshake::SendWelcome(welcome) => Ok(welcome),
        _ => {
            Err(Report::new(GroupError)
                .attach("expected SendWelcome handshake, got something else"))
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
