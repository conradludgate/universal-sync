//! Self-driving [`Weaver`] wrapper with background actors for MLS + Paxos.
//!
//! ```text
//! Weaver (handle)
//!   ├─► GroupActor (owns WeaverLearner, Proposer, QuorumTracker)
//!   │     ├─► AcceptorActor[0] ──► iroh connection
//!   │     ├─► AcceptorActor[1] ──► iroh connection
//!   │     └─► AcceptorActor[n] ──► iroh connection
//!   └─► app_message_rx (decrypted messages)
//! ```

mod acceptor_actor;
mod group_actor;

use std::collections::BTreeSet;
use std::fmt;
use std::time::Instant;

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, ClientId, Crdt, EncryptedAppMessage, Epoch, GroupContextExt, GroupId, GroupInfoExt,
    Handshake, KeyPackageExt, LeafNodeExt, MessageId,
};
use iroh::Endpoint;
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn compute_invite_hmac(hmac_key: &[u8], endpoint_id: &[u8; 32], group_id: &GroupId) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(hmac_key).expect("valid key size");
    mac.update(endpoint_id);
    mac.update(group_id.as_bytes());
    mac.finalize().into_bytes().into()
}

pub(crate) type GroupRegistry = std::sync::Arc<
    std::sync::Mutex<std::collections::HashMap<GroupId, mpsc::Sender<GroupRequest>>>,
>;

use crate::connection::ConnectionManager;
use crate::connector::ProposalRequest;
use crate::group_state::FjallGroupStateStorage;
use crate::learner::{WeaverLearner, fingerprint_of_member};

pub(crate) struct WeaverInfra {
    pub connection_manager: ConnectionManager,
    pub storage: FjallGroupStateStorage,
    pub group_registry: GroupRegistry,
}

/// Marker error for group operations. Use `error_stack::Report<WeaverError>` with
/// context attachments for details.
#[derive(Debug)]
pub struct WeaverError;

impl fmt::Display for WeaverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("group operation failed")
    }
}

impl std::error::Error for WeaverError {}

/// Build `GroupInfo` extensions containing the acceptor list.
#[must_use]
pub(crate) fn group_info_ext_list(
    acceptors: impl IntoIterator<Item = AcceptorId>,
) -> mls_rs::ExtensionList {
    let ext = GroupInfoExt::new(acceptors);
    let mut extensions = mls_rs::ExtensionList::default();
    extensions
        .set_from(ext)
        .expect("GroupInfoExt encoding should not fail");
    extensions
}

/// Create a `GroupInfo` MLS message with a [`GroupInfoExt`] extension.
pub(crate) fn group_info_with_ext<C: mls_rs::client_builder::MlsConfig>(
    group: &mls_rs::Group<C>,
    acceptors: impl IntoIterator<Item = AcceptorId>,
) -> Result<Box<MlsMessage>, Report<WeaverError>> {
    let extensions = group_info_ext_list(acceptors);
    group
        .group_info_message_internal(extensions, true)
        .map(Box::new)
        .change_context(WeaverError)
}

pub(crate) enum GroupRequest {
    GetContext {
        reply: oneshot::Sender<WeaverContext>,
    },
    AddMember {
        key_package: Box<MlsMessage>,
        member_endpoint_id: [u8; 32],
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    UpdateKeys {
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    SendMessage {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    AddAcceptor {
        id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    RemoveAcceptor {
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    CompactSnapshot {
        snapshot: bytes::Bytes,
        level: u8,
        force: bool,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    GenerateInvite {
        reply: oneshot::Sender<Result<Vec<u8>, Report<WeaverError>>>,
    },
    VerifyInviteTag {
        hmac_tag: [u8; 32],
        reply: oneshot::Sender<Result<bool, Report<WeaverError>>>,
    },
    IncomingKeyPackage {
        key_package: Box<MlsMessage>,
        hmac_tag: [u8; 32],
    },
    Shutdown,
}

#[allow(clippy::large_enum_variant)]
enum AcceptorInbound {
    ProposalResponse {
        acceptor_id: AcceptorId,
        response: crate::connector::ProposalResponse,
    },
    EncryptedMessage {
        msg: EncryptedAppMessage,
    },
    Connected {
        acceptor_id: AcceptorId,
    },
    Disconnected {
        acceptor_id: AcceptorId,
    },
}

struct PendingMessage {
    msg: EncryptedAppMessage,
    attempts: u32,
}

const MAX_MESSAGE_ATTEMPTS: u32 = 10;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum AcceptorOutbound {
    ProposalRequest {
        request: ProposalRequest,
    },
    AppMessage {
        id: MessageId,
        level: u8,
        msg: EncryptedAppMessage,
    },
}

/// Events emitted by a [`Weaver`]. `CompactionNeeded` should be handled by the
/// application by calling [`Weaver::compact`] (or [`Weaver::force_compact`] if `force` is set).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum WeaverEvent {
    /// A new member was added to the group at the given roster index.
    MemberAdded { index: u32 },
    /// A member was removed from the group.
    MemberRemoved { index: u32 },
    /// A spool server was added to the group.
    SpoolAdded { id: AcceptorId },
    /// A spool server was removed from the group.
    SpoolRemoved { id: AcceptorId },
    /// An iroh connection to a spool was established.
    SpoolConnected { id: AcceptorId },
    /// An iroh connection to a spool was lost.
    SpoolDisconnected { id: AcceptorId },
    /// The group was re-initialised (e.g. cipher suite change).
    ReInitiated,
    /// An external commit joined the group.
    ExternalInit,
    /// MLS group-level extensions were updated.
    ExtensionsUpdated,
    /// The MLS epoch advanced after a commit was applied.
    EpochAdvanced { epoch: u64 },
    /// Compaction finished at this level. Superseded messages will be
    /// garbage-collected by acceptors.
    CompactionCompleted { level: u8 },
    /// The message count at this level exceeded the compaction threshold.
    /// The application should call [`Weaver::compact`] (or
    /// [`Weaver::force_compact`] when `force` is true) to merge older
    /// updates into a snapshot.
    CompactionNeeded { level: u8, force: bool },
    /// An unrecognised commit effect. Future-proofing for new proposal types.
    Unknown,
}

/// Information about a single member in the group roster.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    /// MLS roster index.
    pub index: u32,
    /// Raw identity bytes from the member's MLS credential.
    pub identity: Vec<u8>,
    /// `true` if this entry represents the local device.
    pub is_self: bool,
    /// Stable identifier derived from the member's signing key and group.
    pub client_id: ClientId,
}

/// Point-in-time snapshot of a group's metadata.
///
/// Obtained via [`Weaver::context`].
#[derive(Debug, Clone)]
pub struct WeaverContext {
    /// Unique identifier for this group.
    pub group_id: GroupId,
    /// Current MLS epoch (increments on every commit).
    pub epoch: Epoch,
    /// Number of members in the roster (including blank slots from removals).
    pub member_count: usize,
    /// Detailed info for each occupied roster slot.
    pub members: Vec<MemberInfo>,
    /// Spool servers assigned to this group.
    pub spools: Vec<AcceptorId>,
    /// Subset of `spools` that currently have a live connection.
    pub connected_spools: BTreeSet<AcceptorId>,
    /// MLS confirmed transcript hash for the current epoch.
    pub confirmed_transcript_hash: Vec<u8>,
}

/// Returned by [`WeaverClient::join`] so the caller can construct the
/// appropriate CRDT before starting to sync.
pub struct JoinInfo {
    /// The newly joined group handle.
    pub group: Weaver,
    /// Application protocol name stored in the group's MLS extensions.
    pub protocol_name: String,
}

/// Handle to a synchronized MLS group with automatic Paxos consensus.
///
/// All mutations are sent to a background actor. Drop cancels actors without waiting;
/// use [`shutdown()`](Self::shutdown) for graceful termination.
const DEFAULT_BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

#[allow(clippy::struct_field_names)]
pub struct Weaver {
    request_tx: mpsc::Sender<GroupRequest>,
    app_message_rx: mpsc::Receiver<Vec<u8>>,
    event_tx: broadcast::Sender<WeaverEvent>,
    cancel_guard: tokio_util::sync::DropGuard,
    actor_handle: Option<JoinHandle<()>>,
    group_id: GroupId,
    client_id: ClientId,
    in_flight_reply: Option<oneshot::Receiver<Result<(), Report<WeaverError>>>>,
    batch_deadline: Option<Instant>,
}

/// `block_in_place` on multi-threaded runtimes, direct call on single-threaded.
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

impl Weaver {
    pub(crate) async fn create<C, CS>(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        infra: &WeaverInfra,
        acceptors: &[AcceptorId],
        protocol_name: &str,
    ) -> Result<Self, Report<WeaverError>>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        use crate::connector::register_group;

        let protocol_name = protocol_name.to_owned();

        let (learner, group_id, group_info) = blocking(|| {
            let mut group_context_extensions = mls_rs::ExtensionList::default();
            group_context_extensions
                .set_from(GroupContextExt::new(&protocol_name, Some(86400)))
                .change_context(WeaverError)
                .attach(OperationContext::CREATING_GROUP)?;

            let mut leaf_node_extensions = mls_rs::ExtensionList::default();
            leaf_node_extensions
                .set_from(LeafNodeExt::random())
                .change_context(WeaverError)
                .attach(OperationContext::CREATING_GROUP)?;

            let group = client
                .create_group(group_context_extensions, leaf_node_extensions, None)
                .change_context(WeaverError)
                .attach(OperationContext::CREATING_GROUP)?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);
            let learner =
                WeaverLearner::new(group, signer, cipher_suite, acceptors.iter().copied());

            let group_info = if acceptors.is_empty() {
                None
            } else {
                Some(
                    group_info_with_ext(learner.group(), acceptors.iter().copied())
                        .attach(OperationContext::CREATING_GROUP)?,
                )
            };

            Ok::<_, Report<WeaverError>>((learner, group_id, group_info))
        })?;

        let endpoint = infra.connection_manager.endpoint();

        if let Some(group_info) = group_info {
            for id in acceptors {
                register_group(endpoint, id, group_info.clone())
                    .await
                    .change_context(WeaverError)?;
            }
        }

        Ok(Self::spawn_actors(learner, group_id, infra, Some(86400)))
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn join<C, CS>(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        infra: &WeaverInfra,
        welcome: &MlsMessage,
    ) -> Result<JoinInfo, Report<WeaverError>>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let (learner, group_id, protocol_name, key_rotation_interval_secs) = blocking(|| {
            let (group, info) = client
                .join_group(None, welcome, None)
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

            let group_info_ext = info
                .group_info_extensions
                .get_as::<GroupInfoExt>()
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let acceptor_ids: Vec<AcceptorId> = group_info_ext
                .as_ref()
                .map(|e| e.acceptors.clone())
                .unwrap_or_default();

            let group_ctx = group
                .context()
                .extensions
                .get_as::<GroupContextExt>()
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let protocol_name = group_ctx
                .as_ref()
                .map_or_else(|| "none".to_owned(), |e| e.protocol_name.clone());

            let key_rotation_interval_secs = group_ctx
                .as_ref()
                .and_then(|e| e.key_rotation_interval_secs);

            let learner = WeaverLearner::new(group, signer, cipher_suite, acceptor_ids);

            Ok::<_, Report<WeaverError>>((
                learner,
                group_id,
                protocol_name,
                key_rotation_interval_secs,
            ))
        })?;

        let group = Self::spawn_actors(learner, group_id, infra, key_rotation_interval_secs);

        Ok(JoinInfo {
            group,
            protocol_name,
        })
    }

    fn spawn_actors<C, CS>(
        learner: WeaverLearner<C, CS>,
        group_id: GroupId,
        infra: &WeaverInfra,
        key_rotation_interval_secs: Option<u64>,
    ) -> Self
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Send + Sync + 'static,
    {
        let client_id = {
            let my_index = learner.group().current_member_index();
            learner
                .group()
                .roster()
                .member_with_index(my_index)
                .map(|m| fingerprint_of_member(&group_id, &m).as_client_id())
                .unwrap_or(ClientId(0))
        };

        let cancel_token = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(64);
        let (request_tx, request_rx) = mpsc::channel(64);
        let (app_message_tx, app_message_rx) = mpsc::channel(4096);

        infra
            .group_registry
            .lock()
            .unwrap()
            .insert(group_id, request_tx.clone());

        let key_rotation_interval = key_rotation_interval_secs.map(std::time::Duration::from_secs);

        let actor = group_actor::GroupActor::new(
            learner,
            group_id,
            infra.connection_manager.endpoint().clone(),
            infra.connection_manager.clone(),
            infra.storage.clone(),
            request_rx,
            app_message_tx,
            event_tx.clone(),
            cancel_token.clone(),
            key_rotation_interval,
        );

        let actor_handle = tokio::spawn(actor.run());

        let cancel_guard = cancel_token.clone().drop_guard();

        Self {
            request_tx,
            app_message_rx,
            event_tx,
            cancel_guard,
            actor_handle: Some(actor_handle),
            group_id,
            client_id,
            in_flight_reply: None,
            batch_deadline: None,
        }
    }

    /// Add a member using a serialised MLS key package.
    /// Welcome is sent directly via the key package's [`KeyPackageExt`].
    /// Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the key package is invalid or consensus fails.
    pub async fn add_member(
        &mut self,
        key_package_bytes: &[u8],
    ) -> Result<(), Report<WeaverError>> {
        let key_package = MlsMessage::from_bytes(key_package_bytes).change_context(WeaverError)?;

        let member_endpoint_id = key_package
            .as_key_package()
            .ok_or_else(|| Report::new(WeaverError).attach("message is not a key package"))?
            .extensions
            .get_as::<KeyPackageExt>()
            .change_context(WeaverError)?
            .ok_or_else(|| {
                Report::new(WeaverError)
                    .attach("key package missing KeyPackageExt with member's endpoint identity")
            })?
            .endpoint_id;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddMember {
                key_package: Box::new(key_package),
                member_endpoint_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Remove a member by index. Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or consensus fails.
    pub async fn remove_member(&mut self, member_index: u32) -> Result<(), Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::RemoveMember {
                member_index,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Update the MLS key material. Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or consensus fails.
    pub async fn update_keys(&mut self) -> Result<(), Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::UpdateKeys { reply: reply_tx })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Flush local CRDT changes and broadcast. No-op if no pending changes.
    ///
    /// Batches updates: defers calling [`Crdt::flush`] while a previous send is
    /// in-flight and the batch timeout has not been reached. Once the in-flight
    /// send is confirmed or the timeout fires, flushes (including all edits since
    /// last confirm) and sends one message. Uses a two-phase protocol so the CRDT
    /// state vector only advances after the send succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if flushing the CRDT or sending the message fails.
    pub async fn send_update(&mut self, crdt: &mut impl Crdt) -> Result<(), Report<WeaverError>> {
        if let Some(rx) = self.in_flight_reply.take() {
            let deadline_passed = self.batch_deadline.is_some_and(|d| Instant::now() >= d);
            if !deadline_passed {
                self.in_flight_reply = Some(rx);
                return Ok(());
            }
            rx.await
                .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;
            crdt.confirm_flush(0);
            self.batch_deadline = None;
        }

        let update = crdt.flush(0).change_context(WeaverError)?;

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
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        self.in_flight_reply = Some(reply_rx);
        self.batch_deadline = Some(Instant::now() + DEFAULT_BATCH_TIMEOUT);
        Ok(())
    }

    /// Wait for any in-flight send to complete and confirm the flush.
    /// Call before comparing CRDT state (e.g. in tests) so the last batch is confirmed.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the in-flight send failed.
    pub async fn wait_pending_send(
        &mut self,
        crdt: &mut impl Crdt,
    ) -> Result<(), Report<WeaverError>> {
        if let Some(rx) = self.in_flight_reply.take() {
            self.batch_deadline = None;
            rx.await
                .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;
            crdt.confirm_flush(0);
        }
        Ok(())
    }

    /// Wait for in-flight send, then flush and send any remaining CRDT state until
    /// [`Crdt::flush`] returns `None`. Ensures all local edits are sent before
    /// comparing state (e.g. in tests).
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if any send fails.
    pub async fn flush_remaining(
        &mut self,
        crdt: &mut impl Crdt,
    ) -> Result<(), Report<WeaverError>> {
        self.wait_pending_send(crdt).await?;
        loop {
            let update = crdt.flush(0).change_context(WeaverError)?;
            let Some(data) = update else {
                break;
            };
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(GroupRequest::SendMessage {
                    data,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;
            reply_rx
                .await
                .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;
            crdt.confirm_flush(0);
        }
        Ok(())
    }

    /// Non-blocking: drain and apply pending CRDT updates. Returns `true` if any applied.
    pub fn sync(&mut self, crdt: &mut impl Crdt) -> bool {
        let mut applied = false;
        while let Ok(data) = self.app_message_rx.try_recv() {
            if let Err(e) = crdt.apply(&data) {
                tracing::warn!(?e, "failed to apply CRDT update");
            } else {
                applied = true;
            }
        }
        applied
    }

    /// Blocks until a remote update arrives. Returns `None` if shutting down.
    pub async fn wait_for_update(&mut self, crdt: &mut impl Crdt) -> Option<()> {
        let data = self.app_message_rx.recv().await?;
        if let Err(e) = crdt.apply(&data) {
            tracing::warn!(?e, "failed to apply CRDT update");
        }
        self.sync(crdt);
        Some(())
    }

    /// Flush the CRDT at the given compaction level and send the resulting
    /// snapshot to acceptors for encryption, distribution, and
    /// `CompactionComplete` consensus.
    ///
    /// Unlike [`Weaver::send_update`], compaction does **not** confirm the
    /// flush. This means each compaction always produces a full snapshot
    /// (from `StateVector::default()`), which is needed for re-encryption
    /// when new members join.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if flushing, encryption, or consensus fails.
    pub async fn compact(
        &mut self,
        crdt: &mut impl Crdt,
        level: u8,
    ) -> Result<(), Report<WeaverError>> {
        self.compact_inner(crdt, level, false).await
    }

    /// Force-compact at the given level, producing a full snapshot by
    /// resetting flush state first. Used on member join.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if flushing, encryption, or consensus fails.
    pub async fn force_compact(
        &mut self,
        crdt: &mut impl Crdt,
        level: u8,
    ) -> Result<(), Report<WeaverError>> {
        self.compact_inner(crdt, level, true).await
    }

    async fn compact_inner(
        &mut self,
        crdt: &mut impl Crdt,
        level: u8,
        force: bool,
    ) -> Result<(), Report<WeaverError>> {
        if force {
            crdt.reset_flush(usize::from(level));
        }
        let data = crdt.flush(usize::from(level)).change_context(WeaverError)?;
        let Some(data) = data else {
            return Ok(());
        };
        let snapshot = bytes::Bytes::from(data);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::CompactSnapshot {
                snapshot,
                level,
                force,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;

        for i in 0..=usize::from(level) {
            crdt.confirm_flush(i);
        }

        Ok(())
    }

    /// Proposes adding a spool via consensus, then registers the group with it.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or consensus fails.
    pub async fn add_spool(&mut self, id: AcceptorId) -> Result<(), Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddAcceptor {
                id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Remove a spool from the group via consensus.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or consensus fails.
    pub async fn remove_spool(
        &mut self,
        acceptor_id: AcceptorId,
    ) -> Result<(), Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::RemoveAcceptor {
                acceptor_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Subscribe to [`WeaverEvent`] notifications.
    ///
    /// Events are broadcast; each subscriber gets its own receiver.
    /// Lagging receivers will skip older events.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<WeaverEvent> {
        self.event_tx.subscribe()
    }

    /// Retrieve a snapshot of the group's current metadata.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed.
    pub async fn context(&mut self) -> Result<WeaverContext, Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::GetContext { reply: reply_tx })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))
    }

    /// Returns the unique identifier for this group.
    #[must_use]
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the stable client identifier for the local device within this group.
    #[must_use]
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or the export secret fails.
    pub async fn generate_invite(&self) -> Result<Vec<u8>, Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::GenerateInvite { reply: reply_tx })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or the export secret fails.
    pub async fn verify_invite_tag(&self, hmac_tag: [u8; 32]) -> Result<bool, Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::VerifyInviteTag {
                hmac_tag,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))?
    }

    /// Unlike `Drop`, this waits for actors to complete.
    pub async fn shutdown(mut self) {
        let _ = self.request_tx.send(GroupRequest::Shutdown).await;
        self.cancel_guard.disarm().cancel();
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
        }
    }
}

pub(crate) enum IncomingHandshake {
    Welcome(Box<MlsMessage>),
    KeyPackage(GroupId, Box<MlsMessage>, [u8; 32]),
}

/// Waits for an incoming handshake (welcome or key package) on the endpoint.
pub(crate) async fn wait_for_incoming(
    endpoint: &Endpoint,
) -> Result<IncomingHandshake, Report<WeaverError>> {
    use futures::StreamExt;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| Report::new(WeaverError).attach("endpoint closed"))?;

    let conn = incoming
        .accept()
        .change_context(WeaverError)?
        .await
        .change_context(WeaverError)?;

    let (_send, recv) = conn.accept_bi().await.change_context(WeaverError)?;
    let mut framed = FramedRead::new(recv, LengthDelimitedCodec::new());

    let handshake_bytes = framed
        .next()
        .await
        .ok_or_else(|| Report::new(WeaverError).attach("no handshake received"))?
        .change_context(WeaverError)?;

    let handshake: Handshake =
        postcard::from_bytes(&handshake_bytes).change_context(WeaverError)?;

    match handshake {
        Handshake::SendWelcome { welcome } => Ok(IncomingHandshake::Welcome(welcome)),
        Handshake::SendKeyPackage {
            group_id,
            key_package,
            hmac_tag,
        } => Ok(IncomingHandshake::KeyPackage(
            group_id,
            key_package,
            hmac_tag,
        )),
        _ => Err(Report::new(WeaverError).attach("unexpected handshake type on client endpoint")),
    }
}

use filament_core::OperationContext;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WeaverEvent>();
    }
}
