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

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, ClientId, CompactionConfig, Crdt, EncryptedAppMessage, Epoch, GroupContextExt,
    GroupId, GroupInfoExt, Handshake, KeyPackageExt, LeafNodeExt, MessageId,
    default_compaction_config,
};
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::connection::ConnectionManager;
use crate::connector::ProposalRequest;
use crate::learner::{WeaverLearner, fingerprint_of_member};

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
    acceptors: impl IntoIterator<Item = EndpointAddr>,
) -> mls_rs::ExtensionList {
    let mut extensions = mls_rs::ExtensionList::default();
    extensions
        .set_from(GroupInfoExt::new(acceptors, vec![]))
        .expect("GroupInfoExt encoding should not fail");
    extensions
}

/// Create a serialised `GroupInfo` message with a [`GroupInfoExt`] extension.
pub(crate) fn group_info_with_ext<C: mls_rs::client_builder::MlsConfig>(
    group: &mls_rs::Group<C>,
    acceptors: impl IntoIterator<Item = EndpointAddr>,
) -> Result<Vec<u8>, Report<WeaverError>> {
    let extensions = group_info_ext_list(acceptors);
    let msg = group
        .group_info_message_internal(extensions, true)
        .change_context(WeaverError)?;
    msg.to_bytes().change_context(WeaverError)
}

enum GroupRequest {
    GetContext {
        reply: oneshot::Sender<WeaverContext>,
    },
    AddMember {
        key_package: Box<MlsMessage>,
        member_addr: EndpointAddr,
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
        addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    RemoveAcceptor {
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
    CompactSnapshot {
        snapshot: bytes::Bytes,
        level: u8,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
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
        msg: EncryptedAppMessage,
    },
}

/// Events emitted by a [`Weaver`]. `CompactionNeeded` should be handled by the
/// application by calling [`Weaver::compact`].
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
    /// A compaction lease was claimed at this level. Another device is
    /// compacting; no action required.
    CompactionClaimed { level: u8 },
    /// Compaction finished at this level. Superseded messages will be
    /// garbage-collected by acceptors.
    CompactionCompleted { level: u8 },
    /// The message count at this level exceeded the compaction threshold.
    /// The application should call [`Weaver::compact`] to merge older
    /// updates into a snapshot.
    CompactionNeeded { level: u8 },
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
    /// Optional CRDT snapshot from the most recent compaction. `None` if the
    /// group has never been compacted; the device will catch up via backfill.
    pub snapshot: Option<bytes::Bytes>,
}

/// Handle to a synchronized MLS group with automatic Paxos consensus.
///
/// All mutations are sent to a background actor. Drop cancels actors without waiting;
/// use [`shutdown()`](Self::shutdown) for graceful termination.
#[allow(clippy::struct_field_names)]
pub struct Weaver {
    request_tx: mpsc::Sender<GroupRequest>,
    app_message_rx: mpsc::Receiver<Vec<u8>>,
    event_tx: broadcast::Sender<WeaverEvent>,
    cancel_guard: tokio_util::sync::DropGuard,
    actor_handle: Option<JoinHandle<()>>,
    group_id: GroupId,
    client_id: ClientId,
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
        connection_manager: &ConnectionManager,
        acceptors: &[EndpointAddr],
        protocol_name: &str,
        compaction_config: CompactionConfig,
    ) -> Result<Self, Report<WeaverError>>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        use crate::connector::register_group_with_addr;

        let protocol_name = protocol_name.to_owned();

        let (learner, group_id, group_info_bytes) = blocking(|| {
            let mut group_context_extensions = mls_rs::ExtensionList::default();
            group_context_extensions
                .set_from(GroupContextExt::new(
                    &protocol_name,
                    compaction_config.clone(),
                    Some(86400),
                ))
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
                WeaverLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

            let group_info_bytes = if acceptors.is_empty() {
                None
            } else {
                Some(
                    group_info_with_ext(learner.group(), acceptors.iter().cloned())
                        .attach(OperationContext::CREATING_GROUP)?,
                )
            };

            Ok::<_, Report<WeaverError>>((learner, group_id, group_info_bytes))
        })?;

        let endpoint = connection_manager.endpoint();

        if let Some(group_info_bytes) = group_info_bytes {
            for addr in acceptors {
                register_group_with_addr(endpoint, addr.clone(), &group_info_bytes)
                    .await
                    .change_context(WeaverError)?;
            }
        }

        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

        Ok(Self::spawn_actors(
            learner,
            group_id,
            endpoint.clone(),
            connection_manager.clone(),
            compaction_config,
            Some(86400),
        ))
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn join<C, CS>(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        connection_manager: &ConnectionManager,
        welcome_bytes: &[u8],
    ) -> Result<JoinInfo, Report<WeaverError>>
    where
        C: MlsConfig + Clone + Send + Sync + 'static,
        CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    {
        let (
            learner,
            group_id,
            protocol_name,
            compaction_config,
            crdt_snapshot_opt,
            key_rotation_interval_secs,
        ) = blocking(|| {
            let welcome = MlsMessage::from_bytes(welcome_bytes)
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let (group, info) = client
                .join_group(None, &welcome, None)
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

            let group_info_ext = info
                .group_info_extensions
                .get_as::<GroupInfoExt>()
                .change_context(WeaverError)
                .attach(OperationContext::JOINING_GROUP)?;

            let acceptors = group_info_ext
                .as_ref()
                .map(|e| e.acceptors.clone())
                .unwrap_or_default();

            let crdt_snapshot_opt = group_info_ext.map(|e| e.snapshot);

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

            let compaction_config =
                group_ctx.map_or_else(default_compaction_config, |e| e.compaction_config);

            let learner = WeaverLearner::new(group, signer, cipher_suite, acceptors);

            Ok::<_, Report<WeaverError>>((
                learner,
                group_id,
                protocol_name,
                compaction_config,
                crdt_snapshot_opt,
                key_rotation_interval_secs,
            ))
        })?;

        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

        let snapshot = match crdt_snapshot_opt {
            Some(s) if !s.is_empty() => Some(s),
            _ => {
                tracing::info!("joining group without CRDT snapshot, will catch up via backfill");
                None
            }
        };

        let group = Self::spawn_actors(
            learner,
            group_id,
            connection_manager.endpoint().clone(),
            connection_manager.clone(),
            compaction_config,
            key_rotation_interval_secs,
        );

        Ok(JoinInfo {
            group,
            protocol_name,
            snapshot,
        })
    }

    fn spawn_actors<C, CS>(
        learner: WeaverLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        compaction_config: CompactionConfig,
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
        let (app_message_tx, app_message_rx) = mpsc::channel(256);

        let key_rotation_interval = key_rotation_interval_secs.map(std::time::Duration::from_secs);

        let actor = group_actor::GroupActor::new(
            learner,
            group_id,
            endpoint,
            connection_manager,
            request_rx,
            app_message_tx,
            event_tx.clone(),
            cancel_token.clone(),
            compaction_config,
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

        let member_addr = key_package
            .as_key_package()
            .ok_or_else(|| Report::new(WeaverError).attach("message is not a key package"))?
            .extensions
            .get_as::<KeyPackageExt>()
            .change_context(WeaverError)?
            .ok_or_else(|| {
                Report::new(WeaverError)
                    .attach("key package missing KeyPackageExt with member's endpoint address")
            })?
            .addr;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddMember {
                key_package: Box::new(key_package),
                member_addr,
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
    /// Uses a two-phase protocol: the CRDT's internal state vector is only
    /// advanced after the send succeeds. If the send fails, the next call
    /// will re-encode from the last confirmed state, automatically including
    /// any new edits (batching).
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if flushing the CRDT or sending the message fails.
    pub async fn send_update(&mut self, crdt: &mut impl Crdt) -> Result<(), Report<WeaverError>> {
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

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;

        crdt.confirm_flush(0);
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
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(WeaverError).attach("group actor dropped reply"))??;

        Ok(())
    }

    /// Proposes adding a spool via consensus, then registers the group with it.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if the group actor is closed or consensus fails.
    pub async fn add_spool(&mut self, addr: EndpointAddr) -> Result<(), Report<WeaverError>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::AddAcceptor {
                addr,
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

    /// Unlike `Drop`, this waits for actors to complete.
    pub async fn shutdown(mut self) {
        let _ = self.request_tx.send(GroupRequest::Shutdown).await;
        self.cancel_guard.disarm().cancel();
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
        }
    }
}

/// Waits for an incoming welcome message on the endpoint.
/// Should be called before the group leader calls `add_member`.
pub(crate) async fn wait_for_welcome(
    endpoint: &Endpoint,
) -> Result<bytes::Bytes, Report<WeaverError>> {
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
        Handshake::SendWelcome(welcome) => Ok(welcome),
        _ => {
            Err(Report::new(WeaverError)
                .attach("expected SendWelcome handshake, got something else"))
        }
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
