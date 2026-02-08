//! Self-driving [`Group`] wrapper with background actors for MLS + Paxos.
//!
//! ```text
//! Group (handle)
//!   ├─► GroupActor (owns GroupLearner, Proposer, QuorumTracker)
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
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_core::{
    AcceptorId, CompactionConfig, Crdt, EncryptedAppMessage, Epoch, GroupContextExt, GroupId,
    GroupInfoExt, Handshake, KeyPackageExt, LeafNodeExt, MessageId, default_compaction_config,
};

use crate::connection::ConnectionManager;
use crate::connector::ProposalRequest;
use crate::learner::{GroupLearner, fingerprint_of_member};

/// Marker error for group operations. Use `error_stack::Report<GroupError>` with
/// context attachments for details.
#[derive(Debug)]
pub struct GroupError;

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("group operation failed")
    }
}

impl std::error::Error for GroupError {}

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
) -> Result<Vec<u8>, Report<GroupError>> {
    let extensions = group_info_ext_list(acceptors);
    let msg = group
        .group_info_message_internal(extensions, true)
        .change_context(GroupError)?;
    msg.to_bytes().change_context(GroupError)
}

enum GroupRequest<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    GetContext {
        reply: oneshot::Sender<GroupContext>,
    },
    AddMember {
        key_package: Box<MlsMessage>,
        member_addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    UpdateKeys {
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    SendMessage {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    AddAcceptor {
        addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    RemoveAcceptor {
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    CompactSnapshot {
        snapshot: Vec<u8>,
        level: u8,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    Shutdown,
    #[allow(dead_code)]
    _Marker(std::marker::PhantomData<(C, CS)>),
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

/// Events emitted by a Group. `CompactionNeeded` should be handled by the
/// application by calling [`Group::compact`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum GroupEvent {
    MemberAdded { index: u32 },
    MemberRemoved { index: u32 },
    AcceptorAdded { id: AcceptorId },
    AcceptorRemoved { id: AcceptorId },
    AcceptorConnected { id: AcceptorId },
    AcceptorDisconnected { id: AcceptorId },
    ReInitiated,
    ExternalInit,
    ExtensionsUpdated,
    EpochAdvanced { epoch: u64 },
    CompactionClaimed { level: u8 },
    CompactionCompleted { level: u8 },
    CompactionNeeded { level: u8 },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub index: u32,
    pub identity: Vec<u8>,
    pub is_self: bool,
    pub client_id: u64,
}

#[derive(Debug, Clone)]
pub struct GroupContext {
    pub group_id: GroupId,
    pub epoch: Epoch,
    pub member_count: usize,
    pub members: Vec<MemberInfo>,
    pub acceptors: Vec<AcceptorId>,
    pub connected_acceptors: BTreeSet<AcceptorId>,
    pub confirmed_transcript_hash: Vec<u8>,
}

/// Returned by [`Group::join`] so the caller can construct the right CRDT.
pub struct JoinInfo<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    pub group: Group<C, CS>,
    pub protocol_name: String,
    pub snapshot: Option<Vec<u8>>,
}

/// Handle to a synchronized MLS group with automatic Paxos consensus.
///
/// All mutations are sent to a background actor. Drop cancels actors without waiting;
/// use [`shutdown()`](Self::shutdown) for graceful termination.
#[allow(clippy::struct_field_names)]
pub struct Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    request_tx: mpsc::Sender<GroupRequest<C, CS>>,
    app_message_rx: mpsc::Receiver<Vec<u8>>,
    event_tx: broadcast::Sender<GroupEvent>,
    cancel_guard: tokio_util::sync::DropGuard,
    actor_handle: Option<JoinHandle<()>>,
    group_id: GroupId,
    my_client_id: u64,
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

impl<C, CS> Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    pub(crate) async fn create(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        connection_manager: &ConnectionManager,
        acceptors: &[EndpointAddr],
        protocol_name: &str,
        compaction_config: CompactionConfig,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
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
                .change_context(GroupError)
                .attach(OperationContext::CREATING_GROUP)?;

            let mut leaf_node_extensions = mls_rs::ExtensionList::default();
            leaf_node_extensions
                .set_from(LeafNodeExt::random())
                .change_context(GroupError)
                .attach(OperationContext::CREATING_GROUP)?;

            let group = client
                .create_group(group_context_extensions, leaf_node_extensions, None)
                .change_context(GroupError)
                .attach(OperationContext::CREATING_GROUP)?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);
            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

            let group_info_bytes = if acceptors.is_empty() {
                None
            } else {
                Some(
                    group_info_with_ext(learner.group(), acceptors.iter().cloned())
                        .attach(OperationContext::CREATING_GROUP)?,
                )
            };

            Ok::<_, Report<GroupError>>((learner, group_id, group_info_bytes))
        })?;

        let endpoint = connection_manager.endpoint();

        if let Some(group_info_bytes) = group_info_bytes {
            for addr in acceptors {
                register_group_with_addr(endpoint, addr.clone(), &group_info_bytes)
                    .await
                    .change_context(GroupError)?;
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
    pub(crate) async fn join(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        connection_manager: &ConnectionManager,
        welcome_bytes: &[u8],
    ) -> Result<JoinInfo<C, CS>, Report<GroupError>>
    where
        CS: Clone,
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
                .change_context(GroupError)
                .attach(OperationContext::JOINING_GROUP)?;

            let (group, info) = client
                .join_group(None, &welcome, None)
                .change_context(GroupError)
                .attach(OperationContext::JOINING_GROUP)?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

            let group_info_ext = info
                .group_info_extensions
                .get_as::<GroupInfoExt>()
                .change_context(GroupError)
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
                .change_context(GroupError)
                .attach(OperationContext::JOINING_GROUP)?;

            let protocol_name = group_ctx
                .as_ref()
                .map_or_else(|| "none".to_owned(), |e| e.protocol_name.clone());

            let key_rotation_interval_secs = group_ctx
                .as_ref()
                .and_then(|e| e.key_rotation_interval_secs);

            let compaction_config =
                group_ctx.map_or_else(default_compaction_config, |e| e.compaction_config);

            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors);

            Ok::<_, Report<GroupError>>((
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

    fn spawn_actors(
        learner: GroupLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        compaction_config: CompactionConfig,
        key_rotation_interval_secs: Option<u64>,
    ) -> Self {
        let my_client_id = {
            let my_index = learner.group().current_member_index();
            learner
                .group()
                .roster()
                .member_with_index(my_index)
                .map(|m| fingerprint_of_member(&group_id, &m).as_client_id())
                .unwrap_or(0)
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
            my_client_id,
        }
    }
}

impl<C, CS> Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// Add a member. Welcome is sent directly via the key package's [`KeyPackageExt`].
    /// Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if the key package is invalid or consensus fails.
    pub async fn add_member(&mut self, key_package: MlsMessage) -> Result<(), Report<GroupError>> {
        let member_addr = key_package
            .as_key_package()
            .ok_or_else(|| Report::new(GroupError).attach("message is not a key package"))?
            .extensions
            .get_as::<KeyPackageExt>()
            .change_context(GroupError)?
            .ok_or_else(|| {
                Report::new(GroupError)
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
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
    }

    /// Remove a member by index. Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if the group actor is closed or consensus fails.
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

    /// Update the MLS key material. Blocks until consensus is reached.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if the group actor is closed or consensus fails.
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

    /// Flush local CRDT changes and broadcast. No-op if no pending changes.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if flushing the CRDT or sending the message fails.
    pub async fn send_update(&mut self, crdt: &mut impl Crdt) -> Result<(), Report<GroupError>> {
        loop {
            let update = crdt.flush_update().change_context(GroupError)?;

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
                .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))??;
        }
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

    /// Snapshot the caller's CRDT and send it to the actor for encryption,
    /// distribution to acceptors, and `CompactionComplete` consensus.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if snapshotting, encryption, or consensus fails.
    pub async fn compact(&mut self, crdt: &impl Crdt, level: u8) -> Result<(), Report<GroupError>> {
        let snapshot = crdt.wire_snapshot().change_context(GroupError)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(GroupRequest::CompactSnapshot {
                snapshot,
                level,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor closed"))?;

        reply_rx
            .await
            .map_err(|_| Report::new(GroupError).attach("group actor dropped reply"))?
    }

    /// Proposes adding an acceptor via consensus, then registers the group with it.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError`] if the group actor is closed or consensus fails.
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

    /// # Errors
    ///
    /// Returns [`GroupError`] if the group actor is closed or consensus fails.
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

    /// Informational only; not required to consume.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<GroupEvent> {
        self.event_tx.subscribe()
    }

    /// # Errors
    ///
    /// Returns [`GroupError`] if the group actor is closed.
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

    #[must_use]
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    #[must_use]
    pub fn my_client_id(&self) -> u64 {
        self.my_client_id
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
pub(crate) async fn wait_for_welcome(endpoint: &Endpoint) -> Result<Vec<u8>, Report<GroupError>> {
    use futures::StreamExt;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| Report::new(GroupError).attach("endpoint closed"))?;

    let conn = incoming
        .accept()
        .change_context(GroupError)?
        .await
        .change_context(GroupError)?;

    let (_send, recv) = conn.accept_bi().await.change_context(GroupError)?;
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

use universal_sync_core::OperationContext;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GroupEvent>();
    }
}
