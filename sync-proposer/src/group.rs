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

enum GroupRequest<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    GetContext { reply: oneshot::Sender<GroupContext> },
    AddMember {
        key_package: Box<MlsMessage>,
        member_addr: EndpointAddr,
        crdt_snapshot: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
    UpdateKeys { reply: oneshot::Sender<Result<(), Report<GroupError>>> },
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
    Shutdown,
    #[allow(dead_code)]
    _Marker(std::marker::PhantomData<(C, CS)>),
}

#[allow(clippy::large_enum_variant)]
enum AcceptorInbound {
    ProposalResponse { acceptor_id: AcceptorId, response: ProposalResponse },
    EncryptedMessage { msg: EncryptedAppMessage },
    Disconnected { acceptor_id: AcceptorId },
}

struct PendingMessage {
    msg: EncryptedAppMessage,
    attempts: u32,
}

const MAX_MESSAGE_ATTEMPTS: u32 = 10;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum AcceptorOutbound {
    ProposalRequest { request: ProposalRequest },
    AppMessage { msg: EncryptedAppMessage },
}

/// Informational events emitted by a Group. Applications don't need to handle these.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum GroupEvent {
    MemberAdded { index: u32 },
    MemberRemoved { index: u32 },
    AcceptorAdded { id: AcceptorId },
    AcceptorRemoved { id: AcceptorId },
    ReInitiated,
    ExternalInit,
    ExtensionsUpdated,
    EpochAdvanced { epoch: u64 },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub index: u32,
    pub identity: Vec<u8>,
    pub is_self: bool,
}

#[derive(Debug, Clone)]
pub struct GroupContext {
    pub group_id: GroupId,
    pub epoch: Epoch,
    pub member_count: usize,
    pub members: Vec<MemberInfo>,
    pub acceptors: Vec<AcceptorId>,
    pub confirmed_transcript_hash: Vec<u8>,
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
    cancel_token: CancellationToken,
    actor_handle: Option<JoinHandle<()>>,
    group_id: GroupId,
    crdt: Box<dyn Crdt>,
}

impl<C, CS> Drop for Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
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
        crdt_factory: &dyn CrdtFactory,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use crate::connector::register_group_with_addr;

        let crdt_type_id = crdt_factory.type_id().to_owned();

        let (learner, group_id, group_info_bytes) = blocking(|| {
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

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);
            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

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

        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

        let crdt = crdt_factory.create();

        Ok(Self::spawn_actors(
            learner,
            group_id,
            endpoint.clone(),
            connection_manager.clone(),
            crdt,
        ))
    }

    #[allow(clippy::unused_async)]
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

        let (learner, group_id, crdt_type_id, crdt_snapshot) = blocking(|| {
            let welcome = MlsMessage::from_bytes(welcome_bytes).map_err(|e| {
                Report::new(GroupError)
                    .attach(OperationContext::JOINING_GROUP)
                    .attach(format!("invalid welcome message: {e:?}"))
            })?;

            let (group, info) = client.join_group(None, &welcome, None).map_err(|e| {
                Report::new(GroupError)
                    .attach(OperationContext::JOINING_GROUP)
                    .attach(format!("MLS join failed: {e:?}"))
            })?;

            let mls_group_id = group.context().group_id.clone();
            let group_id = GroupId::from_slice(&mls_group_id);

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

            let learner = GroupLearner::new(group, signer, cipher_suite, acceptors);

            Ok::<_, Report<GroupError>>((learner, group_id, crdt_type_id, crdt_snapshot))
        })?;

        let crdt_factory = crdt_factories.get(&crdt_type_id).ok_or_else(|| {
            Report::new(GroupError).attach(format!(
                "CRDT type '{crdt_type_id}' not registered. Register a factory with register_crdt_factory()"
            ))
        })?;

        for (id, addr) in learner.acceptors() {
            connection_manager.add_address_hint(*id, addr.clone()).await;
        }

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

    /// Add a member. Welcome is sent directly via the key package's `MemberAddrExt`.
    /// Blocks until consensus is reached.
    pub async fn add_member(&mut self, key_package: MlsMessage) -> Result<(), Report<GroupError>> {
        use universal_sync_core::MemberAddrExt;

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

    /// Blocks until consensus is reached.
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

    /// Blocks until consensus is reached.
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

    #[must_use]
    pub fn crdt(&self) -> &dyn Crdt {
        &*self.crdt
    }

    /// After mutating, call [`send_update`](Self::send_update) to broadcast.
    pub fn crdt_mut(&mut self) -> &mut dyn Crdt {
        &mut *self.crdt
    }

    /// Flush local CRDT changes and broadcast. No-op if no pending changes.
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

    /// Non-blocking: drain and apply pending CRDT updates. Returns `true` if any applied.
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

    /// Blocks until a remote update arrives. Returns `None` if shutting down.
    pub async fn wait_for_update(&mut self) -> Option<()> {
        let data = self.app_message_rx.recv().await?;
        if let Err(e) = self.crdt.apply(&data) {
            tracing::warn!(?e, "failed to apply CRDT update");
        }
        self.sync();
        Some(())
    }

    /// Proposes adding an acceptor via consensus, then registers the group with it.
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

    /// Unlike `Drop`, this waits for actors to complete.
    pub async fn shutdown(mut self) {
        let _ = self.request_tx.send(GroupRequest::Shutdown).await;
        self.cancel_token.cancel();
        if let Some(handle) = self.actor_handle.take() {
            let _ = handle.await;
        }
    }
}

struct GroupActor<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    learner: GroupLearner<C, CS>,
    proposer: Proposer<GroupLearner<C, CS>>,
    quorum_tracker: QuorumTracker<GroupLearner<C, CS>>,
    attempt: universal_sync_core::Attempt,
    group_id: GroupId,
    #[allow(dead_code)]
    endpoint: Endpoint,
    connection_manager: ConnectionManager,
    request_rx: mpsc::Receiver<GroupRequest<C, CS>>,
    app_message_tx: mpsc::Sender<Vec<u8>>,
    event_tx: broadcast::Sender<GroupEvent>,
    cancel_token: CancellationToken,
    acceptor_txs: HashMap<AcceptorId, mpsc::Sender<AcceptorOutbound>>,
    acceptor_rx: mpsc::Receiver<AcceptorInbound>,
    acceptor_inbound_tx: mpsc::Sender<AcceptorInbound>,
    acceptor_handles: HashMap<AcceptorId, JoinHandle<()>>,
    /// Resets on epoch change.
    message_index: u32,
    message_epoch: Epoch,
    seen_messages: HashSet<(MemberId, Epoch, u32)>,
    /// Buffered until we advance to the right epoch.
    pending_messages: Vec<PendingMessage>,
    /// Messages from earlier epochs cannot be decrypted.
    join_epoch: Epoch,
    active_proposal: Option<ActiveProposal>,
}

struct ActiveProposal {
    #[allow(dead_code)]
    proposal: GroupProposal,
    message: GroupMessage,
    reply_kind: ProposalReplyKind,
    started_at: std::time::Instant,
    retries: u32,
}

enum ProposalReplyKind {
    Simple(oneshot::Sender<Result<(), Report<GroupError>>>),
    WithWelcome {
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    },
}

struct WelcomeToSend {
    member_addr: EndpointAddr,
    welcome: Vec<u8>,
    reply: oneshot::Sender<Result<(), Report<GroupError>>>,
}

const MAX_PROPOSAL_RETRIES: u32 = 3;
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
        let join_epoch = message_epoch;
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

    async fn run(mut self) {
        self.spawn_acceptor_actors().await;

        let mut timeout_check = tokio::time::interval(std::time::Duration::from_secs(1));
        timeout_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                () = self.cancel_token.cancelled() => {
                    tracing::debug!("group actor cancelled");
                    break;
                }

                Some(request) = self.request_rx.recv() => {
                    if self.handle_request(request).await {
                        break;
                    }
                }

                Some(inbound) = self.acceptor_rx.recv() => {
                    self.handle_acceptor_inbound(inbound).await;
                }

                _ = timeout_check.tick() => {
                    self.check_proposal_timeout();
                }
            }
        }

        for (_, handle) in self.acceptor_handles.drain() {
            handle.abort();
        }
    }

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

    async fn spawn_acceptor_actors(&mut self) {
        let acceptors: Vec<_> = self.learner.acceptors().clone().into_iter().collect();
        for (acceptor_id, addr) in acceptors {
            self.spawn_acceptor_actor(acceptor_id, addr).await;
        }
    }

    async fn spawn_acceptor_actor(&mut self, acceptor_id: AcceptorId, addr: EndpointAddr) {
        self.spawn_acceptor_actor_inner(acceptor_id, addr, false)
            .await;
    }

    async fn spawn_acceptor_actor_with_registration(
        &mut self,
        acceptor_id: AcceptorId,
        addr: EndpointAddr,
    ) {
        self.spawn_acceptor_actor_inner(acceptor_id, addr, true)
            .await;
    }

    #[allow(clippy::collapsible_if)]
    async fn spawn_acceptor_actor_inner(
        &mut self,
        acceptor_id: AcceptorId,
        addr: EndpointAddr,
        register: bool,
    ) {
        self.connection_manager
            .add_address_hint(acceptor_id, addr.clone())
            .await;

        if register {
            if let Err(e) = self.register_group_with_acceptor(&addr).await {
                tracing::warn!(
                    ?acceptor_id,
                    ?e,
                    "failed to register group with new acceptor"
                );
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

    async fn register_group_with_acceptor(
        &self,
        addr: &EndpointAddr,
    ) -> Result<(), Report<GroupError>> {
        use crate::connector::register_group_with_addr;

        let group_info_bytes = blocking(|| {
            let group_info =
                self.learner.group().group_info_message(true).map_err(|e| {
                    Report::new(GroupError).attach(format!("group info failed: {e:?}"))
                })?;
            group_info.to_bytes().change_context(GroupError)
        })?;

        register_group_with_addr(
            self.connection_manager.endpoint(),
            addr.clone(),
            &group_info_bytes,
        )
        .await
        .change_context(GroupError)?;

        Ok(())
    }

    /// Returns true if shutdown was requested.
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

    fn get_context(&self) -> GroupContext {
        let mls_context = self.learner.group().context();
        let my_index = self.learner.group().current_member_index();
        let members = self
            .learner
            .group()
            .roster()
            .members()
            .iter()
            .map(|m| MemberInfo {
                index: m.index,
                identity: m
                    .signing_identity
                    .credential
                    .as_basic()
                    .map(|b| b.identifier.clone())
                    .unwrap_or_default(),
                is_self: m.index == my_index,
            })
            .collect::<Vec<_>>();
        let member_count = members.len();
        GroupContext {
            group_id: self.group_id,
            epoch: self.learner.mls_epoch(),
            member_count,
            members,
            acceptors: self.learner.acceptor_ids().collect(),
            confirmed_transcript_hash: mls_context.confirmed_transcript_hash.to_vec(),
        }
    }

    async fn handle_add_member(
        &mut self,
        key_package: MlsMessage,
        member_addr: EndpointAddr,
        crdt_snapshot: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let acceptor_count = self.learner.acceptor_ids().len();
        if acceptor_count == 0 {
            let _ = reply.send(Err(Report::new(GroupError).attach(
                "cannot add members to a group without acceptors: add an acceptor first",
            )));
            return;
        }

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

    fn handle_send_message(
        &mut self,
        data: &[u8],
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let current_epoch = self.learner.mls_epoch();
        if current_epoch != self.message_epoch {
            self.message_index = 0;
            self.message_epoch = current_epoch;
        }

        let sender = MemberId(self.learner.group().current_member_index());
        let index = self.message_index;
        self.message_index = self.message_index.wrapping_add(1);

        let authenticated_data = index.to_be_bytes().to_vec();

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

        for acceptor_id in selected_acceptors {
            if let Some(tx) = self.acceptor_txs.get(&acceptor_id) {
                let _ = tx.try_send(AcceptorOutbound::AppMessage {
                    msg: encrypted_msg.clone(),
                });
            }
        }

        let _ = reply.send(Ok(()));
    }

    /// Persistent extensions only, excluding transient `AcceptorAdd`/`AcceptorRemove`.
    fn persistent_context_extensions(&self) -> mls_rs::ExtensionList {
        let current = &self.learner.group().context().extensions;
        let mut persistent = mls_rs::ExtensionList::default();
        if let Ok(Some(crdt_reg)) = current.get_as::<CrdtRegistrationExt>() {
            let _ = persistent.set_from(crdt_reg);
        }
        persistent
    }

    async fn handle_add_acceptor(
        &mut self,
        addr: EndpointAddr,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        use universal_sync_core::AcceptorAdd;

        let result = blocking(|| {
            let add_ext = AcceptorAdd::new(addr.clone());
            let mut extensions = self.persistent_context_extensions();
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
                self.start_proposal(message, reply).await;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    async fn handle_remove_acceptor(
        &mut self,
        acceptor_id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        use universal_sync_core::AcceptorRemove;

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
            let mut extensions = self.persistent_context_extensions();
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
                    retries: 0,
                });

                for (acceptor_id, request) in messages {
                    self.send_proposal_request(acceptor_id, request);
                }
            }
            ProposeResult::Rejected { superseded_by } => {
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                tracing::debug!(?self.attempt, "proposal rejected, retrying");
                let () = self.retry_proposal_simple(message, reply, 0).await;
            }
        }
    }

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
                self.attempt = GroupProposal::next_attempt(superseded_by.attempt());
                self.learner.clear_pending_commit();
                tracing::debug!(?self.attempt, "proposal rejected, retrying");
                let () = self
                    .retry_proposal_with_welcome(message, member_addr, welcome, reply, 0)
                    .await;
            }
        }
    }

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
                tokio::time::sleep(std::time::Duration::from_millis(
                    10 * (u64::from(retries) + 1),
                ))
                .await;
                Box::pin(self.retry_proposal_simple(message, reply, retries + 1)).await;
            }
        }
    }

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

    fn complete_proposal_error(active: ActiveProposal, message: &'static str) {
        let reply = match active.reply_kind {
            ProposalReplyKind::Simple(reply) | ProposalReplyKind::WithWelcome { reply, .. } => {
                reply
            }
        };
        let _ = reply.send(Err(Report::new(GroupError).attach(message)));
    }

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

    async fn send_welcome_to_member(
        &self,
        member_addr: EndpointAddr,
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<GroupError>>>,
    ) {
        let result = self.send_welcome_inner(&member_addr, &welcome).await;
        let _ = reply.send(result);
    }

    async fn send_welcome_inner(
        &self,
        member_addr: &EndpointAddr,
        welcome: &[u8],
    ) -> Result<(), Report<GroupError>> {
        use futures::SinkExt;
        use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

        tracing::debug!(?member_addr, "sending welcome to new member");

        let conn = self
            .endpoint
            .connect(member_addr.clone(), PAXOS_ALPN)
            .await
            .map_err(|e| {
                Report::new(GroupError).attach(format!("failed to connect to new member: {e:?}"))
            })?;

        let (send, _recv) = conn.open_bi().await.map_err(|e| {
            Report::new(GroupError).attach(format!("failed to open stream to new member: {e:?}"))
        })?;

        let mut framed = FramedWrite::new(send, LengthDelimitedCodec::new());

        let handshake = Handshake::SendWelcome(welcome.to_vec());
        let handshake_bytes = postcard::to_stdvec(&handshake).map_err(|e| {
            Report::new(GroupError).attach(format!("failed to serialize handshake: {e:?}"))
        })?;

        framed.send(handshake_bytes.into()).await.map_err(|e| {
            Report::new(GroupError).attach(format!("failed to send welcome: {e:?}"))
        })?;

        let mut send = framed.into_inner();
        send.finish().change_context(GroupError)?;
        send.stopped().await.change_context(GroupError)?;

        tracing::debug!("welcome sent successfully");

        Ok(())
    }

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

    async fn handle_proposal_response(
        &mut self,
        acceptor_id: AcceptorId,
        response: ProposalResponse,
    ) {
        let acceptor_msg: AcceptorMessage<GroupLearner<C, CS>> = AcceptorMessage {
            promised: response.promised,
            accepted: response.accepted,
        };

        if let Some((proposal, message)) = acceptor_msg.accepted.clone() {
            self.quorum_tracker.track(proposal, message);

            let current_epoch = self.learner.mls_epoch();
            if let Some((learned_p, learned_m)) = self.quorum_tracker.check_quorum(current_epoch) {
                let learned_p = learned_p.clone();
                let learned_m = learned_m.clone();

                let my_id = self.learner.node_id();
                let is_ours = learned_p.member_id == my_id;

                self.apply_proposal(&learned_p, learned_m).await;

                if is_ours {
                    if let Some(active) = self.active_proposal.take() {
                        self.complete_proposal_success(active).await;
                    }
                } else {
                    if let Some(active) = self.active_proposal.take() {
                        self.learner.clear_pending_commit();
                        Self::complete_proposal_error(active, "another proposal won");
                    }
                }

                return;
            }
        }

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

                    if let Some(active) = self.active_proposal.take() {
                        let retries = active.retries + 1;
                        if retries > MAX_PROPOSAL_RETRIES {
                            tracing::warn!(retries, "max proposal retries exceeded");
                            Self::complete_proposal_error(active, "max proposal retries exceeded");
                        } else {
                            tracing::debug!(retries, "proposal rejected, retrying");
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
                    started_at: active.started_at,
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

    fn handle_encrypted_message(&mut self, msg: EncryptedAppMessage) {
        use mls_rs::MlsMessage;

        let mls_message = match MlsMessage::from_bytes(&msg.ciphertext) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(?e, "failed to deserialize MLS message");
                return;
            }
        };

        let received = blocking(|| {
            self.learner
                .group_mut()
                .process_incoming_message(mls_message)
        });

        let received = match received {
            Ok(msg) => msg,
            Err(e) => {
                tracing::debug!(?e, "failed to process message, buffering for retry");
                self.pending_messages
                    .push(PendingMessage { msg, attempts: 1 });
                return;
            }
        };

        self.handle_decrypted_message(received);
    }

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
            let _ = self.event_tx.send(GroupEvent::EpochAdvanced {
                epoch: self.learner.mls_epoch().0,
            });

            for (acceptor_id, addr) in new_acceptors {
                self.spawn_acceptor_actor_with_registration(acceptor_id, addr)
                    .await;
            }

            let num_acceptors = self.learner.acceptor_ids().count();
            self.quorum_tracker = QuorumTracker::new(num_acceptors);
        }

        self.attempt = universal_sync_core::Attempt::default();
        self.try_process_pending_messages();
    }

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
                        new_acceptors.push((id, addr));
                        GroupEvent::AcceptorAdded { id }
                    } else if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                        let id = remove.acceptor_id();
                        self.learner.remove_acceptor_id(&id);
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

    fn try_process_pending_messages(&mut self) {
        use mls_rs::MlsMessage;

        let pending = std::mem::take(&mut self.pending_messages);
        let current_epoch = self.learner.mls_epoch();
        let mut still_pending = Vec::new();

        for mut pending_msg in pending {
            if pending_msg.attempts >= MAX_MESSAGE_ATTEMPTS {
                tracing::debug!(
                    attempts = pending_msg.attempts,
                    "dropping message after max retry attempts"
                );
                continue;
            }

            let Ok(mls_message) = MlsMessage::from_bytes(&pending_msg.msg.ciphertext) else {
                tracing::debug!("dropping malformed pending message");
                continue;
            };

            let result = blocking(|| {
                self.learner
                    .group_mut()
                    .process_incoming_message(mls_message)
            });

            match result {
                Ok(received) => {
                    self.handle_decrypted_message(received);
                }
                Err(e) => {
                    pending_msg.attempts += 1;

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

    fn handle_decrypted_message(&mut self, received: ReceivedMessage) {
        let ReceivedMessage::ApplicationMessage(app_msg) = received else {
            tracing::debug!("received non-application message on message stream");
            return;
        };

        let index = if app_msg.authenticated_data.len() >= 4 {
            u32::from_be_bytes(app_msg.authenticated_data[..4].try_into().unwrap_or([0; 4]))
        } else {
            0
        };

        let sender = MemberId(app_msg.sender_index);
        let epoch = self.learner.mls_epoch();

        let key = (sender, epoch, index);
        if self.seen_messages.contains(&key) {
            return;
        }
        self.seen_messages.insert(key);

        let _ = self.app_message_tx.try_send(app_msg.data().to_vec());
    }
}

struct AcceptorActor {
    acceptor_id: AcceptorId,
    group_id: GroupId,
    connection_manager: ConnectionManager,
    outbound_rx: mpsc::Receiver<AcceptorOutbound>,
    inbound_tx: mpsc::Sender<AcceptorInbound>,
    cancel_token: CancellationToken,
}

const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(10);

impl AcceptorActor {
    async fn run(mut self) {
        let mut reconnect_attempts = 0u32;
        let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

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

                    tokio::select! {
                        () = tokio::time::sleep(reconnect_delay) => {}
                        () = self.cancel_token.cancelled() => break,
                    }

                    reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                }
            }
        }

        let _ = self
            .inbound_tx
            .send(AcceptorInbound::Disconnected {
                acceptor_id: self.acceptor_id,
            })
            .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn run_connection(&mut self) -> ConnectionResult {
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
        use universal_sync_core::{MessageRequest, MessageResponse};

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
                        message_reader_opt = None;
                        message_writer_opt = None;
                    }
                }
            }
        }
    }
}

enum ConnectionResult {
    Cancelled,
    Disconnected,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GroupEvent>();
    }
}
