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
    AcceptorId, AcceptorsExt, CompactionConfig, Crdt, CrdtFactory, CrdtRegistrationExt,
    CrdtSnapshotExt, EncryptedAppMessage, Epoch, GroupId, Handshake, MessageId,
};

use crate::connection::ConnectionManager;
use crate::connector::ProposalRequest;
use crate::learner::GroupLearner;

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

/// Build GroupInfo extensions for a welcome message (acceptor addresses + CRDT snapshot).
#[must_use]
pub(crate) fn welcome_group_info_extensions(
    acceptors: impl IntoIterator<Item = EndpointAddr>,
    crdt_snapshot: Vec<u8>,
) -> mls_rs::ExtensionList {
    let mut extensions = mls_rs::ExtensionList::default();
    extensions
        .set_from(AcceptorsExt::new(acceptors))
        .expect("AcceptorsExt encoding should not fail");
    extensions
        .set_from(CrdtSnapshotExt::new(crdt_snapshot))
        .expect("CrdtSnapshotExt encoding should not fail");
    extensions
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
        crdt_snapshot: Vec<u8>,
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
    ProposalRequest { request: ProposalRequest },
    AppMessage { id: MessageId, msg: EncryptedAppMessage },
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
    CompactionClaimed { level: u8 },
    CompactionCompleted { level: u8 },
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

        let compaction_config = crdt_factory.compaction_config();
        let crdt = crdt_factory.create();

        Ok(Self::spawn_actors(
            learner,
            group_id,
            endpoint.clone(),
            connection_manager.clone(),
            crdt,
            compaction_config,
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

        let compaction_config = crdt_factory.compaction_config();
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
            compaction_config,
        ))
    }

    fn spawn_actors(
        learner: GroupLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        crdt: Box<dyn Crdt>,
        compaction_config: CompactionConfig,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(64);
        let (request_tx, request_rx) = mpsc::channel(64);
        let (app_message_tx, app_message_rx) = mpsc::channel(256);

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
