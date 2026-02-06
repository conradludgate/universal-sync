use std::collections::{HashMap, HashSet};

use error_stack::{Report, ResultExt};
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, CrdtRegistrationExt, EncryptedAppMessage, Epoch,
    GroupId, GroupMessage, GroupProposal, Handshake, MemberId, MessageId, PAXOS_ALPN,
};
use universal_sync_paxos::proposer::{ProposeResult, Proposer, QuorumTracker};
use universal_sync_paxos::{AcceptorMessage, Learner, Proposal};

use super::acceptor_actor::AcceptorActor;
use super::{
    AcceptorInbound, AcceptorOutbound, GroupContext, GroupError, GroupEvent, GroupRequest,
    MAX_MESSAGE_ATTEMPTS, MemberInfo, PendingMessage, blocking, welcome_group_info_extensions,
};
use crate::connection::ConnectionManager;
use crate::connector::{ProposalRequest, ProposalResponse};
use crate::learner::GroupLearner;

pub(crate) struct GroupActor<C, CS>
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
    message_index: u32,
    message_epoch: Epoch,
    seen_messages: HashSet<(MemberId, Epoch, u32)>,
    pending_messages: Vec<PendingMessage>,
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
    pub(crate) fn new(
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

    pub(crate) async fn run(mut self) {
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
