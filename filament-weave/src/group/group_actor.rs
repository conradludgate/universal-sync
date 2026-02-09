use std::collections::{BTreeSet, HashMap, HashSet};

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, AuthData, COMPACTION_BASE, EncryptedAppMessage, Epoch, GroupId, GroupMessage,
    GroupProposal, Handshake, MemberFingerprint, MemberId, MessageId, PAXOS_ALPN, StateVector,
    SyncProposal,
};
use filament_warp::proposer::{ProposeResult, Proposer, QuorumTracker};
use filament_warp::{AcceptorMessage, Learner, Proposal};
use iroh::{Endpoint, PublicKey};
use mls_rs::client_builder::MlsConfig;
use mls_rs::group::proposal::{MlsCustomProposal, Proposal as MlsProposal};
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, MlsMessage};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::acceptor_actor::AcceptorActor;
use super::{
    AcceptorInbound, AcceptorOutbound, GroupRequest, MAX_MESSAGE_ATTEMPTS, MemberInfo,
    PendingMessage, WeaverContext, WeaverError, WeaverEvent, blocking, group_info_ext_list,
    group_info_with_ext,
};
use crate::connection::ConnectionManager;
use crate::connector::{ProposalRequest, ProposalResponse};
use crate::learner::{WeaverLearner, fingerprint_of_member};

pub(crate) struct GroupActor<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    learner: WeaverLearner<C, CS>,
    proposer: Proposer<WeaverLearner<C, CS>>,
    quorum_tracker: QuorumTracker<WeaverLearner<C, CS>>,
    attempt: filament_core::Attempt,
    group_id: GroupId,
    #[allow(dead_code)]
    endpoint: Endpoint,
    connection_manager: ConnectionManager,
    request_rx: mpsc::Receiver<GroupRequest>,
    app_message_tx: mpsc::Sender<Vec<u8>>,
    event_tx: broadcast::Sender<WeaverEvent>,
    cancel_token: CancellationToken,
    acceptor_txs: HashMap<AcceptorId, mpsc::Sender<AcceptorOutbound>>,
    acceptor_rx: mpsc::Receiver<AcceptorInbound>,
    acceptor_inbound_tx: mpsc::Sender<AcceptorInbound>,
    acceptor_handles: HashMap<AcceptorId, JoinHandle<()>>,
    connected_acceptors: BTreeSet<AcceptorId>,
    own_fingerprint: MemberFingerprint,
    message_seq: u64,
    state_vector: StateVector,
    seen_messages: HashSet<(MemberFingerprint, u64)>,
    pending_messages: Vec<PendingMessage>,
    join_epoch: Epoch,
    active_proposal: Option<ActiveProposal>,
    compaction_state: CompactionState,
    last_epoch_advance: std::time::Instant,
    key_rotation_interval: Option<std::time::Duration>,
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
    Simple(oneshot::Sender<Result<(), Report<WeaverError>>>),
    WithWelcome {
        member_endpoint_id: [u8; 32],
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    },
}

struct WelcomeToSend {
    member_endpoint_id: [u8; 32],
    welcome: Vec<u8>,
    reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
}

/// Tracks an active compaction claim at a given level.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ActiveCompactionClaim {
    /// Who claimed the compaction.
    claimer: MemberFingerprint,
    /// The level being compacted.
    level: u8,
    /// The watermark (state vector) at the time of the claim.
    watermark: StateVector,
    /// When the claim expires (unix seconds).
    deadline: u64,
    /// Whether this is our own claim.
    is_ours: bool,
}

/// Tracks compaction state for the group.
#[derive(Debug, Default)]
struct CompactionState {
    /// Active claim per level (only one compaction at a time per level).
    active_claims: HashMap<u8, ActiveCompactionClaim>,
    /// Per-level counters: `counts[N]` is the number of entries produced at
    /// level N-1 since the last compaction *into* level N. Index 0 is unused.
    counts: Vec<u64>,
    /// The watermark that was last compacted (messages at or below this are compacted).
    last_compacted_watermark: StateVector,
    /// The highest level that has been compacted into.
    max_compacted_level: u8,
}

impl CompactionState {
    fn new() -> Self {
        Self {
            active_claims: HashMap::new(),
            counts: vec![0, 0],
            last_compacted_watermark: StateVector::new(),
            max_compacted_level: 0,
        }
    }

    /// Returns true if there is an active, non-expired claim at the given level.
    fn has_active_claim(&self, level: u8) -> bool {
        self.active_claims.get(&level).is_some_and(|claim| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now < claim.deadline
        })
    }

    /// Remove expired claims.
    #[allow(dead_code)]
    fn prune_expired_claims(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.active_claims.retain(|_, claim| now < claim.deadline);
    }

    /// Record that one entry was produced at `level` (L0 message sent, or
    /// compaction completed into level N). Increments `counts[level + 1]`.
    fn record_entry(&mut self, level: usize) {
        let next = level + 1;
        while self.counts.len() <= next {
            self.counts.push(0);
        }
        self.counts[next] += 1;
        if level > 0 {
            self.max_compacted_level = self
                .max_compacted_level
                .max(u8::try_from(level).unwrap_or(u8::MAX));
        }
    }

    /// Determine the highest compaction level that would be triggered by a
    /// cascade, starting from L1. When L(N) triggers, it would produce one
    /// entry at L(N), so `counts[N+1]` would increment — if that also meets
    /// the threshold, we cascade further.
    ///
    /// Returns `None` if no level triggers.
    fn cascade_target(&self) -> Option<u8> {
        let threshold = u64::from(COMPACTION_BASE);
        let mut target: Option<usize> = None;

        for level in 1..self.counts.len() {
            let count = if target.is_some() {
                self.counts[level] + 1
            } else {
                self.counts[level]
            };

            if count >= threshold {
                target = Some(level);
            } else if target.is_some() {
                break;
            }
        }

        target.map(|l| u8::try_from(l).expect("compaction levels fit in u8"))
    }

    /// Reset counters for all levels up to and including `level`.
    fn reset_counts_up_to(&mut self, level: u8) {
        for i in 1..=usize::from(level) {
            if i < self.counts.len() {
                self.counts[i] = 0;
            }
        }
    }
}

const MAX_PROPOSAL_RETRIES: u32 = 6;
const PROPOSAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Default deadline for compaction claims (seconds from now).
// Deadline for optimistic compaction claims (kept for future use)
#[allow(dead_code)]
const COMPACTION_DEADLINE_SECS: u64 = 120;

fn exponential_backoff_duration(retries: u32) -> std::time::Duration {
    const BASE_MS: u64 = 10;
    const MAX_EXPONENT: u32 = 6;
    const MAX_DELAY_MS: u64 = 1000;
    let delay_ms = BASE_MS.saturating_mul(1u64 << retries.min(MAX_EXPONENT));
    let jitter = rand::random::<u64>() % (delay_ms / 2 + 1);
    std::time::Duration::from_millis((delay_ms + jitter).min(MAX_DELAY_MS))
}

impl<C, CS> GroupActor<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        learner: WeaverLearner<C, CS>,
        group_id: GroupId,
        endpoint: Endpoint,
        connection_manager: ConnectionManager,
        request_rx: mpsc::Receiver<GroupRequest>,
        app_message_tx: mpsc::Sender<Vec<u8>>,
        event_tx: broadcast::Sender<WeaverEvent>,
        cancel_token: CancellationToken,
        key_rotation_interval: Option<std::time::Duration>,
    ) -> Self {
        let num_acceptors = learner.acceptor_ids().count();
        let join_epoch = learner.mls_epoch();
        let own_fingerprint = learner.own_fingerprint();
        let (acceptor_inbound_tx, acceptor_rx) = mpsc::channel(256);

        Self {
            learner,
            proposer: Proposer::new(),
            quorum_tracker: QuorumTracker::new(num_acceptors),
            attempt: filament_core::Attempt::default(),
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
            connected_acceptors: BTreeSet::new(),
            own_fingerprint,
            message_seq: 0,
            state_vector: StateVector::new(),
            seen_messages: HashSet::new(),
            pending_messages: Vec::new(),
            join_epoch,
            active_proposal: None,
            compaction_state: CompactionState::new(),
            last_epoch_advance: std::time::Instant::now(),
            key_rotation_interval,
        }
    }

    pub(crate) async fn run(mut self) {
        self.spawn_acceptor_actors().await;

        let mut timeout_check = tokio::time::interval(std::time::Duration::from_secs(1));
        timeout_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let key_rotation_check_interval = std::time::Duration::from_secs(
            self.key_rotation_interval
                .map_or(3600, |d| d.as_secs().max(1)),
        );
        let mut key_rotation_check = tokio::time::interval(key_rotation_check_interval);
        key_rotation_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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

                _ = key_rotation_check.tick(), if self.key_rotation_interval.is_some() => {
                    self.maybe_trigger_key_rotation().await;
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
        for acceptor_id in acceptors {
            self.spawn_acceptor_actor(acceptor_id).await;
        }
    }

    async fn spawn_acceptor_actor(&mut self, acceptor_id: AcceptorId) {
        self.spawn_acceptor_actor_inner(acceptor_id, false).await;
    }

    async fn spawn_acceptor_actor_with_registration(&mut self, acceptor_id: AcceptorId) {
        self.spawn_acceptor_actor_inner(acceptor_id, true).await;
    }

    #[allow(clippy::collapsible_if)]
    async fn spawn_acceptor_actor_inner(&mut self, acceptor_id: AcceptorId, register: bool) {
        if register {
            if let Err(e) = self.register_group_with_acceptor(&acceptor_id).await {
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
            current_epoch: self.learner.mls_epoch(),
            own_fingerprint: self.own_fingerprint,
            connection_manager: self.connection_manager.clone(),
            outbound_rx,
            inbound_tx: self.acceptor_inbound_tx.clone(),
            cancel_token: self.cancel_token.clone(),
            protocol_version: self
                .learner
                .protocol_version()
                .expect("GroupContextExt must be present"),
        };

        let handle = tokio::spawn(actor.run());
        self.acceptor_handles.insert(acceptor_id, handle);
    }

    async fn register_group_with_acceptor(
        &self,
        acceptor_id: &AcceptorId,
    ) -> Result<(), Report<WeaverError>> {
        use crate::connector::register_group;

        let acceptors = self.learner.acceptors().iter().copied();
        let group_info_bytes = blocking(|| group_info_with_ext(self.learner.group(), acceptors))?;

        register_group(
            self.connection_manager.endpoint(),
            acceptor_id,
            &group_info_bytes,
        )
        .await
        .change_context(WeaverError)?;

        Ok(())
    }

    /// Returns true if shutdown was requested.
    async fn handle_request(&mut self, request: GroupRequest) -> bool {
        match request {
            GroupRequest::GetContext { reply } => {
                let context = self.get_context();
                let _ = reply.send(context);
            }
            GroupRequest::AddMember {
                key_package,
                member_endpoint_id,
                reply,
            } => {
                self.handle_add_member(*key_package, member_endpoint_id, reply)
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
            GroupRequest::AddAcceptor { id, reply } => {
                self.handle_add_acceptor(id, reply).await;
            }
            GroupRequest::RemoveAcceptor { acceptor_id, reply } => {
                self.handle_remove_acceptor(acceptor_id, reply).await;
            }
            GroupRequest::CompactSnapshot {
                snapshot,
                level,
                reply,
            } => {
                self.handle_compact_snapshot(snapshot, level, reply).await;
            }
            GroupRequest::GenerateExternalGroupInfo { reply } => {
                let result = blocking(|| {
                    let extensions = group_info_ext_list(self.learner.acceptors().iter().copied());
                    self.learner
                        .group()
                        .group_info_message_allowing_ext_commit_with_extensions(true, extensions)
                        .change_context(WeaverError)
                        .attach("failed to generate external commit GroupInfo")?
                        .to_bytes()
                        .change_context(WeaverError)
                });
                let _ = reply.send(result);
            }
            GroupRequest::Shutdown => {
                return true;
            }
        }
        false
    }

    fn get_context(&self) -> WeaverContext {
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
                client_id: fingerprint_of_member(&self.group_id, m).as_client_id(),
            })
            .collect::<Vec<_>>();
        let member_count = members.len();
        WeaverContext {
            group_id: self.group_id,
            epoch: self.learner.mls_epoch(),
            member_count,
            members,
            spools: self.learner.acceptor_ids().collect(),
            connected_spools: self.connected_acceptors.clone(),
            confirmed_transcript_hash: mls_context.confirmed_transcript_hash.to_vec(),
        }
    }

    async fn handle_add_member(
        &mut self,
        key_package: MlsMessage,
        member_endpoint_id: [u8; 32],
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let acceptor_count = self.learner.acceptor_ids().len();
        if acceptor_count == 0 {
            let _ = reply.send(Err(Report::new(WeaverError).attach(
                "cannot add members to a group without acceptors: add an acceptor first",
            )));
            return;
        }

        let current_version = match self.learner.protocol_version() {
            Ok(v) => v,
            Err(e) => {
                let _ = reply.send(Err(e.change_context(WeaverError)));
                return;
            }
        };
        if let Some(kp) = key_package.as_key_package()
            && let Ok(Some(kp_ext)) = kp.extensions.get_as::<filament_core::KeyPackageExt>()
            && !kp_ext.supports_version(current_version)
        {
            let _ = reply.send(Err(Report::new(WeaverError).attach(format!(
                "member does not support protocol version {current_version} (supports {:?})",
                kp_ext.supported_protocol_versions
            ))));
            return;
        }

        let result = blocking(|| {
            let group_info_ext = group_info_ext_list(self.learner.acceptors().iter().copied());

            let commit_output = self
                .learner
                .group_mut()
                .commit_builder()
                .add_member(key_package)
                .change_context(WeaverError)?
                .set_group_info_ext(group_info_ext)
                .build()
                .change_context(WeaverError)?;

            let welcome_bytes = commit_output
                .welcome_messages
                .first()
                .ok_or_else(|| Report::new(WeaverError).attach("no welcome message generated"))?
                .to_bytes()
                .change_context(WeaverError)?;

            Ok::<_, Report<WeaverError>>((commit_output, welcome_bytes))
        });

        match result {
            Ok((commit_output, welcome_bytes)) => {
                let message = GroupMessage::new(commit_output.commit_message);
                self.start_proposal_with_welcome(message, member_endpoint_id, welcome_bytes, reply)
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
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .remove_member(member_index)
                .change_context(WeaverError)?
                .build()
                .change_context(WeaverError)
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

    async fn handle_update_keys(
        &mut self,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .build()
                .change_context(WeaverError)
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
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let seq = self.message_seq;
        self.message_seq += 1;
        self.state_vector.insert(self.own_fingerprint, seq);

        let auth_data = AuthData::update(seq);
        let authenticated_data = match auth_data.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                let _ = reply.send(Err(
                    Report::new(WeaverError).attach(format!("auth data encode: {e}"))
                ));
                return;
            }
        };

        let result = blocking(|| {
            self.learner
                .encrypt_application_message(data, authenticated_data)
        });

        let mls_message = match result.change_context(WeaverError) {
            Ok(msg) => msg,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let ciphertext = match mls_message.to_bytes().change_context(WeaverError) {
            Ok(bytes) => bytes::Bytes::from(bytes),
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let encrypted_msg = EncryptedAppMessage {
            ciphertext: ciphertext.clone(),
        };

        let message_id = MessageId {
            group_id: self.group_id,
            sender: self.own_fingerprint,
            seq,
        };
        let acceptor_ids: Vec<_> = self.learner.acceptor_ids().collect();
        let count = crate::rendezvous::delivery_count(
            0,
            self.compaction_state.counts.len(),
            acceptor_ids.len(),
        );
        let selected_acceptors =
            crate::rendezvous::select_acceptors(&acceptor_ids, &message_id, count);

        for acceptor_id in selected_acceptors {
            if let Some(tx) = self.acceptor_txs.get(&acceptor_id) {
                let _ = tx.try_send(AcceptorOutbound::AppMessage {
                    id: message_id,
                    msg: encrypted_msg.clone(),
                });
            }
        }

        self.compaction_state.record_entry(0);

        if !self.acceptor_txs.is_empty()
            && let Some(level) = self.compaction_state.cascade_target()
            && !self.compaction_state.has_active_claim(level)
        {
            let _ = self.event_tx.send(WeaverEvent::CompactionNeeded {
                level,
                force: false,
            });
        }

        let _ = reply.send(Ok(()));
    }

    async fn handle_add_acceptor(
        &mut self,
        id: AcceptorId,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = blocking(|| {
            let proposal = SyncProposal::acceptor_add(id);
            let custom = proposal.to_custom_proposal().change_context(WeaverError)?;

            self.learner
                .group_mut()
                .commit_builder()
                .custom_proposal(custom)
                .build()
                .change_context(WeaverError)
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
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = blocking(|| {
            let proposal = SyncProposal::acceptor_remove(acceptor_id);
            let custom = proposal.to_custom_proposal().change_context(WeaverError)?;

            self.learner
                .group_mut()
                .commit_builder()
                .custom_proposal(custom)
                .build()
                .change_context(WeaverError)
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
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
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
        member_endpoint_id: [u8; 32],
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                self.apply_proposal(&proposal, message).await;
                self.send_welcome_to_member(member_endpoint_id, welcome, reply)
                    .await;
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::WithWelcome {
                        member_endpoint_id,
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
                    .retry_proposal_with_welcome(message, member_endpoint_id, welcome, reply, 0)
                    .await;
            }
        }
    }

    async fn retry_proposal_simple(
        &mut self,
        message: GroupMessage,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
        retries: u32,
    ) {
        if retries >= MAX_PROPOSAL_RETRIES {
            let _ = reply.send(Err(
                Report::new(WeaverError).attach("max proposal retries exceeded")
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
                tokio::time::sleep(exponential_backoff_duration(retries)).await;
                Box::pin(self.retry_proposal_simple(message, reply, retries + 1)).await;
            }
        }
    }

    async fn retry_proposal_with_welcome(
        &mut self,
        message: GroupMessage,
        member_endpoint_id: [u8; 32],
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
        retries: u32,
    ) {
        if retries >= MAX_PROPOSAL_RETRIES {
            let _ = reply.send(Err(
                Report::new(WeaverError).attach("max proposal retries exceeded")
            ));
            return;
        }

        let result = self
            .proposer
            .propose(&self.learner, self.attempt, message.clone());

        match result {
            ProposeResult::Learned { proposal, message } => {
                self.apply_proposal(&proposal, message).await;
                self.send_welcome_to_member(member_endpoint_id, welcome, reply)
                    .await;
            }
            ProposeResult::Continue(messages) => {
                let proposal = self.learner.propose(self.attempt);
                self.active_proposal = Some(ActiveProposal {
                    proposal: proposal.clone(),
                    message,
                    reply_kind: ProposalReplyKind::WithWelcome {
                        member_endpoint_id,
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
                tokio::time::sleep(exponential_backoff_duration(retries)).await;
                Box::pin(self.retry_proposal_with_welcome(
                    message,
                    member_endpoint_id,
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
                member_endpoint_id,
                welcome,
                reply,
            } => Some(WelcomeToSend {
                member_endpoint_id,
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
        let _ = reply.send(Err(Report::new(WeaverError).attach(message)));
    }

    async fn complete_proposal_success(&mut self, active: ActiveProposal) {
        if let Some(welcome_to_send) = Self::complete_proposal_success_sync(active) {
            self.send_welcome_to_member(
                welcome_to_send.member_endpoint_id,
                welcome_to_send.welcome,
                welcome_to_send.reply,
            )
            .await;
        }
    }

    async fn send_welcome_to_member(
        &self,
        member_endpoint_id: [u8; 32],
        welcome: Vec<u8>,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let result = self.send_welcome_inner(&member_endpoint_id, &welcome).await;
        let _ = reply.send(result);
    }

    async fn send_welcome_inner(
        &self,
        member_endpoint_id: &[u8; 32],
        welcome: &[u8],
    ) -> Result<(), Report<WeaverError>> {
        use futures::SinkExt;
        use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

        let public_key = PublicKey::from_bytes(member_endpoint_id)
            .map_err(|_| Report::new(WeaverError).attach("invalid member endpoint id"))?;

        tracing::debug!(%public_key, "sending welcome to new member");

        let conn = self
            .endpoint
            .connect(public_key, PAXOS_ALPN)
            .await
            .change_context(WeaverError)?;

        let (send, _recv) = conn.open_bi().await.change_context(WeaverError)?;

        let mut framed = FramedWrite::new(send, LengthDelimitedCodec::new());

        let handshake = Handshake::SendWelcome(bytes::Bytes::copy_from_slice(welcome));
        let handshake_bytes = postcard::to_stdvec(&handshake).change_context(WeaverError)?;

        framed
            .send(handshake_bytes.into())
            .await
            .change_context(WeaverError)?;

        let mut send = framed.into_inner();
        send.finish().change_context(WeaverError)?;
        send.stopped().await.change_context(WeaverError)?;

        tracing::debug!("welcome sent successfully");

        Ok(())
    }

    fn send_proposal_request(
        &self,
        acceptor_id: AcceptorId,
        request: filament_warp::AcceptorRequest<WeaverLearner<C, CS>>,
    ) {
        let wire_request = match request {
            filament_warp::AcceptorRequest::Prepare(p) => ProposalRequest::Prepare(p),
            filament_warp::AcceptorRequest::Accept(p, m) => ProposalRequest::Accept(p, m),
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
            AcceptorInbound::Connected { acceptor_id } => {
                if self.connected_acceptors.insert(acceptor_id) {
                    tracing::info!(?acceptor_id, "acceptor connected");
                    let _ = self
                        .event_tx
                        .send(WeaverEvent::SpoolConnected { id: acceptor_id });
                }
            }
            AcceptorInbound::Disconnected { acceptor_id } => {
                if self.connected_acceptors.remove(&acceptor_id) {
                    tracing::warn!(?acceptor_id, "acceptor disconnected");
                    let _ = self
                        .event_tx
                        .send(WeaverEvent::SpoolDisconnected { id: acceptor_id });
                }
            }
        }
    }

    async fn handle_proposal_response(
        &mut self,
        acceptor_id: AcceptorId,
        response: ProposalResponse,
    ) {
        let acceptor_msg: AcceptorMessage<WeaverLearner<C, CS>> = AcceptorMessage {
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
                } else if let Some(active) = self.active_proposal.take() {
                    self.learner.clear_pending_commit();
                    Self::complete_proposal_error(active, "another proposal won");
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
                            tokio::time::sleep(exponential_backoff_duration(retries)).await;
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
                    tokio::time::sleep(exponential_backoff_duration(retries)).await;
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
            let committer_index =
                (proposal.member_id.0 != u32::MAX).then_some(proposal.member_id.0);
            let (events, new_acceptors) = self.process_commit_effect(&effect, committer_index);
            for event in events {
                let _ = self.event_tx.send(event);
            }
            let _ = self.event_tx.send(WeaverEvent::EpochAdvanced {
                epoch: self.learner.mls_epoch().0,
            });
            self.last_epoch_advance = std::time::Instant::now();

            for acceptor_id in new_acceptors {
                self.spawn_acceptor_actor_with_registration(acceptor_id)
                    .await;
            }

            let num_acceptors = self.learner.acceptor_ids().count();
            self.quorum_tracker = QuorumTracker::new(num_acceptors);
        }

        self.attempt = filament_core::Attempt::default();
        self.try_process_pending_messages();
    }

    fn process_commit_effect(
        &mut self,
        effect: &CommitEffect,
        committer_index: Option<u32>,
    ) -> (Vec<WeaverEvent>, Vec<AcceptorId>) {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => {
                return (vec![WeaverEvent::ReInitiated], vec![]);
            }
        };

        let mut events = Vec::new();
        let mut new_acceptors = Vec::new();

        // Determine the committer's fingerprint for compaction claim tracking
        let committer_fingerprint = committer_index.and_then(|idx| {
            self.learner
                .group()
                .roster()
                .member_with_index(idx)
                .map(|m| crate::learner::fingerprint_of_member(&self.group_id, &m))
                .ok()
        });

        for proposal_info in applied_proposals {
            let event = match &proposal_info.proposal {
                MlsProposal::Add(add_proposal) => {
                    let _ = add_proposal;
                    let level = self.compaction_state.max_compacted_level.max(1);
                    let _ = self
                        .event_tx
                        .send(WeaverEvent::CompactionNeeded { level, force: true });
                    WeaverEvent::MemberAdded { index: 0 }
                }
                MlsProposal::Remove(remove_proposal) => WeaverEvent::MemberRemoved {
                    index: remove_proposal.to_remove(),
                },
                MlsProposal::ReInit(_) => WeaverEvent::ReInitiated,
                MlsProposal::ExternalInit(_) => {
                    let level = self.compaction_state.max_compacted_level.max(1);
                    let _ = self
                        .event_tx
                        .send(WeaverEvent::CompactionNeeded { level, force: true });
                    WeaverEvent::ExternalInit
                }
                MlsProposal::GroupContextExtensions(_) => WeaverEvent::ExtensionsUpdated,
                MlsProposal::Custom(custom) => {
                    self.process_custom_proposal(custom, committer_fingerprint, &mut new_acceptors)
                }
                _ => WeaverEvent::Unknown,
            };

            events.push(event);
        }

        (events, new_acceptors)
    }

    fn process_custom_proposal(
        &mut self,
        custom: &mls_rs::group::proposal::CustomProposal,
        committer_fingerprint: Option<MemberFingerprint>,
        new_acceptors: &mut Vec<AcceptorId>,
    ) -> WeaverEvent {
        let Ok(proposal) = SyncProposal::from_custom_proposal(custom) else {
            return WeaverEvent::Unknown;
        };

        match proposal {
            SyncProposal::AcceptorAdd(id) => {
                self.learner.add_acceptor(id);
                new_acceptors.push(id);
                WeaverEvent::SpoolAdded { id }
            }
            SyncProposal::AcceptorRemove(id) => {
                self.learner.remove_acceptor(&id);
                if let Some(handle) = self.acceptor_handles.remove(&id) {
                    handle.abort();
                }
                self.acceptor_txs.remove(&id);
                self.connected_acceptors.remove(&id);
                WeaverEvent::SpoolRemoved { id }
            }
            SyncProposal::CompactionClaim {
                level,
                watermark,
                deadline,
            } => {
                tracing::info!(
                    level,
                    deadline,
                    watermark_entries = watermark.len(),
                    "received CompactionClaim"
                );

                let is_ours = committer_fingerprint.is_some_and(|fp| fp == self.own_fingerprint);

                let active_claim = ActiveCompactionClaim {
                    claimer: committer_fingerprint.unwrap_or(self.own_fingerprint),
                    level,
                    watermark: watermark.clone(),
                    deadline,
                    is_ours,
                };

                self.compaction_state
                    .active_claims
                    .insert(level, active_claim);

                WeaverEvent::CompactionClaimed { level }
            }
            SyncProposal::CompactionComplete { level, watermark } => {
                tracing::info!(
                    level,
                    watermark_entries = watermark.len(),
                    "received CompactionComplete"
                );

                self.compaction_state.active_claims.remove(&level);

                for (fp, &seq) in &watermark {
                    let hw = self
                        .compaction_state
                        .last_compacted_watermark
                        .entry(*fp)
                        .or_insert(0);
                    if seq > *hw {
                        *hw = seq;
                    }
                }

                self.compaction_state.reset_counts_up_to(level);
                self.compaction_state.record_entry(usize::from(level));

                WeaverEvent::CompactionCompleted { level }
            }
        }
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

        let auth_data = match AuthData::from_bytes(&app_msg.authenticated_data) {
            Ok(ad) => ad,
            Err(e) => {
                tracing::warn!(?e, "failed to decode auth data, dropping message");
                return;
            }
        };

        let seq = auth_data.seq();

        let sender_member = MemberId(app_msg.sender_index);
        let sender_fp = self
            .learner
            .group()
            .roster()
            .member_with_index(sender_member.0)
            .map(|m| crate::learner::fingerprint_of_member(&self.group_id, &m))
            .ok();

        let Some(sender_fp) = sender_fp else {
            tracing::debug!(
                ?sender_member,
                "sender not found in roster, dropping message"
            );
            return;
        };

        let key = (sender_fp, seq);
        if self.seen_messages.contains(&key) {
            return;
        }
        self.seen_messages.insert(key);

        let hw = self.state_vector.entry(sender_fp).or_insert(0);
        if seq > *hw {
            *hw = seq;
        }

        match &auth_data {
            AuthData::Update { .. } => {
                self.compaction_state.record_entry(0);
            }
            AuthData::Compaction { level, .. } => {
                self.compaction_state.record_entry(usize::from(*level));
            }
        }

        let _ = self.app_message_tx.try_send(app_msg.data().to_vec());
    }

    async fn handle_compact_snapshot(
        &mut self,
        snapshot: bytes::Bytes,
        level: u8,
        reply: oneshot::Sender<Result<(), Report<WeaverError>>>,
    ) {
        let watermark = self.state_vector.clone();

        if !self.encrypt_and_send_to_acceptors(&snapshot, level, &watermark) {
            let _ =
                reply
                    .send(Err(Report::new(WeaverError)
                        .attach("failed to encrypt/send compacted snapshot")));
            return;
        }

        self.compaction_state.reset_counts_up_to(level);
        self.compaction_state.record_entry(usize::from(level));

        self.submit_compaction_complete(level, watermark).await;

        let _ = reply.send(Ok(()));
    }

    async fn maybe_trigger_key_rotation(&mut self) {
        let Some(interval) = self.key_rotation_interval else {
            return;
        };

        if self.active_proposal.is_some() {
            return;
        }

        if self.learner.acceptor_ids().len() == 0 {
            return;
        }

        if self.last_epoch_advance.elapsed() < interval {
            return;
        }

        tracing::info!(
            elapsed_secs = self.last_epoch_advance.elapsed().as_secs(),
            "triggering time-based key rotation"
        );

        let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
        self.handle_update_keys(reply_tx).await;
    }

    /// Encrypt data at the current epoch and send it to acceptors using the
    /// replication factor for the given compaction level. Returns `false` on
    /// encryption failure.
    fn encrypt_and_send_to_acceptors(
        &mut self,
        data: &[u8],
        level: u8,
        watermark: &StateVector,
    ) -> bool {
        let seq = self.message_seq;
        self.message_seq += 1;
        self.state_vector.insert(self.own_fingerprint, seq);

        let authenticated_data =
            match AuthData::compaction(seq, level, watermark.clone()).to_bytes() {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(?e, "failed to encode compaction auth data");
                    return false;
                }
            };
        let mls_message = match blocking(|| {
            self.learner
                .encrypt_application_message(data, authenticated_data)
        }) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(?e, "failed to encrypt compacted message");
                return false;
            }
        };

        let ciphertext = match mls_message.to_bytes() {
            Ok(bytes) => bytes::Bytes::from(bytes),
            Err(e) => {
                tracing::warn!(?e, "failed to serialize compacted message");
                return false;
            }
        };

        let encrypted_msg = EncryptedAppMessage { ciphertext };
        let message_id = MessageId {
            group_id: self.group_id,
            sender: self.own_fingerprint,
            seq,
        };

        let acceptor_ids: Vec<_> = self.learner.acceptor_ids().collect();
        let count = crate::rendezvous::delivery_count(
            usize::from(level),
            self.compaction_state.counts.len(),
            acceptor_ids.len(),
        );

        let selected = crate::rendezvous::select_acceptors(&acceptor_ids, &message_id, count);

        tracing::info!(
            level,
            count,
            total_acceptors = acceptor_ids.len(),
            "sending compacted message to acceptors"
        );

        for acceptor_id in selected {
            if let Some(tx) = self.acceptor_txs.get(&acceptor_id) {
                let _ = tx.try_send(AcceptorOutbound::AppMessage {
                    id: message_id,
                    msg: encrypted_msg.clone(),
                });
            }
        }

        true
    }

    /// Build and submit a `CompactionComplete` commit through Paxos consensus.
    async fn submit_compaction_complete(&mut self, level: u8, watermark: StateVector) {
        let proposal = SyncProposal::compaction_complete(level, watermark);

        let custom_proposal = match proposal.to_custom_proposal() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(?e, "failed to encode CompactionComplete proposal");
                return;
            }
        };

        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .custom_proposal(custom_proposal)
                .build()
                .change_context(WeaverError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                let (reply_tx, reply_rx) = oneshot::channel();
                self.start_proposal(message, reply_tx).await;

                tokio::spawn(async move {
                    match reply_rx.await {
                        Ok(Ok(())) => tracing::info!("compaction complete commit accepted"),
                        Ok(Err(e)) => tracing::warn!(?e, "compaction complete commit failed"),
                        Err(_) => tracing::debug!("compaction complete reply dropped"),
                    }
                });
            }
            Err(e) => {
                tracing::warn!(?e, "failed to build compaction complete commit");
            }
        }
    }

    /// Build and submit a `CompactionClaim` commit through Paxos consensus.
    /// Currently unused — kept for future optimistic locking support.
    #[allow(dead_code)]
    async fn submit_compaction_claim(&mut self, level: u8) {
        let watermark = self.state_vector.clone();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + COMPACTION_DEADLINE_SECS;

        let proposal = SyncProposal::compaction_claim(level, watermark, deadline);

        let custom_proposal = match proposal.to_custom_proposal() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(?e, "failed to encode CompactionClaim proposal");
                return;
            }
        };

        let result = blocking(|| {
            self.learner
                .group_mut()
                .commit_builder()
                .custom_proposal(custom_proposal)
                .build()
                .change_context(WeaverError)
        });

        match result {
            Ok(commit_output) => {
                let message = GroupMessage::new(commit_output.commit_message);
                let (reply_tx, reply_rx) = oneshot::channel();
                self.start_proposal(message, reply_tx).await;

                // We don't block waiting for the reply; the compaction coordination
                // is driven by observing CompactionClaimed events in process_commit_effect.
                // Spawn a background task to log the result.
                tokio::spawn(async move {
                    match reply_rx.await {
                        Ok(Ok(())) => tracing::debug!("compaction claim commit accepted"),
                        Ok(Err(e)) => tracing::warn!(?e, "compaction claim commit failed"),
                        Err(_) => tracing::debug!("compaction claim reply dropped"),
                    }
                });
            }
            Err(e) => {
                tracing::warn!(?e, "failed to build compaction claim commit");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_claim(level: u8, deadline: u64) -> ActiveCompactionClaim {
        ActiveCompactionClaim {
            claimer: MemberFingerprint([0u8; 8]),
            level,
            watermark: StateVector::default(),
            deadline,
            is_ours: false,
        }
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn compaction_state_no_active_claims() {
        let state = CompactionState::new();
        assert!(!state.has_active_claim(1));
        assert!(!state.has_active_claim(2));
    }

    #[test]
    fn compaction_state_active_claim_not_expired() {
        let mut state = CompactionState::new();
        state
            .active_claims
            .insert(1, make_claim(1, now_secs() + 60));
        assert!(state.has_active_claim(1));
        assert!(!state.has_active_claim(2));
    }

    #[test]
    fn compaction_state_expired_claim() {
        let mut state = CompactionState::new();
        state.active_claims.insert(1, make_claim(1, now_secs() - 1));
        assert!(!state.has_active_claim(1));
    }

    #[test]
    fn compaction_state_prune_expired() {
        let mut state = CompactionState::new();
        state.active_claims.insert(1, make_claim(1, now_secs() - 1));
        state
            .active_claims
            .insert(2, make_claim(2, now_secs() + 60));
        assert_eq!(state.active_claims.len(), 2);

        state.prune_expired_claims();
        assert_eq!(state.active_claims.len(), 1);
        assert!(state.active_claims.contains_key(&2));
    }

    #[test]
    fn compaction_state_multiple_levels() {
        let mut state = CompactionState::new();
        state
            .active_claims
            .insert(1, make_claim(1, now_secs() + 60));
        state
            .active_claims
            .insert(2, make_claim(2, now_secs() + 60));
        assert!(state.has_active_claim(1));
        assert!(state.has_active_claim(2));
        assert!(!state.has_active_claim(3));
    }

    #[test]
    fn compaction_state_initial_values() {
        let state = CompactionState::new();
        assert_eq!(state.counts, vec![0, 0]);
        assert!(state.last_compacted_watermark.is_empty());
        assert!(state.active_claims.is_empty());
        assert_eq!(state.max_compacted_level, 0);
    }

    #[test]
    fn compaction_state_dynamic_record_entry() {
        let mut state = CompactionState::new();
        state.record_entry(0);
        assert_eq!(state.counts, vec![0, 1]);

        state.record_entry(1);
        assert_eq!(state.counts, vec![0, 1, 1]);

        state.record_entry(5);
        assert_eq!(state.counts, vec![0, 1, 1, 0, 0, 0, 1]);
    }

    #[test]
    fn compaction_state_cascade_with_base() {
        let mut state = CompactionState::new();
        // 19 L0 messages → not enough for L1 (COMPACTION_BASE=20)
        for _ in 0..19 {
            state.record_entry(0);
        }
        assert_eq!(state.cascade_target(), None);

        // 20th L0 message → triggers L1
        state.record_entry(0);
        assert_eq!(state.cascade_target(), Some(1));
    }

    #[test]
    fn compaction_state_cascade_multi_level() {
        let mut state = CompactionState::new();
        // Simulate 19 previous L1 compactions
        state.counts = vec![0, 20, 19];
        // L1 triggers (20 >= 20), completing it bumps L2 to 20 → cascade to L2
        assert_eq!(state.cascade_target(), Some(2));
    }

    #[test]
    fn compaction_state_max_compacted_level_tracking() {
        let mut state = CompactionState::new();
        assert_eq!(state.max_compacted_level, 0);

        state.record_entry(1);
        assert_eq!(state.max_compacted_level, 1);

        state.record_entry(3);
        assert_eq!(state.max_compacted_level, 3);

        // L0 entries don't affect max_compacted_level
        state.record_entry(0);
        assert_eq!(state.max_compacted_level, 3);
    }

    #[test]
    fn compaction_state_reset_counts_up_to() {
        let mut state = CompactionState::new();
        state.counts = vec![0, 10, 4];
        state.reset_counts_up_to(2);
        assert_eq!(state.counts, vec![0, 0, 0]);
    }

    #[test]
    fn compaction_state_reset_counts_l1_only() {
        let mut state = CompactionState::new();
        state.counts = vec![0, 10, 4];
        state.reset_counts_up_to(1);
        assert_eq!(state.counts, vec![0, 0, 4]);
    }

    #[test]
    fn exponential_backoff_bounds() {
        for _ in 0..100 {
            let d0 = exponential_backoff_duration(0);
            assert!(d0.as_millis() >= 10, "retry 0 base is 10ms");
            assert!(d0.as_millis() <= 15, "retry 0 max is 10 + 5 jitter");

            let d3 = exponential_backoff_duration(3);
            assert!(d3.as_millis() >= 80);
            assert!(d3.as_millis() <= 120);

            let d6 = exponential_backoff_duration(6);
            assert!(d6.as_millis() >= 640);
            assert!(d6.as_millis() <= 1000, "capped at MAX_DELAY_MS");

            let d10 = exponential_backoff_duration(10);
            assert!(d10.as_millis() <= 1000);
        }
    }

    #[test]
    fn exponential_backoff_growth() {
        let base = |r: u32| 10u128 * (1u128 << r.min(6));
        for r in 0..6 {
            assert_eq!(base(r + 1), base(r) * 2);
        }
    }
}
