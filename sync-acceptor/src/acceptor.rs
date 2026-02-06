//! Paxos Acceptor for federated servers.
//!
//! Wraps an MLS `ExternalGroup` to validate proposals without decrypting messages.
//! Only applies values after consensus (quorum), not mere acceptance.

use error_stack::{Report, ResultExt};
use iroh::{EndpointAddr, PublicKey, SecretKey, Signature};
use mls_rs::CipherSuiteProvider;
use mls_rs::crypto::SignaturePublicKey;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalGroup, ExternalReceivedMessage};
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, Attempt, Epoch, GroupMessage, GroupProposal, MemberId,
    UnsignedProposal,
};

/// Error marker for `GroupAcceptor` operations.
#[derive(Debug, Default)]
pub struct AcceptorError;

impl std::fmt::Display for AcceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("acceptor operation failed")
    }
}

impl std::error::Error for AcceptorError {}

#[derive(Debug, Clone)]
pub(crate) enum AcceptorChangeEvent {
    Added { id: AcceptorId },
    Removed { id: AcceptorId },
}

/// Validates and orders group operations via Paxos.
///
/// Wraps an MLS `ExternalGroup` to verify signatures without decrypting messages.
/// Tracks the acceptor set via `AcceptorAdd`/`AcceptorRemove` extensions in commits.
pub struct GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    external_group: ExternalGroup<C>,
    cipher_suite: CS,
    secret_key: SecretKey,
    acceptors: std::collections::BTreeMap<AcceptorId, EndpointAddr>,
    /// Enables epoch roster lookups for validating proposals from past epochs.
    state_store: Option<crate::state_store::GroupStateStore>,
}

impl<C, CS> GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    pub(crate) fn new(
        external_group: ExternalGroup<C>,
        cipher_suite: CS,
        secret_key: SecretKey,
        acceptors: impl IntoIterator<Item = EndpointAddr>,
    ) -> Self {
        Self {
            external_group,
            cipher_suite,
            secret_key,
            acceptors: acceptors
                .into_iter()
                .map(|addr| {
                    let id = AcceptorId::from_bytes(*addr.id.as_bytes());
                    (id, addr)
                })
                .collect(),
            state_store: None,
        }
    }

    #[must_use]
    pub(crate) fn with_state_store(
        mut self,
        state_store: crate::state_store::GroupStateStore,
    ) -> Self {
        self.state_store = Some(state_store);
        self
    }

    pub(crate) fn acceptor_addrs(&self) -> impl Iterator<Item = (&AcceptorId, &EndpointAddr)> {
        self.acceptors.iter()
    }

    fn get_member_public_key(&self, member_id: MemberId) -> Option<SignaturePublicKey> {
        self.external_group
            .roster()
            .member_with_index(member_id.0)
            .map(|m| m.signing_identity.signature_key)
            .ok()
    }

    /// Looks up the signing key for a member at a specific epoch,
    /// falling back to the current roster if no historical data is available.
    fn get_member_public_key_for_epoch(
        &self,
        member_id: MemberId,
        epoch: Epoch,
    ) -> Option<SignaturePublicKey> {
        if let Some(ref store) = self.state_store
            && let Some(roster) = store.get_epoch_roster_at_or_before(epoch)
            && let Some(key_bytes) = roster.get_member_key(member_id)
        {
            return Some(SignaturePublicKey::new_slice(key_bytes));
        }

        let current_epoch = Epoch(self.external_group.group_context().epoch);
        if epoch == current_epoch {
            return self.get_member_public_key(member_id);
        }

        None
    }

    fn is_known_acceptor(&self, id: &AcceptorId) -> bool {
        self.acceptors.contains_key(id)
    }
}

impl<C, CS> universal_sync_paxos::Learner for GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    type Proposal = GroupProposal;
    type Message = GroupMessage;
    type Error = Report<AcceptorError>;
    type AcceptorId = AcceptorId;

    fn node_id(&self) -> MemberId {
        MemberId(u32::MAX)
    }

    fn current_round(&self) -> Epoch {
        Epoch(self.external_group.group_context().epoch)
    }

    fn acceptors(&self) -> impl IntoIterator<Item = AcceptorId, IntoIter: ExactSizeIterator> {
        self.acceptors.keys().copied().collect::<Vec<_>>()
    }

    fn propose(&self, attempt: Attempt) -> GroupProposal {
        let unsigned = UnsignedProposal::new(
            MemberId(u32::MAX),
            Epoch(self.external_group.group_context().epoch),
            attempt,
            *self.secret_key.public().as_bytes(),
        );
        let data = unsigned.to_bytes();
        let signature = self.secret_key.sign(&data);
        unsigned.with_signature(signature.to_bytes().to_vec())
    }

    fn validate(
        &self,
        proposal: &GroupProposal,
    ) -> Result<
        universal_sync_paxos::Validated,
        error_stack::Report<universal_sync_paxos::ValidationError>,
    > {
        use error_stack::Report;
        use universal_sync_paxos::{Validated, ValidationError};

        tracing::debug!(
            proposal = ?proposal,
            "validating proposal"
        );

        if proposal.member_id == MemberId(u32::MAX) {
            let acceptor_id = AcceptorId::from_bytes(proposal.message_hash);

            if !self.is_known_acceptor(&acceptor_id) {
                tracing::debug!(?acceptor_id, "sync proposal from unknown acceptor");
                return Err(Report::new(ValidationError)
                    .attach("sync proposal from unknown acceptor")
                    .attach(format!("acceptor_id: {acceptor_id:?}")));
            }

            let public_key = PublicKey::from_bytes(&proposal.message_hash).map_err(|_| {
                tracing::debug!("invalid public key in message_hash");
                Report::new(ValidationError).attach("invalid public key in message_hash")
            })?;

            let sig_bytes: [u8; Signature::LENGTH] =
                proposal.signature.as_slice().try_into().map_err(|_| {
                    tracing::debug!("invalid signature length");
                    Report::new(ValidationError).attach({
                        format!(
                            "invalid signature length: expected {}, got {}",
                            Signature::LENGTH,
                            proposal.signature.len()
                        )
                    })
                })?;
            let signature = Signature::from_bytes(&sig_bytes);

            let data = proposal.unsigned().to_bytes();
            public_key.verify(&data, &signature).map_err(|_| {
                tracing::debug!("sync proposal signature verification failed");
                Report::new(ValidationError).attach("sync proposal signature verification failed")
            })?;

            tracing::debug!(?acceptor_id, "accepting sync proposal from known acceptor");
            return Ok(Validated::assert_valid());
        }

        let public_key = self
            .get_member_public_key_for_epoch(proposal.member_id, proposal.epoch)
            .ok_or_else(|| {
                tracing::debug!(
                    member_id = ?proposal.member_id,
                    epoch = ?proposal.epoch,
                    "proposal member not found in roster for epoch"
                );
                Report::new(ValidationError).attach(format!(
                    "member {:?} not found in roster for epoch {:?}",
                    proposal.member_id, proposal.epoch
                ))
            })?;

        let data = proposal.unsigned().to_bytes();

        self.cipher_suite
            .verify(&public_key, &proposal.signature, &data)
            .map_err(|_| {
                tracing::debug!("proposal signature verification failed");
                Report::new(ValidationError).attach("proposal signature verification failed")
            })?;

        Ok(Validated::assert_valid())
    }

    async fn apply(
        &mut self,
        _proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), Report<AcceptorError>> {
        let result = self
            .external_group
            .process_incoming_message(message.mls_message)
            .change_context(AcceptorError)?;

        match result {
            ExternalReceivedMessage::Commit(commit_desc) => {
                let new_epoch = Epoch(self.external_group.group_context().epoch);
                tracing::debug!(epoch = ?new_epoch, "learned commit");

                let changes = self.process_commit_acceptor_changes(&commit_desc);
                for change in &changes {
                    match change {
                        AcceptorChangeEvent::Added { id, .. } => {
                            tracing::debug!(?id, "acceptor added");
                        }
                        AcceptorChangeEvent::Removed { id } => {
                            tracing::debug!(?id, "acceptor removed");
                        }
                    }
                }

                self.store_current_epoch_roster(new_epoch);
            }
            ExternalReceivedMessage::Proposal(_) => {
                tracing::debug!("learned proposal");
            }
            _ => {
                tracing::debug!("learned other message type");
            }
        }

        tracing::trace!(
            members = ?self.external_group.roster().members_iter().map(|m| m.index).collect::<Vec<_>>(),
            "current member indices"
        );

        Ok(())
    }
}

impl<C, CS> GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// Extracts `AcceptorAdd`/`AcceptorRemove` from applied proposals and updates the acceptor set.
    fn process_commit_acceptor_changes(
        &mut self,
        commit_desc: &mls_rs::group::CommitMessageDescription,
    ) -> Vec<AcceptorChangeEvent> {
        use mls_rs::group::CommitEffect;
        use mls_rs::group::proposal::Proposal as MlsProposal;

        let mut changes = Vec::new();

        let applied_proposals = match &commit_desc.effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => return changes,
        };

        for proposal_info in applied_proposals {
            if let MlsProposal::GroupContextExtensions(extensions) = &proposal_info.proposal {
                if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                    let id = add.acceptor_id();
                    let addr = add.0.clone();
                    self.acceptors.insert(id, addr.clone());
                    changes.push(AcceptorChangeEvent::Added { id });
                }

                if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                    let id = remove.acceptor_id();
                    self.acceptors.remove(&id);
                    changes.push(AcceptorChangeEvent::Removed { id });
                }
            }
        }

        changes
    }

    fn store_current_epoch_roster(&self, epoch: Epoch) {
        if let Some(ref store) = self.state_store {
            use crate::epoch_roster::EpochRoster;

            let members: Vec<_> = self
                .external_group
                .roster()
                .members_iter()
                .map(|m| {
                    (
                        MemberId(m.index),
                        m.signing_identity.signature_key.as_bytes().to_vec(),
                    )
                })
                .collect();

            let roster = EpochRoster::new(epoch, members);

            if let Err(e) = store.store_epoch_roster(&roster) {
                tracing::warn!(?e, ?epoch, "failed to store epoch roster");
            } else {
                tracing::debug!(?epoch, "stored epoch roster");
            }
        }
    }

    pub(crate) fn store_initial_epoch_roster(&self) {
        let epoch = Epoch(self.external_group.group_context().epoch);
        self.store_current_epoch_roster(epoch);
    }
}
