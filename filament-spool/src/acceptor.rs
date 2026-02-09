//! Paxos Acceptor for federated servers.
//!
//! Wraps an MLS `ExternalGroup` to validate proposals without decrypting messages.
//! Only applies values after consensus (quorum), not mere acceptance.

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, Attempt, Epoch, GroupContextExt, GroupId, GroupMessage, GroupProposal, LeafNodeExt,
    MemberFingerprint, MemberId, SyncProposal, UnsignedProposal,
};
use iroh::{PublicKey, SecretKey, Signature};
use mls_rs::CipherSuiteProvider;
use mls_rs::crypto::SignaturePublicKey;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalGroup, ExternalReceivedMessage};

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
    CompactionCompleted { level: u8, deleted: usize },
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
    acceptors: std::collections::BTreeSet<AcceptorId>,
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
        acceptors: impl IntoIterator<Item = AcceptorId>,
    ) -> Self {
        Self {
            external_group,
            cipher_suite,
            secret_key,
            acceptors: acceptors.into_iter().collect(),
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

    pub(crate) fn acceptor_ids(&self) -> &std::collections::BTreeSet<AcceptorId> {
        &self.acceptors
    }

    /// Read the protocol version from the group's `GroupContextExt`.
    ///
    /// # Errors
    ///
    /// Returns [`AcceptorError`] if the extension is missing or cannot be parsed.
    pub fn protocol_version(&self) -> Result<u32, Report<AcceptorError>> {
        self.external_group
            .group_context()
            .extensions
            .get_as::<GroupContextExt>()
            .change_context(AcceptorError)
            .attach("failed to decode GroupContextExt")?
            .map(|ext| ext.protocol_version)
            .ok_or_else(|| Report::new(AcceptorError).attach("missing GroupContextExt"))
    }

    fn get_member_public_key(&self, member_id: MemberId) -> Option<SignaturePublicKey> {
        self.external_group
            .roster()
            .member_with_index(member_id.0)
            .map(|m| m.signing_identity.signature_key)
            .ok()
    }

    /// Looks up the signing key for a member at a specific epoch.
    ///
    /// For current or future epochs, uses the in-memory roster (membership changes only take
    /// effect after the commit is applied, so the current roster is valid for upcoming rounds).
    /// For past epochs: would reconstruct from the nearest snapshot + replaying accepted values.
    // TODO: implement historical roster reconstruction from snapshots with throttling/caching
    fn get_member_public_key_for_epoch(
        &self,
        member_id: MemberId,
        epoch: Epoch,
    ) -> Option<SignaturePublicKey> {
        let current_epoch = Epoch(self.external_group.group_context().epoch);
        if epoch >= current_epoch {
            return self.get_member_public_key(member_id);
        }

        None
    }

    fn is_known_acceptor(&self, id: &AcceptorId) -> bool {
        self.acceptors.contains(id)
    }

    pub(crate) fn is_fingerprint_in_roster(
        &self,
        group_id: &GroupId,
        fingerprint: MemberFingerprint,
    ) -> bool {
        self.external_group.roster().members_iter().any(|member| {
            let binding_id = member
                .extensions
                .get_as::<LeafNodeExt>()
                .ok()
                .flatten()
                .map_or(0, |ext| ext.binding_id);
            let fp = MemberFingerprint::from_key(
                group_id,
                &member.signing_identity.signature_key,
                binding_id,
            );
            fp == fingerprint
        })
    }
}

impl<C, CS> filament_warp::Learner for GroupAcceptor<C, CS>
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
        self.acceptors.iter().copied().collect::<Vec<_>>()
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
    ) -> Result<filament_warp::Validated, error_stack::Report<filament_warp::ValidationError>> {
        use error_stack::Report;
        use filament_warp::{Validated, ValidationError};

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
                proposal.signature.as_ref().try_into().map_err(|_| {
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
                        AcceptorChangeEvent::CompactionCompleted { level, deleted } => {
                            tracing::info!(
                                level,
                                deleted,
                                "compaction completed — messages deleted"
                            );
                        }
                    }
                }

                self.store_snapshot(new_epoch);
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
    /// Extracts sync protocol custom proposals from applied proposals.
    fn process_commit_acceptor_changes(
        &mut self,
        commit_desc: &mls_rs::group::CommitMessageDescription,
    ) -> Vec<AcceptorChangeEvent> {
        use mls_rs::group::CommitEffect;
        use mls_rs::group::proposal::{MlsCustomProposal, Proposal as MlsProposal};

        let mut changes = Vec::new();

        let applied_proposals = match &commit_desc.effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => return changes,
        };

        for proposal_info in applied_proposals {
            if let MlsProposal::Custom(custom) = &proposal_info.proposal
                && let Ok(proposal) = SyncProposal::from_custom_proposal(custom)
            {
                match proposal {
                    SyncProposal::AcceptorAdd(id) => {
                        self.acceptors.insert(id);
                        changes.push(AcceptorChangeEvent::Added { id });
                    }
                    SyncProposal::AcceptorRemove(id) => {
                        self.acceptors.remove(&id);
                        changes.push(AcceptorChangeEvent::Removed { id });
                    }
                    SyncProposal::CompactionComplete { level, watermark } => {
                        tracing::info!(
                            level,
                            watermark_entries = watermark.len(),
                            "processing CompactionComplete proposal"
                        );

                        if let Some(ref store) = self.state_store {
                            match store.delete_before_watermark(&watermark) {
                                Ok(deleted) => {
                                    tracing::info!(
                                        deleted,
                                        level,
                                        "deleted messages for compaction"
                                    );
                                    changes.push(AcceptorChangeEvent::CompactionCompleted {
                                        level,
                                        deleted,
                                    });
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        ?e,
                                        level,
                                        "failed to delete messages for compaction"
                                    );
                                }
                            }
                        }
                    }
                    SyncProposal::CompactionClaim { .. } => {}
                }
            }
        }

        changes
    }

    fn store_snapshot(&self, epoch: Epoch) {
        if let Some(ref store) = self.state_store {
            match self.external_group.snapshot().to_bytes() {
                Ok(snapshot_bytes) => {
                    if let Err(e) = store.store_snapshot(epoch, &snapshot_bytes) {
                        tracing::warn!(?e, ?epoch, "failed to store snapshot");
                    } else {
                        tracing::debug!(?epoch, "stored snapshot");
                    }
                }
                Err(e) => {
                    tracing::warn!(?e, ?epoch, "failed to serialize snapshot");
                }
            }
        }
    }

    pub(crate) fn store_initial_snapshot(&self) {
        let epoch = Epoch(self.external_group.group_context().epoch);
        self.store_snapshot(epoch);
    }

    pub(crate) fn snapshot_bytes(&self) -> Result<Vec<u8>, mls_rs::error::MlsError> {
        self.external_group.snapshot().to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use filament_warp::Learner;
    use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
    use mls_rs::{CipherSuite, CryptoProvider, ExtensionList};
    use mls_rs_crypto_rustcrypto::RustCryptoProvider;

    use super::*;

    fn make_external_client() -> mls_rs::external_client::ExternalClient<impl ExternalMlsConfig> {
        let crypto = RustCryptoProvider::default();
        mls_rs::external_client::ExternalClient::builder()
            .crypto_provider(crypto)
            .identity_provider(BasicIdentityProvider::new())
            .extension_type(filament_core::SYNC_EXTENSION_TYPE)
            .custom_proposal_types(Some(filament_core::SYNC_PROPOSAL_TYPE))
            .build()
    }

    fn make_cipher_suite() -> impl CipherSuiteProvider + Clone {
        let crypto = RustCryptoProvider::default();
        crypto
            .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
            .unwrap()
    }

    fn make_group_info_and_sk() -> (Vec<u8>, SecretKey) {
        let crypto = RustCryptoProvider::default();
        let cs = crypto
            .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
            .unwrap();
        let (secret_key, public_key) = cs.signature_key_generate().unwrap();
        let credential = BasicCredential::new(b"test-member".to_vec());
        let signing_identity =
            mls_rs::identity::SigningIdentity::new(credential.into_credential(), public_key);

        let client = mls_rs::Client::builder()
            .crypto_provider(crypto)
            .identity_provider(BasicIdentityProvider::new())
            .extension_type(filament_core::SYNC_EXTENSION_TYPE)
            .custom_proposal_types(Some(filament_core::SYNC_PROPOSAL_TYPE))
            .signing_identity(
                signing_identity,
                secret_key.clone(),
                CipherSuite::CURVE25519_AES128,
            )
            .build();

        let group = client
            .create_group(ExtensionList::default(), ExtensionList::default(), None)
            .unwrap();
        let group_info = group.group_info_message(true).unwrap().to_bytes().unwrap();

        let sk = SecretKey::from_bytes(&secret_key.as_ref()[..32].try_into().unwrap());
        (group_info, sk)
    }

    fn make_acceptor() -> (
        GroupAcceptor<impl ExternalMlsConfig, impl CipherSuiteProvider + Clone>,
        SecretKey,
    ) {
        let external_client = make_external_client();
        let cs = make_cipher_suite();
        let (group_info_bytes, sk) = make_group_info_and_sk();

        let mls_message = mls_rs::MlsMessage::from_bytes(&group_info_bytes).unwrap();
        let external_group = external_client
            .observe_group(mls_message, None, None)
            .unwrap();

        let acceptor_sk = SecretKey::generate(&mut rand::rng());
        let acceptor_id = AcceptorId::from_bytes(*acceptor_sk.public().as_bytes());

        let acceptor =
            GroupAcceptor::new(external_group, cs, acceptor_sk.clone(), vec![acceptor_id]);

        (acceptor, sk)
    }

    #[test]
    fn acceptor_error_display() {
        let err = AcceptorError;
        assert_eq!(err.to_string(), "acceptor operation failed");
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn acceptor_change_event_debug() {
        let id = AcceptorId::from_bytes([1u8; 32]);
        let event = AcceptorChangeEvent::Added { id };
        let _ = format!("{event:?}");

        let event2 = event.clone();
        assert!(matches!(event2, AcceptorChangeEvent::Added { .. }));
    }

    #[test]
    fn node_id_is_max() {
        let (acceptor, _) = make_acceptor();
        assert_eq!(acceptor.node_id(), MemberId(u32::MAX));
    }

    #[test]
    fn current_round_returns_epoch() {
        let (acceptor, _) = make_acceptor();
        let round = acceptor.current_round();
        assert_eq!(round, Epoch(0));
    }

    #[test]
    fn acceptors_returns_known_ids() {
        let (acceptor, _) = make_acceptor();
        let ids: Vec<_> = acceptor.acceptors().into_iter().collect();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn validate_sync_proposal_unknown_acceptor() {
        let (acceptor, _) = make_acceptor();

        let unknown_key = SecretKey::generate(&mut rand::rng());
        let unsigned = UnsignedProposal::new(
            MemberId(u32::MAX),
            Epoch(0),
            Attempt(0),
            *unknown_key.public().as_bytes(),
        );
        let data = unsigned.to_bytes();
        let signature = unknown_key.sign(&data);
        let proposal = unsigned.with_signature(signature.to_bytes().to_vec());

        let result = acceptor.validate(&proposal);
        assert!(result.is_err());
    }

    #[test]
    fn validate_sync_proposal_invalid_signature_length() {
        let (acceptor, _) = make_acceptor();

        let acceptor_id = acceptor.acceptors().into_iter().next().unwrap();

        let unsigned = UnsignedProposal::new(
            MemberId(u32::MAX),
            Epoch(0),
            Attempt(0),
            *acceptor_id.as_bytes(),
        );
        let proposal = unsigned.with_signature(vec![1, 2, 3]);

        let result = acceptor.validate(&proposal);
        assert!(result.is_err());
    }

    #[test]
    fn validate_sync_proposal_success() {
        let (acceptor, _) = make_acceptor();

        let proposal = acceptor.propose(Attempt(0));
        let result = acceptor.validate(&proposal);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_member_proposal_unknown_member() {
        let (acceptor, _) = make_acceptor();

        let unsigned = UnsignedProposal::new(MemberId(999), Epoch(0), Attempt(0), [0u8; 32]);
        let proposal = unsigned.with_signature(vec![0; 64]);

        let result = acceptor.validate(&proposal);
        assert!(result.is_err());
    }

    #[test]
    fn validate_member_proposal_past_epoch() {
        let (acceptor, _) = make_acceptor();

        let unsigned = UnsignedProposal::new(MemberId(0), Epoch(100), Attempt(0), [0u8; 32]);
        let proposal = unsigned.with_signature(vec![0; 64]);

        let result = acceptor.validate(&proposal);
        assert!(result.is_err());
    }

    #[test]
    fn is_known_acceptor_yes_and_no() {
        let (acceptor, _) = make_acceptor();
        let known_id = acceptor.acceptors().into_iter().next().unwrap();
        assert!(acceptor.is_known_acceptor(&known_id));

        let unknown_id = AcceptorId::from_bytes([0xFF; 32]);
        assert!(!acceptor.is_known_acceptor(&unknown_id));
    }

    #[test]
    fn store_snapshot_no_state_store() {
        let (acceptor, _) = make_acceptor();
        assert!(acceptor.state_store.is_none());
        acceptor.store_snapshot(Epoch(0));
    }
}
