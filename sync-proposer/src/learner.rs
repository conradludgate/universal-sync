//! `GroupLearner` - implements paxos Learner for MLS group members (devices)

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt};
use iroh::EndpointAddr;
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::{SignaturePublicKey, SignatureSecretKey};
use mls_rs::group::proposal::{MlsCustomProposal, Proposal as MlsProposal};
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Group};
use universal_sync_core::{
    AcceptorId, Attempt, Epoch, GroupMessage, GroupProposal, MemberFingerprint, MemberId,
    SyncProposal, UnsignedProposal,
};

/// Error marker for `GroupLearner` operations.
///
/// Use `error_stack::Report<LearnerError>` with context attachments for detailed errors.
#[derive(Debug, Default)]
pub struct LearnerError;

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("learner operation failed")
    }
}

impl std::error::Error for LearnerError {}

/// A device that participates in the MLS group as a full member
///
/// Wraps an MLS `Group` and implements the paxos `Learner` trait.
/// Can propose changes and learn from consensus.
///
/// The list of acceptors is tracked separately and updated when
/// commits containing [`AcceptorAdd`] or [`AcceptorRemove`] extensions are applied.
pub struct GroupLearner<C, CS>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// The MLS group (full member with encryption keys)
    group: Group<C>,

    /// The signing secret key for this member
    signer: SignatureSecretKey,

    /// Cipher suite provider for signing operations
    cipher_suite: CS,

    /// Current set of acceptors with their endpoint addresses
    acceptors: BTreeMap<AcceptorId, EndpointAddr>,
}

impl<C, CS> GroupLearner<C, CS>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// Create a new group learner from an MLS group
    ///
    /// # Arguments
    /// * `group` - The MLS group
    /// * `signer` - The signing secret key for this member
    /// * `cipher_suite` - Cipher suite provider for crypto operations
    /// * `acceptors` - Initial set of acceptor endpoint addresses
    pub(crate) fn new(
        group: Group<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        acceptors: impl IntoIterator<Item = EndpointAddr>,
    ) -> Self {
        Self {
            group,
            signer,
            cipher_suite,
            acceptors: acceptors
                .into_iter()
                .map(|addr| (AcceptorId::from_bytes(*addr.id.as_bytes()), addr))
                .collect(),
        }
    }

    /// Get access to the underlying MLS group
    pub(crate) fn group(&self) -> &Group<C> {
        &self.group
    }

    /// Get mutable access to the underlying MLS group
    pub(crate) fn group_mut(&mut self) -> &mut Group<C> {
        &mut self.group
    }

    /// Get the current set of acceptor IDs
    pub(crate) fn acceptor_ids(&self) -> impl ExactSizeIterator<Item = AcceptorId> + '_ {
        self.acceptors.keys().copied()
    }

    /// Get the current set of acceptors with their addresses
    pub(crate) fn acceptors(&self) -> &BTreeMap<AcceptorId, EndpointAddr> {
        &self.acceptors
    }

    /// Add an acceptor to the set by endpoint address
    pub(crate) fn add_acceptor_addr(&mut self, addr: EndpointAddr) {
        let id = AcceptorId::from_bytes(*addr.id.as_bytes());
        self.acceptors.insert(id, addr);
    }

    /// Remove an acceptor from the set by ID
    pub(crate) fn remove_acceptor_id(&mut self, acceptor_id: &AcceptorId) {
        self.acceptors.remove(acceptor_id);
    }

    /// Our own signing public key fingerprint (SHA-256).
    pub(crate) fn own_fingerprint(&self) -> MemberFingerprint {
        let member_index = self.group.current_member_index();
        let signing_key = self
            .group
            .roster()
            .members()
            .iter()
            .find(|m| m.index == member_index)
            .expect("own member must be in roster")
            .signing_identity
            .signature_key
            .clone();
        MemberFingerprint::from_signing_key(&signing_key)
    }

    /// Apply a pending commit that this member created
    ///
    /// This processes `AcceptorAdd` and `AcceptorRemove` extensions to update
    /// the internal acceptor set.
    ///
    /// # Errors
    /// Returns an error if applying the pending commit fails.
    pub(crate) fn apply_pending_commit(&mut self) -> Result<(), Report<LearnerError>> {
        let commit_output = self
            .group
            .apply_pending_commit()
            .change_context(LearnerError)?;
        self.process_commit_effect(&commit_output.effect);
        Ok(())
    }

    /// Clear any pending commit without applying it
    ///
    /// Call this when another proposer's value won consensus and we need
    /// to discard our pending commit before processing the winning value.
    pub(crate) fn clear_pending_commit(&mut self) {
        self.group.clear_pending_commit();
    }

    /// Check if we have a pending commit
    #[must_use]
    pub(crate) fn has_pending_commit(&self) -> bool {
        self.group.has_pending_commit()
    }

    /// Process a commit effect to update the acceptor set
    ///
    /// Checks for `AcceptorAdd` and `AcceptorRemove` sync proposals.
    fn process_commit_effect(&mut self, effect: &CommitEffect) {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => return,
        };

        for proposal_info in applied_proposals {
            if let MlsProposal::Custom(custom) = &proposal_info.proposal
                && let Ok(proposal) = SyncProposal::from_custom_proposal(custom)
            {
                match proposal {
                    SyncProposal::AcceptorAdd(addr) => {
                        let id = AcceptorId::from_bytes(*addr.id.as_bytes());
                        tracing::debug!(?id, "adding acceptor");
                        self.add_acceptor_addr(addr);
                    }
                    SyncProposal::AcceptorRemove(id) => {
                        tracing::debug!(?id, "removing acceptor");
                        self.remove_acceptor_id(&id);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Get the current epoch from MLS state
    pub(crate) fn mls_epoch(&self) -> Epoch {
        Epoch(self.group.context().epoch)
    }

    /// Encrypt an application message using MLS.
    ///
    /// The `authenticated_data` is included in the signature but not encrypted.
    /// It can be used for per-message metadata like message indices.
    ///
    /// # Errors
    /// Returns an error if encryption fails.
    pub(crate) fn encrypt_application_message(
        &mut self,
        message: &[u8],
        authenticated_data: Vec<u8>,
    ) -> Result<mls_rs::MlsMessage, Report<LearnerError>> {
        self.group
            .encrypt_application_message(message, authenticated_data)
            .change_context(LearnerError)
    }

    /// Check if a member ID is in the current roster
    fn is_member(&self, member_id: MemberId) -> bool {
        self.group
            .roster()
            .members()
            .iter()
            .any(|m| m.index == member_id.0)
    }

    /// Sign data using this member's signing key
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>, Report<LearnerError>> {
        self.cipher_suite
            .sign(&self.signer, data)
            .map_err(|e| Report::new(LearnerError).attach(format!("signing failed: {e:?}")))
    }

    /// Sign an unsigned proposal
    fn sign_proposal(
        &self,
        unsigned: UnsignedProposal,
    ) -> Result<GroupProposal, Report<LearnerError>> {
        let data = unsigned.to_bytes();
        let signature = self.sign(&data)?;
        Ok(unsigned.with_signature(signature))
    }

    /// Create a signed sync proposal (zero message hash)
    fn create_sync_proposal(
        &self,
        attempt: Attempt,
    ) -> Result<GroupProposal, Report<LearnerError>> {
        let unsigned = UnsignedProposal::for_sync(
            MemberId(self.group.current_member_index()),
            Epoch(self.group.context().epoch),
            attempt,
        );
        self.sign_proposal(unsigned)
    }

    /// Get a member's public signing key from the roster
    fn get_member_public_key(&self, member_id: MemberId) -> Option<SignaturePublicKey> {
        self.group
            .roster()
            .members()
            .iter()
            .find(|m| m.index == member_id.0)
            .map(|m| m.signing_identity.signature_key.clone())
    }

    /// Verify a proposal's signature
    fn verify_proposal(&self, proposal: &GroupProposal) -> Result<(), Report<LearnerError>> {
        let public_key = self
            .get_member_public_key(proposal.member_id)
            .ok_or_else(|| {
                Report::new(LearnerError).attach(format!(
                    "member {:?} not found in roster",
                    proposal.member_id
                ))
            })?;

        let data = proposal.unsigned().to_bytes();

        self.cipher_suite
            .verify(&public_key, &proposal.signature, &data)
            .map_err(|e| {
                Report::new(LearnerError).attach(format!("signature verification failed: {e:?}"))
            })?;

        Ok(())
    }
}

// Implement Learner trait for GroupLearner
impl<C, CS> universal_sync_paxos::Learner for GroupLearner<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    type Proposal = GroupProposal;
    type Message = GroupMessage;
    type Error = Report<LearnerError>;
    type AcceptorId = AcceptorId;

    fn node_id(&self) -> MemberId {
        MemberId(self.group.current_member_index())
    }

    fn current_round(&self) -> Epoch {
        Epoch(self.group.context().epoch)
    }

    fn acceptors(&self) -> impl IntoIterator<Item = AcceptorId, IntoIter: ExactSizeIterator> {
        self.acceptors.keys().copied()
    }

    fn propose(&self, attempt: Attempt) -> GroupProposal {
        // Create a signed sync proposal
        self.create_sync_proposal(attempt)
            .expect("signing should not fail")
    }

    fn validate(
        &self,
        proposal: &GroupProposal,
    ) -> Result<universal_sync_paxos::Validated, Report<universal_sync_paxos::ValidationError>>
    {
        use universal_sync_paxos::{Validated, ValidationError};

        // Check epoch matches current
        if proposal.epoch.0 != self.group.context().epoch {
            return Err(Report::new(ValidationError).attach(format!(
                "epoch mismatch: expected {}, got {}",
                self.group.context().epoch,
                proposal.epoch.0
            )));
        }

        // Check sender is a valid group member
        if !self.is_member(proposal.member_id) {
            return Err(Report::new(ValidationError).attach(format!(
                "member {:?} not found in roster",
                proposal.member_id
            )));
        }

        // All proposals must have signatures
        if proposal.signature.is_empty() {
            return Err(Report::new(ValidationError).attach("proposal has empty signature"));
        }

        // Verify the signature
        self.verify_proposal(proposal)
            .change_context(ValidationError)
            .attach("signature verification failed")?;

        Ok(Validated::assert_valid())
    }

    async fn apply(
        &mut self,
        proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), Report<LearnerError>> {
        // Check if this is our own proposal
        let my_proposal = proposal.member_id == MemberId(self.group.current_member_index());

        if my_proposal && self.has_pending_commit() {
            // This is our proposal and we have a pending commit - apply it
            tracing::debug!("applying our own pending commit");
            self.apply_pending_commit()
        } else {
            // This is someone else's proposal, or we don't have a pending commit
            // Clear any pending commit we might have and process the incoming message
            if self.has_pending_commit() {
                tracing::debug!("clearing pending commit - another proposal won");
                self.clear_pending_commit();
            }

            // Process the MLS message from GroupMessage
            let result = self
                .group
                .process_incoming_message(message.mls_message)
                .change_context(LearnerError)?;

            // Handle the result and update acceptor set if needed
            match result {
                ReceivedMessage::Commit(commit_desc) => {
                    tracing::debug!(epoch = self.group.context().epoch, "applied commit");

                    // Process acceptor changes from applied proposals
                    self.process_commit_effect(&commit_desc.effect);
                }
                ReceivedMessage::Proposal(_) => {
                    tracing::debug!("applied proposal");
                }
                ReceivedMessage::ApplicationMessage(_) => {
                    tracing::debug!("applied application message");
                }
                _ => {
                    tracing::debug!("applied other message type");
                }
            }

            Ok(())
        }
    }
}
