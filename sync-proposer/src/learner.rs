//! `GroupLearner` - implements paxos Learner for MLS group members (devices)

use std::collections::BTreeMap;

use iroh::EndpointAddr;
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::{SignaturePublicKey, SignatureSecretKey};
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Group};
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, Attempt, Epoch, GroupMessage, GroupProposal, MemberId,
    UnsignedProposal,
};

/// Errors that can occur in `GroupLearner` operations
#[derive(Debug)]
pub enum LearnerError {
    /// MLS processing error
    Mls(mls_rs::error::MlsError),
    /// Crypto error during signing/verification
    Crypto(String),
    /// IO error
    Io(std::io::Error),
    /// Unexpected message type
    UnexpectedMessageType,
}

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LearnerError::Mls(e) => write!(f, "MLS error: {e:?}"),
            LearnerError::Crypto(e) => write!(f, "crypto error: {e}"),
            LearnerError::Io(e) => write!(f, "IO error: {e}"),
            LearnerError::UnexpectedMessageType => write!(f, "unexpected message type"),
        }
    }
}

impl std::error::Error for LearnerError {}

impl From<mls_rs::error::MlsError> for LearnerError {
    fn from(e: mls_rs::error::MlsError) -> Self {
        LearnerError::Mls(e)
    }
}

impl From<crate::connector::ConnectorError> for LearnerError {
    fn from(e: crate::connector::ConnectorError) -> Self {
        LearnerError::Crypto(e.to_string())
    }
}

impl From<std::io::Error> for LearnerError {
    fn from(e: std::io::Error) -> Self {
        LearnerError::Io(e)
    }
}

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
    pub fn new(
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
    pub fn group(&self) -> &Group<C> {
        &self.group
    }

    /// Get mutable access to the underlying MLS group
    pub fn group_mut(&mut self) -> &mut Group<C> {
        &mut self.group
    }

    /// Get the current set of acceptor IDs
    pub fn acceptor_ids(&self) -> impl Iterator<Item = AcceptorId> + '_ {
        self.acceptors.keys().copied()
    }

    /// Get the current set of acceptors with their addresses
    pub fn acceptors(&self) -> &BTreeMap<AcceptorId, EndpointAddr> {
        &self.acceptors
    }

    /// Add an acceptor to the set by endpoint address
    pub fn add_acceptor_addr(&mut self, addr: EndpointAddr) {
        let id = AcceptorId::from_bytes(*addr.id.as_bytes());
        self.acceptors.insert(id, addr);
    }

    /// Remove an acceptor from the set by ID
    pub fn remove_acceptor_id(&mut self, acceptor_id: &AcceptorId) {
        self.acceptors.remove(acceptor_id);
    }

    /// Apply a pending commit that this member created
    ///
    /// This processes `AcceptorAdd` and `AcceptorRemove` extensions to update
    /// the internal acceptor set.
    ///
    /// # Errors
    /// Returns an error if applying the pending commit fails.
    pub fn apply_pending_commit(&mut self) -> Result<(), LearnerError> {
        let commit_output = self.group.apply_pending_commit()?;
        self.process_commit_effect(&commit_output.effect);
        Ok(())
    }

    /// Clear any pending commit without applying it
    ///
    /// Call this when another proposer's value won consensus and we need
    /// to discard our pending commit before processing the winning value.
    pub fn clear_pending_commit(&mut self) {
        self.group.clear_pending_commit();
    }

    /// Check if we have a pending commit
    #[must_use]
    pub fn has_pending_commit(&self) -> bool {
        self.group.has_pending_commit()
    }

    /// Process a commit effect to update the acceptor set
    ///
    /// Checks for `AcceptorAdd` and `AcceptorRemove` extensions in any
    /// `GroupContextExtensions` proposals that were applied.
    fn process_commit_effect(&mut self, effect: &CommitEffect) {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => return, // No proposals to process
        };

        // Look for GroupContextExtensions proposals
        for proposal_info in applied_proposals {
            if let MlsProposal::GroupContextExtensions(extensions) = &proposal_info.proposal {
                // Check for AcceptorAdd
                if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                    tracing::debug!(acceptor_id = ?add.acceptor_id(), "adding acceptor");
                    self.add_acceptor_addr(add.0.clone());
                }

                // Check for AcceptorRemove
                if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                    tracing::debug!(acceptor_id = ?remove.acceptor_id(), "removing acceptor");
                    self.remove_acceptor_id(&remove.acceptor_id());
                }
            }
        }
    }

    /// Get the current epoch from MLS state
    pub fn mls_epoch(&self) -> Epoch {
        Epoch(self.group.context().epoch)
    }

    /// Encrypt an application message using MLS.
    ///
    /// The `authenticated_data` is included in the signature but not encrypted.
    /// It can be used for per-message metadata like message indices.
    ///
    /// # Errors
    /// Returns an error if encryption fails.
    pub fn encrypt_application_message(
        &mut self,
        message: &[u8],
        authenticated_data: Vec<u8>,
    ) -> Result<mls_rs::MlsMessage, LearnerError> {
        self.group
            .encrypt_application_message(message, authenticated_data)
            .map_err(LearnerError::Mls)
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
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>, LearnerError> {
        self.cipher_suite
            .sign(&self.signer, data)
            .map_err(|e| LearnerError::Crypto(format!("{e:?}")))
    }

    /// Sign an unsigned proposal
    fn sign_proposal(&self, unsigned: UnsignedProposal) -> Result<GroupProposal, LearnerError> {
        let data = unsigned.to_bytes();
        let signature = self.sign(&data)?;
        Ok(unsigned.with_signature(signature))
    }

    /// Create a signed sync proposal (zero message hash)
    fn create_sync_proposal(&self, attempt: Attempt) -> Result<GroupProposal, LearnerError> {
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
    fn verify_proposal(&self, proposal: &GroupProposal) -> Result<(), LearnerError> {
        let public_key = self
            .get_member_public_key(proposal.member_id)
            .ok_or_else(|| {
                LearnerError::Crypto(format!(
                    "member {:?} not found in roster",
                    proposal.member_id
                ))
            })?;

        let data = proposal.unsigned().to_bytes();

        self.cipher_suite
            .verify(&public_key, &proposal.signature, &data)
            .map_err(|e| LearnerError::Crypto(format!("{e:?}")))?;

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
    type Error = LearnerError;
    type AcceptorId = AcceptorId;

    fn node_id(&self) -> MemberId {
        MemberId(self.group.current_member_index())
    }

    fn current_round(&self) -> Epoch {
        Epoch(self.group.context().epoch)
    }

    fn acceptors(&self) -> impl IntoIterator<Item = AcceptorId> {
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
    ) -> Result<
        universal_sync_paxos::Validated,
        error_stack::Report<universal_sync_paxos::ValidationError>,
    > {
        use error_stack::Report;
        use universal_sync_paxos::{Validated, ValidationError};

        // Check epoch matches current
        if proposal.epoch.0 != self.group.context().epoch {
            return Err(Report::new(ValidationError).attach_printable({
                format!(
                    "epoch mismatch: expected {}, got {}",
                    self.group.context().epoch,
                    proposal.epoch.0
                )
            }));
        }

        // Check sender is a valid group member
        if !self.is_member(proposal.member_id) {
            return Err(Report::new(ValidationError).attach_printable({
                format!("member {:?} not found in roster", proposal.member_id)
            }));
        }

        // All proposals must have signatures
        if proposal.signature.is_empty() {
            return Err(
                Report::new(ValidationError).attach_printable("proposal has empty signature")
            );
        }

        // Verify the signature
        self.verify_proposal(proposal).map_err(|e| {
            Report::new(ValidationError)
                .attach_printable("signature verification failed")
                .attach_printable(e.to_string())
        })?;

        Ok(Validated::assert_valid())
    }

    async fn apply(
        &mut self,
        proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), LearnerError> {
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
            let result = self.group.process_incoming_message(message.mls_message)?;

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
