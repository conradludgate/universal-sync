//! GroupLearner - implements paxos Learner for MLS group members (devices)

use std::collections::BTreeSet;

use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::{SignaturePublicKey, SignatureSecretKey};
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Group};

use crate::extension::{AcceptorAdd, AcceptorRemove};
use crate::message::GroupMessage;
use crate::proposal::{AcceptorId, Attempt, Epoch, GroupProposal, MemberId, UnsignedProposal};

/// Errors that can occur in GroupLearner operations
#[derive(Debug)]
pub enum LearnerError {
    /// MLS processing error
    Mls(mls_rs::error::MlsError),
    /// Crypto error during signing/verification
    Crypto(String),
    /// Unexpected message type
    UnexpectedMessageType,
}

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LearnerError::Mls(e) => write!(f, "MLS error: {e}"),
            LearnerError::Crypto(e) => write!(f, "crypto error: {e}"),
            LearnerError::UnexpectedMessageType => write!(f, "unexpected message type"),
        }
    }
}

impl std::error::Error for LearnerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LearnerError::Mls(e) => Some(e),
            LearnerError::Crypto(_) | LearnerError::UnexpectedMessageType => None,
        }
    }
}

impl From<mls_rs::error::MlsError> for LearnerError {
    fn from(e: mls_rs::error::MlsError) -> Self {
        LearnerError::Mls(e)
    }
}

/// A device that participates in the MLS group as a full member
///
/// Wraps an MLS `Group` and implements the paxos `Learner` trait.
/// Can propose changes and learn from consensus.
///
/// The list of acceptors is tracked separately and updated when
/// commits containing [`AcceptorAdd`](crate::extension::AcceptorAdd) or
/// [`AcceptorRemove`](crate::extension::AcceptorRemove) extensions are applied.
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

    /// Current set of acceptor IDs (iroh public keys)
    acceptors: BTreeSet<AcceptorId>,
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
    /// * `acceptors` - Initial set of acceptor IDs
    pub fn new(
        group: Group<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        acceptors: impl IntoIterator<Item = AcceptorId>,
    ) -> Self {
        Self {
            group,
            signer,
            cipher_suite,
            acceptors: acceptors.into_iter().collect(),
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
    pub fn acceptor_ids(&self) -> &BTreeSet<AcceptorId> {
        &self.acceptors
    }

    /// Add an acceptor to the set
    fn add_acceptor(&mut self, acceptor_id: AcceptorId) {
        self.acceptors.insert(acceptor_id);
    }

    /// Remove an acceptor from the set
    fn remove_acceptor(&mut self, acceptor_id: &AcceptorId) {
        self.acceptors.remove(acceptor_id);
    }

    /// Process a commit effect to update the acceptor set
    ///
    /// Checks for `AcceptorAdd` and `AcceptorRemove` extensions in any
    /// `GroupContextExtensions` proposals that were applied.
    fn process_commit_effect(&mut self, effect: &CommitEffect) {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) => &new_epoch.applied_proposals,
            CommitEffect::Removed { new_epoch, .. } => &new_epoch.applied_proposals,
            CommitEffect::ReInit(_) => return, // No proposals to process
        };

        // Look for GroupContextExtensions proposals
        for proposal_info in applied_proposals {
            if let MlsProposal::GroupContextExtensions(extensions) = &proposal_info.proposal {
                // Check for AcceptorAdd
                if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                    tracing::debug!(acceptor_id = ?add.acceptor_id(), "adding acceptor");
                    self.add_acceptor(add.acceptor_id());
                }

                // Check for AcceptorRemove
                if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                    tracing::debug!(acceptor_id = ?remove.acceptor_id(), "removing acceptor");
                    self.remove_acceptor(&remove.acceptor_id());
                }
            }
        }
    }

    /// Get the current epoch from MLS state
    pub fn mls_epoch(&self) -> Epoch {
        Epoch(self.group.context().epoch)
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
    fn verify_proposal(&self, proposal: &GroupProposal) -> Result<bool, LearnerError> {
        let Some(public_key) = self.get_member_public_key(proposal.member_id) else {
            return Ok(false);
        };

        let data = proposal.unsigned().to_bytes();

        self.cipher_suite
            .verify(&public_key, &proposal.signature, &data)
            .map_err(|e| LearnerError::Crypto(format!("{e:?}")))?;

        Ok(true)
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

    fn validate(&self, proposal: &GroupProposal) -> bool {
        // Check epoch matches current
        if proposal.epoch.0 != self.group.context().epoch {
            return false;
        }

        // Check sender is a valid group member
        if !self.is_member(proposal.member_id) {
            return false;
        }

        // All proposals must have signatures
        if proposal.signature.is_empty() {
            return false;
        }

        // Verify the signature
        self.verify_proposal(proposal).unwrap_or(false)
    }

    async fn apply(
        &mut self,
        _proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), LearnerError> {
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

// Implement Proposer trait for GroupLearner - devices can create proposals
impl<C, CS> universal_sync_paxos::Proposer for GroupLearner<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    fn acceptors(&self) -> impl IntoIterator<Item = AcceptorId> {
        self.acceptors.iter().copied()
    }

    fn propose(&self, attempt: Attempt) -> GroupProposal {
        // Create a signed sync proposal
        self.create_sync_proposal(attempt)
            .expect("signing should not fail")
    }
}
