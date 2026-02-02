//! GroupAcceptor - implements paxos Acceptor for federated servers

use mls_rs::CipherSuiteProvider;
use mls_rs::crypto::SignaturePublicKey;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalGroup, ExternalReceivedMessage};
use universal_sync_paxos::Learner;

use crate::message::GroupMessage;
use crate::proposal::{AcceptorId, Epoch, GroupProposal, MemberId};

/// Errors that can occur in GroupAcceptor operations
#[derive(Debug)]
pub enum AcceptorError {
    /// MLS processing error
    Mls(mls_rs::error::MlsError),
    /// Crypto error during verification
    Crypto(String),
    /// Validation failed
    ValidationFailed,
    /// Persistence error
    Persistence(std::io::Error),
}

impl std::fmt::Display for AcceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptorError::Mls(e) => write!(f, "MLS error: {e}"),
            AcceptorError::Crypto(e) => write!(f, "crypto error: {e}"),
            AcceptorError::ValidationFailed => write!(f, "validation failed"),
            AcceptorError::Persistence(e) => write!(f, "persistence error: {e}"),
        }
    }
}

impl std::error::Error for AcceptorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AcceptorError::Mls(e) => Some(e),
            AcceptorError::Persistence(e) => Some(e),
            AcceptorError::Crypto(_) | AcceptorError::ValidationFailed => None,
        }
    }
}

impl From<mls_rs::error::MlsError> for AcceptorError {
    fn from(e: mls_rs::error::MlsError) -> Self {
        AcceptorError::Mls(e)
    }
}

/// A federated server that validates and orders group operations
///
/// Wraps an MLS `ExternalGroup` to verify signatures without being
/// able to decrypt private messages. Implements paxos `Acceptor`.
///
/// Acceptors receive signed proposals from devices, validate the signatures
/// against the group roster, and store the accepted (proposal, message) pairs.
/// Acceptors do NOT create or sign their own proposals.
///
/// The list of acceptors is tracked separately and updated when
/// commits containing [`AcceptorAdd`](crate::extension::AcceptorAdd) or
/// [`AcceptorRemove`](crate::extension::AcceptorRemove) extensions are applied.
pub struct GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// The MLS external group state (can verify signatures, track membership)
    external_group: ExternalGroup<C>,

    /// Cipher suite provider for signature verification
    cipher_suite: CS,
}

impl<C, CS> GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// Create a new acceptor from an external group
    ///
    /// # Arguments
    /// * `external_group` - The MLS external group
    /// * `cipher_suite` - Cipher suite for signature verification
    /// * `acceptors` - Initial set of acceptor IDs
    pub fn new(
        external_group: ExternalGroup<C>,
        cipher_suite: CS,
    ) -> Self {
        Self {
            external_group,
            cipher_suite,
        }
    }

    /// Check if a member ID is in the current roster
    fn is_member(&self, member_id: MemberId) -> bool {
        self.external_group
            .roster()
            .member_with_index(member_id.0)
            .is_ok()
    }

    /// Get a member's public signing key from the roster
    fn get_member_public_key(&self, member_id: MemberId) -> Option<SignaturePublicKey> {
        self.external_group
            .roster()
            .member_with_index(member_id.0)
            .map(|m| m.signing_identity.signature_key)
            .ok()
    }

    /// Verify a proposal's signature against the group roster
    fn verify_proposal(&self, proposal: &GroupProposal) -> Result<bool, AcceptorError> {
        let Some(public_key) = self.get_member_public_key(proposal.member_id) else {
            return Ok(false);
        };

        let data = proposal.unsigned().to_bytes();

        self.cipher_suite
            .verify(&public_key, &proposal.signature, &data)
            .map_err(|e| AcceptorError::Crypto(format!("{e:?}")))?;

        Ok(true)
    }

    /// Validate and process an incoming MLS message
    ///
    /// This verifies the signature and updates internal state if valid.
    /// Returns the processed message type.
    fn process_message(
        &mut self,
        msg: mls_rs::MlsMessage,
    ) -> Result<ExternalReceivedMessage, mls_rs::error::MlsError> {
        let result = self.external_group.process_incoming_message(msg)?;

        Ok(result)
    }
}

// Implement Learner trait for GroupAcceptor
impl<C, CS> universal_sync_paxos::Learner for GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    type Proposal = GroupProposal;
    type Message = GroupMessage;
    type Error = AcceptorError;
    type AcceptorId = AcceptorId;

    fn node_id(&self) -> MemberId {
        // Acceptors don't have an MLS member ID - return a dummy value
        // This is only used for proposing, which acceptors don't do
        MemberId(u32::MAX)
    }

    fn current_round(&self) -> Epoch {
        Epoch(self.external_group.group_context().epoch)
    }

    fn validate(&self, proposal: &GroupProposal) -> bool {
        // Check epoch matches current
        if proposal.epoch.0 != self.external_group.group_context().epoch {
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
    ) -> Result<(), AcceptorError> {
        // Process the MLS message from GroupMessage
        let result = self
            .external_group
            .process_incoming_message(message.mls_message)?;

        // Log what we processed
        match result {
            ExternalReceivedMessage::Commit(_) => {
                tracing::debug!(
                    epoch = self.external_group.group_context().epoch,
                    "applied commit"
                );
            }
            ExternalReceivedMessage::Proposal(_) => {
                tracing::debug!("applied proposal");
            }
            _ => {
                tracing::debug!("applied other message type");
            }
        }

        Ok(())
    }
}

// Implement Acceptor trait for GroupAcceptor
impl<C, CS> universal_sync_paxos::Acceptor for GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    async fn accept(
        &mut self,
        proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), AcceptorError> {
        // The proposal signature has already been validated before reaching here.
        // The (proposal, message) pair is stored by the AcceptorStateStore.
        //
        // TODO: Persist the accepted proposal + message durably before returning.
        // This is critical for crash recovery - we must not lose accepted values.
        //
        // For now, we just apply it (which updates the ExternalGroup state).
        // A real implementation would:
        // 1. Write (proposal, message) to durable storage
        // 2. fsync
        // 3. Then apply

        self.apply(proposal, message).await
    }
}
