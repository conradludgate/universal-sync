//! `GroupAcceptor` - implements paxos Acceptor for federated servers
//!
//! # Consensus vs Acceptance
//!
//! An important distinction in Paxos is between *acceptance* and *consensus*:
//!
//! - **Acceptance**: A single acceptor has received and persisted a value
//! - **Consensus**: A quorum of acceptors have accepted the same value
//!
//! Only when consensus is reached should the value be considered committed.
//! The MLS state should only be updated with committed values.
//!
//! # Single Acceptor Deployment
//!
//! With a single acceptor, acceptance equals consensus (quorum of 1).
//! The current implementation supports this case: when a value is accepted,
//! it's stored in the state store and later "learned" when the acceptor
//! is recreated for a new connection.
//!
//! # Multi-Acceptor Deployment (TODO)
//!
//! For multiple acceptors, each acceptor server should:
//!
//! 1. Run the acceptor protocol (handle incoming Accept requests)
//! 2. Run a **learner process** that connects to all acceptors
//! 3. The learner tracks which values have reached quorum
//! 4. Only when quorum is confirmed should `apply()` be called
//!
//! The learner for an acceptor only needs `(quorum - 1)` confirmations from
//! other acceptors since it counts itself. For 3 acceptors with quorum 2,
//! an acceptor that has accepted a value only needs 1 confirmation from
//! another acceptor before applying.

use iroh::{EndpointAddr, PublicKey, SecretKey, Signature};
use mls_rs::CipherSuiteProvider;
use mls_rs::crypto::SignaturePublicKey;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalGroup, ExternalReceivedMessage};
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, Attempt, Epoch, GroupMessage, GroupProposal, MemberId,
    UnsignedProposal,
};

use crate::connector::ConnectorError;

/// Errors that can occur in `GroupAcceptor` operations
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

impl From<ConnectorError> for AcceptorError {
    fn from(e: ConnectorError) -> Self {
        AcceptorError::Crypto(e.to_string())
    }
}

impl From<std::io::Error> for AcceptorError {
    fn from(e: std::io::Error) -> Self {
        AcceptorError::Crypto(format!("IO error: {e}"))
    }
}

/// Event emitted when the acceptor set changes during apply
#[derive(Debug, Clone)]
pub(crate) enum AcceptorChangeEvent {
    /// An acceptor was added
    Added {
        /// The acceptor ID
        id: AcceptorId,
        /// The endpoint address
        addr: EndpointAddr,
    },
    /// An acceptor was removed
    Removed {
        /// The acceptor ID
        id: AcceptorId,
    },
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
/// commits containing [`AcceptorAdd`](universal_sync_core::AcceptorAdd) or
/// [`AcceptorRemove`](universal_sync_core::AcceptorRemove) extensions are applied.
pub struct GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// The MLS external group state (can verify signatures, track membership)
    external_group: ExternalGroup<C>,

    /// Cipher suite provider for MLS signature verification
    cipher_suite: CS,

    /// This acceptor's iroh secret key (for signing sync proposals)
    secret_key: SecretKey,

    /// Map of known acceptors: ID -> endpoint address
    ///
    /// This is updated when commits containing `AcceptorAdd`/`AcceptorRemove`
    /// are applied.
    acceptors: std::collections::BTreeMap<AcceptorId, EndpointAddr>,

    /// Optional state store for epoch roster lookups
    ///
    /// When set, this allows looking up member public keys from historical epochs
    /// instead of just the current roster. This is needed because member indices
    /// can change across epochs (e.g., when members are removed).
    state_store: Option<crate::state_store::GroupStateStore>,
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
    /// * `cipher_suite` - Cipher suite for MLS signature verification
    /// * `secret_key` - This acceptor's iroh secret key (for signing sync proposals)
    /// * `acceptors` - Initial set of known acceptors with addresses (from `GroupInfo` extensions)
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

    /// Set the state store for epoch roster lookups
    #[must_use]
    pub(crate) fn with_state_store(
        mut self,
        state_store: crate::state_store::GroupStateStore,
    ) -> Self {
        self.state_store = Some(state_store);
        self
    }

    /// Get this acceptor's own ID (derived from secret key)
    pub(crate) fn own_id(&self) -> AcceptorId {
        AcceptorId::from_bytes(*self.secret_key.public().as_bytes())
    }

    /// Update the set of known acceptors from addresses
    pub(crate) fn set_acceptors(&mut self, acceptors: impl IntoIterator<Item = EndpointAddr>) {
        self.acceptors = acceptors
            .into_iter()
            .map(|addr| {
                let id = AcceptorId::from_bytes(*addr.id.as_bytes());
                (id, addr)
            })
            .collect();
    }

    /// Get a reference to the external group
    pub(crate) fn external_group(&self) -> &ExternalGroup<C> {
        &self.external_group
    }

    /// Get the acceptor addresses
    pub(crate) fn acceptor_addrs(&self) -> impl Iterator<Item = (&AcceptorId, &EndpointAddr)> {
        self.acceptors.iter()
    }

    /// Add an acceptor by address
    ///
    /// Returns the `AcceptorId` that was added.
    pub(crate) fn add_acceptor(&mut self, addr: EndpointAddr) -> AcceptorId {
        let id = AcceptorId::from_bytes(*addr.id.as_bytes());
        self.acceptors.insert(id, addr);
        id
    }

    /// Remove an acceptor by ID
    ///
    /// Returns the address if the acceptor was present.
    pub(crate) fn remove_acceptor(&mut self, id: &AcceptorId) -> Option<EndpointAddr> {
        self.acceptors.remove(id)
    }

    /// Get a member's public signing key from the current roster
    fn get_member_public_key(&self, member_id: MemberId) -> Option<SignaturePublicKey> {
        self.external_group
            .roster()
            .member_with_index(member_id.0)
            .map(|m| m.signing_identity.signature_key)
            .ok()
    }

    /// Get a member's public signing key for a specific epoch
    ///
    /// First tries to look up from stored epoch rosters (if state store is set),
    /// then falls back to the current roster if the epoch matches or no roster is found.
    fn get_member_public_key_for_epoch(
        &self,
        member_id: MemberId,
        epoch: Epoch,
    ) -> Option<SignaturePublicKey> {
        // If we have a state store, try to look up from epoch rosters
        if let Some(ref store) = self.state_store
            && let Some(roster) = store.get_epoch_roster_at_or_before(epoch)
            && let Some(key_bytes) = roster.get_member_key(member_id)
        {
            return Some(SignaturePublicKey::new_slice(key_bytes));
        }

        // Fall back to current roster if epoch matches current or no epoch roster found
        let current_epoch = Epoch(self.external_group.group_context().epoch);
        if epoch == current_epoch {
            return self.get_member_public_key(member_id);
        }

        // No way to validate this proposal - member may have been removed
        None
    }

    /// Check if an acceptor ID is in the known set
    fn is_known_acceptor(&self, id: &AcceptorId) -> bool {
        self.acceptors.contains_key(id)
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
        // Acceptors don't have an MLS member ID - use a sentinel value
        // Sync proposals from acceptors use this ID
        MemberId(u32::MAX)
    }

    fn current_round(&self) -> Epoch {
        Epoch(self.external_group.group_context().epoch)
    }

    fn acceptors(&self) -> impl IntoIterator<Item = AcceptorId, IntoIter: ExactSizeIterator> {
        self.acceptors.keys().copied().collect::<Vec<_>>()
    }

    fn propose(&self, attempt: Attempt) -> GroupProposal {
        // Create a signed sync proposal for the learning process
        //
        // For sync proposals (node_id == MemberId(u32::MAX)):
        // - message_hash = iroh public key (32 bytes)
        // - signature = iroh ed25519 signature over unsigned proposal bytes
        //
        // Other acceptors validate by checking:
        // 1. The public key is in the known acceptor set
        // 2. The signature is valid for that public key
        let unsigned = UnsignedProposal::new(
            MemberId(u32::MAX),
            Epoch(self.external_group.group_context().epoch),
            attempt,
            *self.secret_key.public().as_bytes(), // Store our public key in message_hash
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

        // Sync proposals from acceptors have sentinel MemberId
        // The message_hash contains the acceptor's iroh public key
        // We validate by checking the public key is known and signature is valid
        if proposal.member_id == MemberId(u32::MAX) {
            // Extract acceptor's public key from message_hash
            let acceptor_id = AcceptorId::from_bytes(proposal.message_hash);

            // Check this is a known acceptor
            if !self.is_known_acceptor(&acceptor_id) {
                tracing::debug!(?acceptor_id, "sync proposal from unknown acceptor");
                return Err(Report::new(ValidationError)
                    .attach("sync proposal from unknown acceptor")
                    .attach(format!("acceptor_id: {acceptor_id:?}")));
            }

            // Verify the iroh ed25519 signature
            let public_key = PublicKey::from_bytes(&proposal.message_hash).map_err(|_| {
                tracing::debug!("invalid public key in message_hash");
                Report::new(ValidationError).attach("invalid public key in message_hash")
            })?;

            // Signature must be exactly 64 bytes (ed25519)
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

        // Regular proposals from MLS group members
        // Look up the public key for the epoch the proposal was created in
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
    ) -> Result<(), AcceptorError> {
        // Apply a LEARNED value to the MLS state.
        //
        // This should only be called when the value has reached consensus
        // (quorum of acceptors have accepted). The caller is responsible
        // for confirming quorum before calling this.
        //
        // For a single-acceptor deployment, acceptance = consensus, so
        // this is called immediately after accept.
        //
        // For multi-acceptor deployments, a background learner process
        // should track quorum and call this when consensus is reached.

        // Process the MLS message from GroupMessage
        let result = self
            .external_group
            .process_incoming_message(message.mls_message)?;

        // Log what we processed and store epoch roster for commits
        match result {
            ExternalReceivedMessage::Commit(commit_desc) => {
                let new_epoch = Epoch(self.external_group.group_context().epoch);
                tracing::debug!(epoch = ?new_epoch, "learned commit");

                // Process acceptor changes from the commit
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

                // Store the epoch roster for future signature validation
                self.store_current_epoch_roster(new_epoch);
            }
            ExternalReceivedMessage::Proposal(_) => {
                tracing::debug!("learned proposal");
            }
            _ => {
                tracing::debug!("learned other message type");
            }
        }

        tracing::debug!(
            members = ?self.external_group.roster().members_iter().map(|m| (m.index, m.signing_identity.signature_key)).collect::<Vec<_>>(),
            "current members"
        );

        Ok(())
    }
}

// Implement Acceptor trait for GroupAcceptor (marker trait)
impl<C, CS> universal_sync_paxos::Acceptor for GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    // No methods required - Acceptor is now a marker trait.
    //
    // The key insight is that:
    // - Validation is handled by Learner::validate()
    // - Persistence is handled by AcceptorStateStore::accept()
    // - Application is handled by Learner::apply() when quorum is confirmed
    //
    // This acceptor validates proposals from MLS group members and
    // stores accepted values, but only applies them when learning
    // confirms that consensus was reached.
}

// Private helper methods
impl<C, CS> GroupAcceptor<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// Process acceptor changes from a commit
    ///
    /// Extracts `AcceptorAdd`/`AcceptorRemove` from the applied proposals
    /// and updates the acceptor set accordingly.
    ///
    /// Returns a list of change events for the caller to handle (e.g., to
    /// spawn or close peer connections).
    fn process_commit_acceptor_changes(
        &mut self,
        commit_desc: &mls_rs::group::CommitMessageDescription,
    ) -> Vec<AcceptorChangeEvent> {
        use mls_rs::group::CommitEffect;
        use mls_rs::group::proposal::Proposal as MlsProposal;

        let mut changes = Vec::new();

        // Get the applied proposals from the commit effect
        let applied_proposals = match &commit_desc.effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => return changes, // No proposals to process
        };

        // Iterate through applied proposals looking for GroupContextExtensions
        for proposal_info in applied_proposals {
            if let MlsProposal::GroupContextExtensions(extensions) = &proposal_info.proposal {
                // Check for AcceptorAdd
                if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                    let id = add.acceptor_id();
                    let addr = add.0.clone();
                    self.acceptors.insert(id, addr.clone());
                    changes.push(AcceptorChangeEvent::Added { id, addr });
                }

                // Check for AcceptorRemove
                if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                    let id = remove.acceptor_id();
                    self.acceptors.remove(&id);
                    changes.push(AcceptorChangeEvent::Removed { id });
                }
            }
        }

        changes
    }

    /// Store the current roster as an epoch snapshot
    fn store_current_epoch_roster(&self, epoch: Epoch) {
        if let Some(ref store) = self.state_store {
            use crate::epoch_roster::EpochRoster;

            // Collect member public keys from the current roster
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
}
