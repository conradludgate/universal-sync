//! GroupProposal - implements paxos Proposal trait for MLS group operations

use serde::{Deserialize, Serialize};
use universal_sync_paxos::Proposal;

/// Unique identifier for a group member (MLS leaf index)
///
/// Proposers (devices) are identified by their MLS leaf index within the group.
/// This is used as `Proposal::NodeId` in the Paxos protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberId(pub u32);

/// Unique identifier for an acceptor (iroh ed25519 public key)
///
/// Acceptors (federated servers and other nodes) are identified by their
/// iroh public key (32 bytes). This is used as `Learner::AcceptorId` in
/// the Paxos protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AcceptorId(pub [u8; 32]);

impl AcceptorId {
    /// Create an AcceptorId from raw bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Round identifier - MLS epoch number
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct Epoch(pub u64);

/// Attempt identifier within a round
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct Attempt(pub u64);

/// The unsigned portion of a proposal (for signing/verification)
///
/// Create this first, then sign it to produce a [`GroupProposal`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsignedProposal {
    /// The proposing member's ID (MLS leaf index)
    pub member_id: MemberId,
    /// Current MLS epoch (Paxos round)
    pub epoch: Epoch,
    /// Attempt number within this epoch
    pub attempt: Attempt,
    /// SHA-256 hash of the MlsMessage bytes, or zeros for sync/dummy proposals
    pub message_hash: [u8; 32],
}

impl UnsignedProposal {
    /// Create a new unsigned proposal
    pub fn new(
        member_id: MemberId,
        epoch: Epoch,
        attempt: Attempt,
        message_hash: [u8; 32],
    ) -> Self {
        Self {
            member_id,
            epoch,
            attempt,
            message_hash,
        }
    }

    /// Create an unsigned proposal for sync (zero message hash)
    pub fn for_sync(member_id: MemberId, epoch: Epoch, attempt: Attempt) -> Self {
        Self::new(member_id, epoch, attempt, [0u8; 32])
    }

    /// Serialize for signing
    pub fn to_bytes(&self) -> Vec<u8> {
        // Use postcard for deterministic serialization
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

    /// Sign this proposal to produce a [`GroupProposal`]
    pub fn with_signature(self, signature: Vec<u8>) -> GroupProposal {
        GroupProposal {
            member_id: self.member_id,
            epoch: self.epoch,
            attempt: self.attempt,
            message_hash: self.message_hash,
            signature,
        }
    }
}

/// A signed proposal for group state changes
///
/// This is the Paxos ordering key - it identifies WHO is proposing
/// and provides ordering via (epoch, attempt, member_id).
///
/// The proposal is signed by the proposing member's MLS signing key,
/// binding the proposal to their identity. The `message_hash` commits
/// to the actual MLS message content (in [`GroupMessage`](crate::message::GroupMessage)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProposal {
    /// The proposing member's ID (MLS leaf index)
    pub member_id: MemberId,
    /// Current MLS epoch (Paxos round)
    pub epoch: Epoch,
    /// Attempt number within this epoch
    pub attempt: Attempt,
    /// SHA-256 hash of the MlsMessage bytes, or zeros for sync/dummy proposals
    pub message_hash: [u8; 32],
    /// Signature over (member_id, epoch, attempt, message_hash) using MLS signing key
    pub signature: Vec<u8>,
}

impl GroupProposal {
    /// Get the unsigned portion for verification
    pub fn unsigned(&self) -> UnsignedProposal {
        UnsignedProposal {
            member_id: self.member_id,
            epoch: self.epoch,
            attempt: self.attempt,
            message_hash: self.message_hash,
        }
    }

    /// Check if this is a sync proposal (zero message hash)
    pub fn is_sync(&self) -> bool {
        self.message_hash == [0u8; 32]
    }
}

impl Proposal for GroupProposal {
    type NodeId = MemberId;
    type RoundId = Epoch;
    type AttemptId = Attempt;

    fn node_id(&self) -> Self::NodeId {
        self.member_id
    }

    fn round(&self) -> Self::RoundId {
        self.epoch
    }

    fn attempt(&self) -> Self::AttemptId {
        self.attempt
    }

    fn next_attempt(attempt: Self::AttemptId) -> Self::AttemptId {
        Attempt(attempt.0 + 1)
    }
}
