//! Paxos [`Proposal`] implementation for MLS group operations.

use filament_warp::Proposal;
use serde::{Deserialize, Serialize};

/// Group member identifier (MLS leaf index, used as `Proposal::NodeId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberId(pub u32);

/// Acceptor identifier (iroh ed25519 public key, 32 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AcceptorId(pub [u8; 32]);

impl AcceptorId {
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Paxos round identifier (MLS epoch number).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Epoch(pub u64);

/// Attempt number within a Paxos round.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Attempt(pub u64);

/// Unsigned proposal data, to be signed into a [`GroupProposal`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsignedProposal {
    pub member_id: MemberId,
    pub epoch: Epoch,
    pub attempt: Attempt,
    /// SHA-256 of the `MlsMessage` bytes, or zeros for sync proposals.
    pub message_hash: [u8; 32],
}

impl UnsignedProposal {
    #[must_use]
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

    #[must_use]
    pub fn for_sync(member_id: MemberId, epoch: Epoch, attempt: Attempt) -> Self {
        Self::new(member_id, epoch, attempt, [0u8; 32])
    }

    /// # Panics
    ///
    /// Panics if postcard serialization fails (should not happen for this type).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

    #[must_use]
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

/// Signed proposal — the Paxos ordering key.
///
/// Ordered by (epoch, attempt, `member_id`). Signed with the member's MLS key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProposal {
    pub member_id: MemberId,
    pub epoch: Epoch,
    pub attempt: Attempt,
    pub message_hash: [u8; 32],
    pub signature: Vec<u8>,
}

impl GroupProposal {
    #[must_use]
    pub fn unsigned(&self) -> UnsignedProposal {
        UnsignedProposal {
            member_id: self.member_id,
            epoch: self.epoch,
            attempt: self.attempt,
            message_hash: self.message_hash,
        }
    }

    #[must_use]
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

use crate::codec::Versioned;

impl Versioned for GroupProposal {
    fn serialize_versioned(&self, protocol_version: u32) -> Result<Vec<u8>, postcard::Error> {
        match protocol_version {
            1 => postcard::to_allocvec(self),
            _ => Err(postcard::Error::SerializeBufferFull),
        }
    }

    fn deserialize_versioned(protocol_version: u32, bytes: &[u8]) -> Result<Self, postcard::Error> {
        match protocol_version {
            1 => postcard::from_bytes(bytes),
            _ => Err(postcard::Error::DeserializeUnexpectedEnd),
        }
    }
}
