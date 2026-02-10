//! Paxos [`Proposal`] implementation for MLS group operations.

use bytes::Bytes;
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

impl From<[u8; 32]> for AcceptorId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
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
    pub fn with_signature(self, signature: impl Into<Bytes>) -> GroupProposal {
        GroupProposal {
            member_id: self.member_id,
            epoch: self.epoch,
            attempt: self.attempt,
            message_hash: self.message_hash,
            signature: signature.into(),
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
    pub signature: Bytes,
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
    fn serialize_versioned(
        &self,
        protocol_version: u32,
    ) -> Result<Vec<u8>, crate::codec::VersionedError> {
        crate::codec::serialize_v1(self, protocol_version)
    }

    fn deserialize_versioned(
        protocol_version: u32,
        bytes: &[u8],
    ) -> Result<Self, crate::codec::VersionedError> {
        crate::codec::deserialize_v1(protocol_version, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_proposal_roundtrip() {
        let p = UnsignedProposal::new(MemberId(1), Epoch(2), Attempt(3), [0xAA; 32]);
        let bytes = p.to_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn unsigned_for_sync_has_zero_hash() {
        let p = UnsignedProposal::for_sync(MemberId(1), Epoch(0), Attempt(0));
        assert_eq!(p.message_hash, [0u8; 32]);
    }

    #[test]
    fn group_proposal_is_sync() {
        let sync = UnsignedProposal::for_sync(MemberId(1), Epoch(0), Attempt(0))
            .with_signature(vec![1, 2, 3]);
        assert!(sync.is_sync());

        let non_sync = UnsignedProposal::new(MemberId(1), Epoch(0), Attempt(0), [1u8; 32])
            .with_signature(vec![1, 2, 3]);
        assert!(!non_sync.is_sync());
    }

    #[test]
    fn group_proposal_unsigned_roundtrip() {
        let gp = UnsignedProposal::new(MemberId(5), Epoch(10), Attempt(2), [0xBB; 32])
            .with_signature(vec![9, 8, 7]);
        let unsigned = gp.unsigned();
        assert_eq!(unsigned.member_id, MemberId(5));
        assert_eq!(unsigned.epoch, Epoch(10));
        assert_eq!(unsigned.attempt, Attempt(2));
        assert_eq!(unsigned.message_hash, [0xBB; 32]);
    }

    #[test]
    fn proposal_trait_impls() {
        let gp = UnsignedProposal::new(MemberId(1), Epoch(5), Attempt(3), [0; 32])
            .with_signature(vec![]);
        assert_eq!(gp.node_id(), MemberId(1));
        assert_eq!(gp.round(), Epoch(5));
        assert_eq!(gp.attempt(), Attempt(3));
        assert_eq!(GroupProposal::next_attempt(Attempt(3)), Attempt(4));
    }

    #[test]
    fn versioned_roundtrip() {
        let gp = UnsignedProposal::new(MemberId(1), Epoch(0), Attempt(0), [0; 32])
            .with_signature(vec![1, 2]);
        let bytes = gp.serialize_versioned(1).unwrap();
        let decoded = GroupProposal::deserialize_versioned(1, &bytes).unwrap();
        assert_eq!(decoded.member_id, MemberId(1));
    }

    #[test]
    fn versioned_unknown_version() {
        let gp = UnsignedProposal::new(MemberId(1), Epoch(0), Attempt(0), [0; 32])
            .with_signature(vec![]);
        assert!(gp.serialize_versioned(99).is_err());
        assert!(GroupProposal::deserialize_versioned(99, &[0]).is_err());
    }
}
