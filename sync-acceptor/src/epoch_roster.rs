//! Epoch roster snapshots for signature validation
//!
//! When validating proposal signatures, we need to look up the member's public key
//! using the roster from the epoch when the proposal was created - not the current epoch.
//!
//! Member IDs (leaf indices) are not stable across epochs: when a member is removed,
//! the indices of members after them may shift. This means a proposal signed by
//! "member 5 in epoch 10" might refer to a different person than "member 5 in epoch 11".
//!
//! This module provides storage for epoch roster snapshots, allowing us to look up
//! the correct public key for `(epoch, member_id)` pairs.

use serde::{Deserialize, Serialize};
use universal_sync_core::{Epoch, MemberId};

/// A snapshot of a group's roster at a specific epoch.
///
/// Contains the public signing key for each member, indexed by their MLS leaf index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EpochRoster {
    /// The epoch this roster is for
    pub(crate) epoch: Epoch,

    /// Member public keys indexed by member ID (leaf index)
    ///
    /// The key is serialized as bytes for storage efficiency.
    /// Format: `Vec<(member_id, public_key_bytes)>`
    pub(crate) members: Vec<(MemberId, Vec<u8>)>,
}

impl EpochRoster {
    /// Create a new epoch roster snapshot
    pub(crate) fn new(
        epoch: Epoch,
        members: impl IntoIterator<Item = (MemberId, Vec<u8>)>,
    ) -> Self {
        Self {
            epoch,
            members: members.into_iter().collect(),
        }
    }

    /// Get a member's public key by ID
    #[must_use]
    pub(crate) fn get_member_key(&self, member_id: MemberId) -> Option<&[u8]> {
        self.members
            .iter()
            .find(|(id, _)| *id == member_id)
            .map(|(_, key)| key.as_slice())
    }

    /// Serialize to bytes for storage
    ///
    /// # Panics
    /// Panics if serialization fails (should not happen with valid data).
    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

    /// Deserialize from bytes
    #[must_use]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_roster_roundtrip() {
        let roster = EpochRoster::new(
            Epoch(5),
            vec![
                (MemberId(0), vec![1, 2, 3, 4]),
                (MemberId(1), vec![5, 6, 7, 8]),
            ],
        );

        let bytes = roster.to_bytes();
        let decoded = EpochRoster::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.epoch, Epoch(5));
        assert_eq!(decoded.get_member_key(MemberId(0)), Some(&[1, 2, 3, 4][..]));
        assert_eq!(decoded.get_member_key(MemberId(1)), Some(&[5, 6, 7, 8][..]));
        assert_eq!(decoded.get_member_key(MemberId(2)), None);
    }
}
