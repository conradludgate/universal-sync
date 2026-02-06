//! Epoch roster snapshots for signature validation.
//!
//! Member IDs (leaf indices) are not stable across epochs, so we store
//! roster snapshots to look up the correct public key for `(epoch, member_id)`.

use serde::{Deserialize, Serialize};
use universal_sync_core::{Epoch, MemberId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EpochRoster {
    pub(crate) epoch: Epoch,
    pub(crate) members: Vec<(MemberId, Vec<u8>)>,
}

impl EpochRoster {
    pub(crate) fn new(
        epoch: Epoch,
        members: impl IntoIterator<Item = (MemberId, Vec<u8>)>,
    ) -> Self {
        Self {
            epoch,
            members: members.into_iter().collect(),
        }
    }

    #[must_use]
    pub(crate) fn get_member_key(&self, member_id: MemberId) -> Option<&[u8]> {
        self.members
            .iter()
            .find(|(id, _)| *id == member_id)
            .map(|(_, key)| key.as_slice())
    }

    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

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
