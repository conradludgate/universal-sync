//! Wire types for the client↔acceptor protocol: handshake, group messages,
//! and application message delivery.

use std::collections::BTreeMap;

use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

/// Group identifier (32 bytes, zero-padded from MLS group ID).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(pub [u8; 32]);

impl GroupId {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Pads with zeros or truncates to 32 bytes.
    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Self {
        let mut id = [0u8; 32];
        let len = bytes.len().min(32);
        id[..len].copy_from_slice(&bytes[..len]);
        Self(id)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for GroupId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for GroupId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Type of stream within a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamType {
    /// Paxos consensus for MLS commits.
    Proposals,
    /// Application message delivery.
    Messages,
}

/// First message on a bidirectional stream, identifying group and intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Handshake {
    /// Join an existing group's proposal stream.
    JoinProposals(GroupId),
    /// Register a new group with serialized `GroupInfo` bytes.
    CreateGroup(Vec<u8>),
    /// Join an existing group's message stream.
    JoinMessages(GroupId),
    /// Deliver a serialized MLS `Welcome` message.
    SendWelcome(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandshakeResponse {
    Ok,
    GroupNotFound,
    InvalidGroupInfo(String),
    Error(String),
}

/// Paxos payload wrapping an MLS message (commit or proposal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    #[serde(with = "mls_bytes")]
    pub mls_message: MlsMessage,
}

impl GroupMessage {
    #[must_use]
    pub fn new(mls_message: MlsMessage) -> Self {
        Self { mls_message }
    }
}

/// SHA-256 of the member's MLS signing public key. Stable across epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberFingerprint(pub [u8; 32]);

impl MemberFingerprint {
    #[must_use]
    pub fn from_signing_key(key: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(key);
        Self(hash.into())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl AsRef<[u8]> for MemberFingerprint {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Per-member high-water mark: "I have all messages from this sender up to this seq".
pub type StateVector = BTreeMap<MemberFingerprint, u64>;

/// Unique identifier for an application message: (group, sender fingerprint, seq).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    pub group_id: GroupId,
    pub sender: MemberFingerprint,
    pub seq: u64,
}

/// Encrypted application message (MLS `PrivateMessage` ciphertext).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EncryptedAppMessage {
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRequest {
    Send {
        id: MessageId,
        message: EncryptedAppMessage,
    },
    Subscribe {
        state_vector: StateVector,
    },
    Backfill {
        state_vector: StateVector,
        limit: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageResponse {
    Stored,
    Message {
        id: MessageId,
        message: EncryptedAppMessage,
    },
    BackfillComplete {
        has_more: bool,
    },
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_fingerprint_deterministic() {
        let key = b"some signing public key";
        let fp1 = MemberFingerprint::from_signing_key(key);
        let fp2 = MemberFingerprint::from_signing_key(key);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_different_keys() {
        let fp1 = MemberFingerprint::from_signing_key(b"key1");
        let fp2 = MemberFingerprint::from_signing_key(b"key2");
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_empty_key() {
        let fp = MemberFingerprint::from_signing_key(b"");
        // SHA-256 of empty input is a known value
        assert_ne!(fp.0, [0u8; 32]);
    }

    #[test]
    fn test_group_id_from_slice_exact() {
        let bytes = [42u8; 32];
        let id = GroupId::from_slice(&bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn test_group_id_from_slice_short() {
        let bytes = [1u8, 2, 3];
        let id = GroupId::from_slice(&bytes);
        let mut expected = [0u8; 32];
        expected[..3].copy_from_slice(&bytes);
        assert_eq!(id.as_bytes(), &expected);
    }

    #[test]
    fn test_group_id_from_slice_long() {
        let bytes = [99u8; 64];
        let id = GroupId::from_slice(&bytes);
        assert_eq!(id.as_bytes(), &[99u8; 32]);
    }

    #[test]
    fn test_group_id_from_slice_empty() {
        let id = GroupId::from_slice(&[]);
        assert_eq!(id.as_bytes(), &[0u8; 32]);
    }

    #[test]
    fn test_message_id_roundtrip() {
        let id = MessageId {
            group_id: GroupId::new([1u8; 32]),
            sender: MemberFingerprint([2u8; 32]),
            seq: 42,
        };
        let bytes = postcard::to_allocvec(&id).unwrap();
        let decoded: MessageId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_encrypted_app_message_roundtrip() {
        let msg = EncryptedAppMessage {
            ciphertext: vec![1, 2, 3, 4, 5],
        };
        let bytes = postcard::to_allocvec(&msg).unwrap();
        let decoded: EncryptedAppMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_message_request_variants_roundtrip() {
        let send = MessageRequest::Send {
            id: MessageId {
                group_id: GroupId::new([0u8; 32]),
                sender: MemberFingerprint([1u8; 32]),
                seq: 1,
            },
            message: EncryptedAppMessage {
                ciphertext: vec![10, 20],
            },
        };
        let bytes = postcard::to_allocvec(&send).unwrap();
        let decoded: MessageRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, MessageRequest::Send { .. }));

        let backfill = MessageRequest::Backfill {
            state_vector: Default::default(),
            limit: 100,
        };
        let bytes = postcard::to_allocvec(&backfill).unwrap();
        let decoded: MessageRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, MessageRequest::Backfill { limit: 100, .. }));
    }
}

mod mls_bytes {
    use mls_rs::mls_rs_codec::{MlsDecode, MlsEncode};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: MlsEncode, S: Serializer>(value: &T, ser: S) -> Result<S::Ok, S::Error> {
        let bytes = value
            .mls_encode_to_vec()
            .map_err(|e| serde::ser::Error::custom(format!("{e:?}")))?;
        bytes.serialize(ser)
    }

    pub fn deserialize<'de, T: MlsDecode, D: Deserializer<'de>>(de: D) -> Result<T, D::Error> {
        let bytes = Vec::<u8>::deserialize(de)?;
        T::mls_decode(&mut bytes.as_slice()).map_err(|e| serde::de::Error::custom(format!("{e:?}")))
    }
}
