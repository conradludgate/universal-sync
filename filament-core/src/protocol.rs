//! Wire types for the client↔acceptor protocol: handshake, group messages,
//! and application message delivery.

use std::collections::BTreeMap;

use bytes::Bytes;
use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

use crate::Epoch;

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
    /// Join an existing group's proposal stream, replaying from `since_epoch`.
    JoinProposals {
        group_id: GroupId,
        since_epoch: Epoch,
    },
    /// Register a new group with a `GroupInfo` MLS message.
    CreateGroup {
        #[serde(with = "mls_bytes")]
        group_info: Box<MlsMessage>,
    },
    /// Join an existing group's message stream.
    ///
    /// Includes the subscriber's fingerprint so the acceptor can filter out
    /// messages originally sent by this member.
    JoinMessages(GroupId, MemberFingerprint),
    /// Deliver an MLS `Welcome` message.
    SendWelcome {
        #[serde(with = "mls_bytes")]
        welcome: Box<MlsMessage>,
    },
    /// Send a key package to request joining a group.
    ///
    /// The receiver uses `add_member` with the key package through the
    /// normal Paxos commit flow.
    SendKeyPackage {
        group_id: GroupId,
        #[serde(with = "mls_bytes")]
        key_package: Box<MlsMessage>,
        hmac_tag: [u8; 32],
    },
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

impl crate::codec::Versioned for GroupMessage {
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

/// Truncated SHA-256 of (`group_id` || `signing_key` || `binding_id`). Scoped per-group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberFingerprint(pub [u8; 8]);

impl MemberFingerprint {
    #[must_use]
    pub fn from_key(group_id: &GroupId, signing_key: &[u8], binding_id: u64) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(group_id.as_bytes());
        hasher.update(signing_key);
        hasher.update(binding_id.to_le_bytes());
        let hash = hasher.finalize();
        let mut fp = [0u8; 8];
        fp.copy_from_slice(&hash[..8]);
        Self(fp)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    /// Derive a [`ClientId`] from this fingerprint.
    #[must_use]
    pub fn as_client_id(&self) -> ClientId {
        // Mask to 32 bits: Yrs internally uses u32 client IDs despite
        // accepting u64 in its API. Values above 2^32 cause silent
        // encoding failures in V2 update format.
        ClientId(u64::from_le_bytes(self.0) & 0xFFFF_FFFF)
    }
}

impl AsRef<[u8]> for MemberFingerprint {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Stable per-member identifier derived from a [`MemberFingerprint`].
///
/// Masked to 32 bits for compatibility with Yrs V2 encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u64);

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
    pub ciphertext: Bytes,
}

/// Authenticated data carried alongside an MLS application message.
///
/// This is placed in the MLS `authenticated_data` field — authenticated but
/// not encrypted, so acceptors can inspect it without decrypting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthData {
    /// A regular L0 CRDT update.
    Update {
        /// Sender-scoped sequence number.
        seq: u64,
    },
    /// A compacted snapshot that supersedes earlier messages.
    Compaction {
        /// Sender-scoped sequence number (shared space with `Update`).
        seq: u64,
        /// Compaction level (1 = L0→L1, 2 = L1→L2, etc.).
        level: u8,
        /// State vector watermark: messages at or below these per-sender
        /// sequence numbers are superseded by this compaction.
        watermark: StateVector,
    },
}

impl AuthData {
    /// Create authenticated data for a regular update.
    #[must_use]
    pub fn update(seq: u64) -> Self {
        Self::Update { seq }
    }

    /// Create authenticated data for a compaction.
    #[must_use]
    pub fn compaction(seq: u64, level: u8, watermark: StateVector) -> Self {
        Self::Compaction {
            seq,
            level,
            watermark,
        }
    }

    /// Extract the sequence number regardless of variant.
    #[must_use]
    pub fn seq(&self) -> u64 {
        match self {
            Self::Update { seq } | Self::Compaction { seq, .. } => *seq,
        }
    }

    /// Encode to bytes for MLS `authenticated_data`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Decode from MLS `authenticated_data` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    /// Decode from MLS `authenticated_data` bytes using the given protocol version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unknown or deserialization fails.
    pub fn from_bytes_versioned(
        bytes: &[u8],
        protocol_version: u32,
    ) -> Result<Self, crate::codec::VersionedError> {
        Self::deserialize_versioned(protocol_version, bytes)
    }
}

use crate::codec::Versioned;

impl Versioned for AuthData {
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

impl Versioned for MessageRequest {
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

impl Versioned for MessageResponse {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRequest {
    Send {
        id: MessageId,
        level: u8,
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
    use mls_rs::ExtensionList;

    use super::*;

    #[test]
    fn test_member_fingerprint_deterministic() {
        let gid = GroupId::new([1u8; 32]);
        let key = b"some signing public key";
        let fp1 = MemberFingerprint::from_key(&gid, key, 0);
        let fp2 = MemberFingerprint::from_key(&gid, key, 0);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_different_keys() {
        let gid = GroupId::new([1u8; 32]);
        let fp1 = MemberFingerprint::from_key(&gid, b"key1", 0);
        let fp2 = MemberFingerprint::from_key(&gid, b"key2", 0);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_different_groups() {
        let gid1 = GroupId::new([1u8; 32]);
        let gid2 = GroupId::new([2u8; 32]);
        let fp1 = MemberFingerprint::from_key(&gid1, b"key", 0);
        let fp2 = MemberFingerprint::from_key(&gid2, b"key", 0);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_different_binding_ids() {
        let gid = GroupId::new([1u8; 32]);
        let fp1 = MemberFingerprint::from_key(&gid, b"key", 0);
        let fp2 = MemberFingerprint::from_key(&gid, b"key", 1);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_member_fingerprint_empty_key() {
        let gid = GroupId::new([1u8; 32]);
        let fp = MemberFingerprint::from_key(&gid, b"", 0);
        assert_ne!(fp.0, [0u8; 8]);
    }

    #[test]
    fn test_member_fingerprint_as_client_id() {
        let gid = GroupId::new([1u8; 32]);
        let fp = MemberFingerprint::from_key(&gid, b"key", 42);
        let cid = fp.as_client_id();
        assert_eq!(cid.0 & !0xFFFF_FFFFu64, 0, "must fit in 32 bits");
        assert_eq!(cid.0, u64::from_le_bytes(fp.0) & 0xFFFF_FFFF);
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
            sender: MemberFingerprint([2u8; 8]),
            seq: 42,
        };
        let bytes = postcard::to_allocvec(&id).unwrap();
        let decoded: MessageId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_encrypted_app_message_roundtrip() {
        let msg = EncryptedAppMessage {
            ciphertext: Bytes::from_static(&[1, 2, 3, 4, 5]),
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
                sender: MemberFingerprint([1u8; 8]),
                seq: 1,
            },
            level: 0,
            message: EncryptedAppMessage {
                ciphertext: Bytes::from_static(&[10, 20]),
            },
        };
        let bytes = postcard::to_allocvec(&send).unwrap();
        let decoded: MessageRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, MessageRequest::Send { .. }));

        let backfill = MessageRequest::Backfill {
            state_vector: BTreeMap::default(),
            limit: 100,
        };
        let bytes = postcard::to_allocvec(&backfill).unwrap();
        let decoded: MessageRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            MessageRequest::Backfill { limit: 100, .. }
        ));
    }

    #[test]
    fn auth_data_update_roundtrip() {
        let ad = AuthData::update(42);
        let bytes = ad.to_bytes().unwrap();
        let decoded = AuthData::from_bytes(&bytes).unwrap();
        assert_eq!(ad, decoded);
        assert_eq!(decoded.seq(), 42);
    }

    #[test]
    fn auth_data_compaction_roundtrip() {
        let mut watermark = StateVector::new();
        watermark.insert(MemberFingerprint([0xAA; 8]), 100);
        watermark.insert(MemberFingerprint([0xBB; 8]), 200);

        let ad = AuthData::compaction(50, 1, watermark.clone());
        let bytes = ad.to_bytes().unwrap();
        let decoded = AuthData::from_bytes(&bytes).unwrap();
        assert_eq!(ad, decoded);
        assert_eq!(decoded.seq(), 50);

        match decoded {
            AuthData::Compaction {
                level,
                watermark: wm,
                ..
            } => {
                assert_eq!(level, 1);
                assert_eq!(wm, watermark);
            }
            AuthData::Update { .. } => panic!("expected Compaction"),
        }
    }

    #[test]
    fn auth_data_seq_accessor() {
        assert_eq!(AuthData::update(7).seq(), 7);
        assert_eq!(AuthData::compaction(99, 2, StateVector::new()).seq(), 99);
    }

    #[test]
    fn handshake_join_proposals_round_trip() {
        use crate::Epoch;
        let group_id = GroupId::new([1u8; 32]);
        let handshake = Handshake::JoinProposals {
            group_id,
            since_epoch: Epoch(5),
        };
        let bytes = postcard::to_allocvec(&handshake).unwrap();
        let decoded: Handshake = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Handshake::JoinProposals { since_epoch, .. } => assert_eq!(since_epoch, Epoch(5)),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn group_id_from_array() {
        let bytes = [7u8; 32];
        let id: GroupId = bytes.into();
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn group_id_as_ref() {
        let id = GroupId::new([3u8; 32]);
        let r: &[u8] = id.as_ref();
        assert_eq!(r, &[3u8; 32]);
    }

    #[test]
    fn member_fingerprint_as_ref() {
        let fp = MemberFingerprint([0xAB; 8]);
        let r: &[u8] = fp.as_ref();
        assert_eq!(r, &[0xAB; 8]);
        assert_eq!(fp.as_bytes(), &[0xAB; 8]);
    }

    #[test]
    fn auth_data_from_bytes_versioned() {
        let ad = AuthData::update(10);
        let bytes = ad.serialize_versioned(1).unwrap();
        let decoded = AuthData::from_bytes_versioned(&bytes, 1).unwrap();
        assert_eq!(decoded.seq(), 10);
        assert!(AuthData::from_bytes_versioned(&bytes, 99).is_err());
    }

    #[test]
    fn message_request_versioned_roundtrip() {
        let req = MessageRequest::Subscribe {
            state_vector: BTreeMap::new(),
        };
        let bytes = req.serialize_versioned(1).unwrap();
        let decoded = MessageRequest::deserialize_versioned(1, &bytes).unwrap();
        assert!(matches!(decoded, MessageRequest::Subscribe { .. }));
        assert!(MessageRequest::deserialize_versioned(99, &bytes).is_err());
    }

    #[test]
    fn message_response_versioned_roundtrip() {
        let resp = MessageResponse::Stored;
        let bytes = resp.serialize_versioned(1).unwrap();
        let decoded = MessageResponse::deserialize_versioned(1, &bytes).unwrap();
        assert!(matches!(decoded, MessageResponse::Stored));
        assert!(MessageResponse::deserialize_versioned(99, &bytes).is_err());
    }

    #[test]
    fn message_response_versioned_unknown_serialize() {
        let resp = MessageResponse::Stored;
        assert!(resp.serialize_versioned(99).is_err());
        let req = MessageRequest::Subscribe {
            state_vector: BTreeMap::new(),
        };
        assert!(req.serialize_versioned(99).is_err());
    }

    #[test]
    fn handshake_response_roundtrip() {
        for variant in [
            HandshakeResponse::Ok,
            HandshakeResponse::GroupNotFound,
            HandshakeResponse::InvalidGroupInfo("bad".into()),
            HandshakeResponse::Error("err".into()),
        ] {
            let bytes = postcard::to_allocvec(&variant).unwrap();
            let _decoded: HandshakeResponse = postcard::from_bytes(&bytes).unwrap();
        }
    }

    fn test_mls_message() -> MlsMessage {
        use mls_rs::identity::SigningIdentity;
        use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
        use mls_rs::{CipherSuite, CipherSuiteProvider, CryptoProvider};
        use mls_rs_crypto_rustcrypto::RustCryptoProvider;

        let crypto = RustCryptoProvider::default();
        let cs = crypto
            .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
            .unwrap();
        let (sk, pk) = cs.signature_key_generate().unwrap();
        let cred = BasicCredential::new(b"test".to_vec());
        let id = SigningIdentity::new(cred.into_credential(), pk);

        let client = mls_rs::Client::builder()
            .crypto_provider(crypto)
            .identity_provider(BasicIdentityProvider::new())
            .signing_identity(id, sk, CipherSuite::CURVE25519_AES128)
            .build();

        client
            .generate_key_package_message(ExtensionList::default(), ExtensionList::default(), None)
            .unwrap()
    }

    #[test]
    fn handshake_variants_roundtrip() {
        let msg = Box::new(test_mls_message());
        let variants: Vec<Handshake> = vec![
            Handshake::JoinProposals {
                group_id: GroupId::new([0; 32]),
                since_epoch: Epoch(1),
            },
            Handshake::CreateGroup {
                group_info: msg.clone(),
            },
            Handshake::JoinMessages(GroupId::new([0; 32]), MemberFingerprint([1; 8])),
            Handshake::SendWelcome {
                welcome: msg.clone(),
            },
            Handshake::SendKeyPackage {
                group_id: GroupId::new([7; 32]),
                key_package: msg,
                hmac_tag: [0xAB; 32],
            },
        ];
        for h in variants {
            let bytes = postcard::to_allocvec(&h).unwrap();
            let _decoded: Handshake = postcard::from_bytes(&bytes).unwrap();
        }
    }

    #[test]
    fn stream_type_roundtrip() {
        for st in [StreamType::Proposals, StreamType::Messages] {
            let bytes = postcard::to_allocvec(&st).unwrap();
            let decoded: StreamType = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(decoded, st);
        }
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
