//! Custom MLS extensions and proposals for the sync protocol.
//!
//! All protocol-specific extensions share a single [`SYNC_EXTENSION_TYPE`].
//! Each MLS context (group context, key package, group info) uses a different
//! struct — [`GroupContextExt`], [`KeyPackageExt`], [`GroupInfoExt`] — but they
//! all return the same extension type. This works because each struct only
//! appears in its own extension list and never alongside the others.
//!
//! All custom proposals share a single [`SYNC_PROPOSAL_TYPE`] via the
//! [`SyncProposal`] enum.

use std::collections::BTreeMap;

use iroh::EndpointAddr;
use mls_rs::extension::{ExtensionType, MlsCodecExtension};
use mls_rs::group::proposal::MlsCustomProposal;
use mls_rs::mls_rs_codec::{self as mls_rs_codec, MlsDecode, MlsEncode, MlsSize};
use mls_rs_core::group::ProposalType;
use serde::{Deserialize, Serialize};

use crate::crdt::CompactionConfig;
use crate::proposal::AcceptorId;
use crate::protocol::MemberFingerprint;

/// Single extension type shared by all sync protocol extensions (private use range).
///
/// Each MLS context (group context, key package, group info) carries its own
/// struct but they all use this type ID. No collisions because the structs
/// live in separate extension lists.
pub const SYNC_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF796);

/// Single proposal type for all sync protocol custom proposals (private use range).
pub const SYNC_PROPOSAL_TYPE: ProposalType = ProposalType::new(0xF796);

/// Current protocol version for group context extensions.
pub const CURRENT_PROTOCOL_VERSION: u32 = 1;

/// Version 1 schema for the group context extension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupContextExtV1 {
    pub protocol_name: String,
    pub compaction_config: CompactionConfig,
    pub key_rotation_interval_secs: Option<u64>,
}

/// CRDT type, compaction config, and protocol version (group context extension).
///
/// Set at group creation. Joiners use `protocol_name` to select the right CRDT
/// implementation and `compaction_config` to drive hierarchical compaction.
///
/// The binary encoding uses a fixed 4-byte `u32` version prefix followed by
/// a version-specific postcard payload. The version number doubles as the
/// protocol version for all group-scoped messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupContextExt {
    pub protocol_version: u32,
    pub protocol_name: String,
    pub compaction_config: CompactionConfig,
    pub key_rotation_interval_secs: Option<u64>,
}

impl GroupContextExt {
    #[must_use]
    pub fn new(
        protocol_name: impl Into<String>,
        compaction_config: CompactionConfig,
        key_rotation_interval_secs: Option<u64>,
    ) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            protocol_name: protocol_name.into(),
            compaction_config,
            key_rotation_interval_secs,
        }
    }
}

impl From<GroupContextExtV1> for GroupContextExt {
    fn from(v1: GroupContextExtV1) -> Self {
        Self {
            protocol_version: 1,
            protocol_name: v1.protocol_name,
            compaction_config: v1.compaction_config,
            key_rotation_interval_secs: v1.key_rotation_interval_secs,
        }
    }
}

impl From<&GroupContextExt> for GroupContextExtV1 {
    fn from(ext: &GroupContextExt) -> Self {
        Self {
            protocol_name: ext.protocol_name.clone(),
            compaction_config: ext.compaction_config.clone(),
            key_rotation_interval_secs: ext.key_rotation_interval_secs,
        }
    }
}

impl MlsSize for GroupContextExt {
    fn mls_encoded_len(&self) -> usize {
        let v1 = GroupContextExtV1::from(self);
        let payload_len = postcard::to_allocvec(&v1).map_or(0, |v| v.len());
        // version (4) + payload
        4 + payload_len
    }
}

impl MlsEncode for GroupContextExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        let v1 = GroupContextExtV1::from(self);
        let payload =
            postcard::to_allocvec(&v1).map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        writer.extend_from_slice(&self.protocol_version.to_be_bytes());
        writer.extend_from_slice(&payload);
        Ok(())
    }
}

impl MlsDecode for GroupContextExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        if reader.len() < 4 {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }
        let version = u32::from_be_bytes([reader[0], reader[1], reader[2], reader[3]]);
        let payload = &reader[4..];
        *reader = &[];
        match version {
            1 => {
                let v1: GroupContextExtV1 = postcard::from_bytes(payload)
                    .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
                Ok(v1.into())
            }
            _ => Err(mls_rs_codec::Error::Custom(POSTCARD_ERROR)),
        }
    }
}

/// Extract the protocol version from raw extension data without fully parsing.
///
/// Reads the version from the first 4 bytes (big-endian `u32`).
///
/// # Errors
///
/// Returns an error if the data is too short.
pub fn read_protocol_version(extension_data: &[u8]) -> Result<u32, mls_rs_codec::Error> {
    if extension_data.len() < 4 {
        return Err(mls_rs_codec::Error::UnexpectedEOF);
    }
    Ok(u32::from_be_bytes([
        extension_data[0],
        extension_data[1],
        extension_data[2],
        extension_data[3],
    ]))
}

impl MlsCodecExtension for GroupContextExt {
    fn extension_type() -> ExtensionType {
        SYNC_EXTENSION_TYPE
    }
}

/// Member endpoint address, supported CRDTs, and supported protocol versions
/// (key package extension).
///
/// Included in key packages so the group leader can send the Welcome
/// directly, verify CRDT compatibility, and check protocol version support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyPackageExt {
    pub addr: EndpointAddr,
    pub supported_crdts: Vec<String>,
    pub supported_protocol_versions: Vec<u32>,
}

impl KeyPackageExt {
    #[must_use]
    pub fn new(
        addr: EndpointAddr,
        supported_crdts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            addr,
            supported_crdts: supported_crdts.into_iter().map(Into::into).collect(),
            supported_protocol_versions: vec![CURRENT_PROTOCOL_VERSION],
        }
    }

    #[must_use]
    pub fn supports(&self, type_id: &str) -> bool {
        self.supported_crdts.iter().any(|id| id == type_id)
    }

    #[must_use]
    pub fn supports_version(&self, version: u32) -> bool {
        self.supported_protocol_versions.contains(&version)
    }
}

impl MlsSize for KeyPackageExt {
    fn mls_encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for KeyPackageExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(self, writer)
    }
}

impl MlsDecode for KeyPackageExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        postcard_decode(reader)
    }
}

impl MlsCodecExtension for KeyPackageExt {
    fn extension_type() -> ExtensionType {
        SYNC_EXTENSION_TYPE
    }
}

/// Random binding ID for fingerprint collision resolution (leaf node extension).
///
/// Included in key packages' leaf node extensions so the binding ID persists
/// in the group roster. Used as additional input when computing
/// [`MemberFingerprint`](crate::MemberFingerprint).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeafNodeExt {
    pub binding_id: u64,
}

impl LeafNodeExt {
    #[must_use]
    pub fn random() -> Self {
        Self {
            binding_id: rand::random(),
        }
    }
}

impl MlsSize for LeafNodeExt {
    fn mls_encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for LeafNodeExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(self, writer)
    }
}

impl MlsDecode for LeafNodeExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        postcard_decode(reader)
    }
}

impl MlsCodecExtension for LeafNodeExt {
    fn extension_type() -> ExtensionType {
        SYNC_EXTENSION_TYPE
    }
}

/// Current acceptor list and CRDT snapshot (group info extension).
///
/// Sent in Welcome messages so joiners can discover acceptors and optionally
/// bootstrap their CRDT state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupInfoExt {
    pub acceptors: Vec<EndpointAddr>,
    pub snapshot: Vec<u8>,
}

impl GroupInfoExt {
    #[must_use]
    pub fn new(acceptors: impl IntoIterator<Item = EndpointAddr>, snapshot: Vec<u8>) -> Self {
        Self {
            acceptors: acceptors.into_iter().collect(),
            snapshot,
        }
    }

    #[must_use]
    pub fn acceptor_ids(&self) -> Vec<AcceptorId> {
        self.acceptors
            .iter()
            .map(|addr| AcceptorId::from_bytes(*addr.id.as_bytes()))
            .collect()
    }
}

impl MlsSize for GroupInfoExt {
    fn mls_encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for GroupInfoExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(self, writer)
    }
}

impl MlsDecode for GroupInfoExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        postcard_decode(reader)
    }
}

impl MlsCodecExtension for GroupInfoExt {
    fn extension_type() -> ExtensionType {
        SYNC_EXTENSION_TYPE
    }
}

// ---------------------------------------------------------------------------
// SyncProposal — unified custom proposal enum
// ---------------------------------------------------------------------------

/// Unified custom proposal for all sync protocol actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncProposal {
    AcceptorAdd(EndpointAddr),
    AcceptorRemove(AcceptorId),
    CompactionClaim {
        level: u8,
        watermark: BTreeMap<MemberFingerprint, u64>,
        deadline: u64,
    },
    CompactionComplete {
        level: u8,
        watermark: BTreeMap<MemberFingerprint, u64>,
    },
}

impl SyncProposal {
    #[must_use]
    pub fn acceptor_add(addr: EndpointAddr) -> Self {
        Self::AcceptorAdd(addr)
    }

    #[must_use]
    pub fn acceptor_remove(id: AcceptorId) -> Self {
        Self::AcceptorRemove(id)
    }

    #[must_use]
    pub fn compaction_claim(
        level: u8,
        watermark: BTreeMap<MemberFingerprint, u64>,
        deadline: u64,
    ) -> Self {
        Self::CompactionClaim {
            level,
            watermark,
            deadline,
        }
    }

    #[must_use]
    pub fn compaction_complete(level: u8, watermark: BTreeMap<MemberFingerprint, u64>) -> Self {
        Self::CompactionComplete { level, watermark }
    }
}

impl MlsSize for SyncProposal {
    fn mls_encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for SyncProposal {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(self, writer)
    }
}

impl MlsDecode for SyncProposal {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        postcard_decode(reader)
    }
}

impl MlsCustomProposal for SyncProposal {
    fn proposal_type() -> ProposalType {
        SYNC_PROPOSAL_TYPE
    }
}

impl crate::codec::Versioned for SyncProposal {
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

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

const POSTCARD_ERROR: u8 = 1;

#[expect(clippy::cast_possible_truncation)]
fn postcard_encode<T: Serialize>(
    value: &T,
    writer: &mut Vec<u8>,
) -> Result<(), mls_rs_codec::Error> {
    let bytes =
        postcard::to_allocvec(value).map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
    let len = bytes.len() as u32;
    writer.extend_from_slice(&len.to_be_bytes());
    writer.extend_from_slice(&bytes);
    Ok(())
}

fn postcard_decode<T: for<'de> Deserialize<'de>>(
    reader: &mut &[u8],
) -> Result<T, mls_rs_codec::Error> {
    let data = read_length_prefixed(reader)?;
    postcard::from_bytes(data).map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))
}

fn read_length_prefixed<'a>(reader: &mut &'a [u8]) -> Result<&'a [u8], mls_rs_codec::Error> {
    if reader.len() < 4 {
        return Err(mls_rs_codec::Error::UnexpectedEOF);
    }
    let len = u32::from_be_bytes([reader[0], reader[1], reader[2], reader[3]]) as usize;
    *reader = &reader[4..];

    if reader.len() < len {
        return Err(mls_rs_codec::Error::UnexpectedEOF);
    }
    let data = &reader[..len];
    *reader = &reader[len..];
    Ok(data)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;
    use crate::CompactionLevel;

    fn test_addr(seed: u8) -> EndpointAddr {
        let secret = SecretKey::from_bytes(&[seed; 32]);
        EndpointAddr::new(secret.public())
    }

    #[test]
    fn group_context_ext_roundtrip() {
        let config = vec![
            CompactionLevel {
                threshold: 0,
                replication: 1,
            },
            CompactionLevel {
                threshold: 5,
                replication: 0,
            },
        ];
        let ext = GroupContextExt::new("yjs", config.clone(), None);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupContextExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.protocol_name, "yjs");
        assert_eq!(decoded.compaction_config, config);
    }

    #[test]
    fn key_package_ext_roundtrip() {
        let addr = test_addr(42);
        let ext = KeyPackageExt::new(addr.clone(), ["yjs", "automerge"]);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = KeyPackageExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.addr, addr);
        assert!(decoded.supports("yjs"));
        assert!(decoded.supports("automerge"));
        assert!(!decoded.supports("unknown"));
    }

    #[test]
    fn group_info_ext_roundtrip() {
        let addrs = vec![test_addr(1), test_addr(2)];
        let ext = GroupInfoExt::new(addrs, b"crdt snapshot data".to_vec());

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupInfoExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.snapshot, b"crdt snapshot data");
        assert_eq!(decoded.acceptors.len(), 2);
    }

    #[test]
    fn group_info_ext_no_snapshot() {
        let ext = GroupInfoExt::new(Vec::<EndpointAddr>::new(), vec![]);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupInfoExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert!(decoded.snapshot.is_empty());
    }

    #[test]
    fn group_info_ext_acceptor_ids() {
        let addr1 = test_addr(1);
        let addr2 = test_addr(2);
        let expected_id1 = AcceptorId::from_bytes(*addr1.id.as_bytes());
        let expected_id2 = AcceptorId::from_bytes(*addr2.id.as_bytes());

        let ext = GroupInfoExt::new(vec![addr1, addr2], vec![]);
        let ids = ext.acceptor_ids();

        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], expected_id1);
        assert_eq!(ids[1], expected_id2);
    }

    #[test]
    fn sync_proposal_acceptor_add_roundtrip() {
        let addr = test_addr(42);
        let proposal = SyncProposal::acceptor_add(addr.clone());

        let custom = proposal.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), SYNC_PROPOSAL_TYPE);

        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
        assert!(matches!(decoded, SyncProposal::AcceptorAdd(a) if a == addr));
    }

    #[test]
    fn sync_proposal_acceptor_remove_roundtrip() {
        let id = AcceptorId::from_bytes([42u8; 32]);
        let proposal = SyncProposal::acceptor_remove(id);

        let custom = proposal.to_custom_proposal().unwrap();
        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_proposal_compaction_claim_roundtrip() {
        let mut watermark = BTreeMap::new();
        watermark.insert(MemberFingerprint([1u8; 8]), 42);
        watermark.insert(MemberFingerprint([2u8; 8]), 100);
        let proposal = SyncProposal::compaction_claim(1, watermark, 1_700_000_000);

        let custom = proposal.to_custom_proposal().unwrap();
        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_proposal_compaction_complete_roundtrip() {
        let mut watermark = BTreeMap::new();
        watermark.insert(MemberFingerprint([3u8; 8]), 99);
        let proposal = SyncProposal::compaction_complete(2, watermark);

        let custom = proposal.to_custom_proposal().unwrap();
        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_types_in_private_range() {
        assert!(SYNC_EXTENSION_TYPE.raw_value() >= 0xF000);
        assert!(SYNC_PROPOSAL_TYPE.raw_value() >= 0xF000);
    }

    #[test]
    fn group_context_ext_with_key_rotation_roundtrip() {
        let ext =
            GroupContextExt::new("test-crdt", crate::default_compaction_config(), Some(86400));
        assert_eq!(ext.key_rotation_interval_secs, Some(86400));

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupContextExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.key_rotation_interval_secs, Some(86400));
        assert_eq!(decoded.protocol_name, "test-crdt");
    }

    #[test]
    fn group_context_ext_key_rotation_disabled() {
        let ext = GroupContextExt::new("test", crate::default_compaction_config(), None);
        assert_eq!(ext.key_rotation_interval_secs, None);
    }

    #[test]
    fn group_context_ext_version_prefix() {
        let ext = GroupContextExt::new("yjs", crate::default_compaction_config(), None);
        let encoded = ext.mls_encode_to_vec().unwrap();

        // First 4 bytes = version (no length prefix)
        assert!(encoded.len() >= 4);
        let version = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(version, 1);
        assert_eq!(ext.protocol_version, 1);
    }

    #[test]
    fn group_context_ext_read_protocol_version() {
        let ext = GroupContextExt::new("yjs", crate::default_compaction_config(), None);
        let encoded = ext.mls_encode_to_vec().unwrap();
        let version = super::read_protocol_version(&encoded).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn group_context_ext_unknown_version_rejected() {
        let ext = GroupContextExt::new("yjs", crate::default_compaction_config(), None);
        let mut encoded = ext.mls_encode_to_vec().unwrap();

        // Corrupt the version to an unknown value (99)
        encoded[0] = 0;
        encoded[1] = 0;
        encoded[2] = 0;
        encoded[3] = 99;
        assert!(GroupContextExt::mls_decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn key_package_ext_version_support() {
        let addr = test_addr(42);
        let ext = KeyPackageExt::new(addr, ["yjs"]);

        assert!(ext.supports_version(1));
        assert!(!ext.supports_version(2));
        assert_eq!(
            ext.supported_protocol_versions,
            vec![CURRENT_PROTOCOL_VERSION]
        );
    }

    #[test]
    fn sync_proposal_versioned_roundtrip() {
        use crate::codec::Versioned;

        let addr = test_addr(42);
        let proposal = SyncProposal::acceptor_add(addr);

        let bytes = proposal.serialize_versioned(1).unwrap();
        let decoded = SyncProposal::deserialize_versioned(1, &bytes).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_proposal_versioned_unknown_version() {
        use crate::codec::Versioned;
        assert!(SyncProposal::deserialize_versioned(99, &[0]).is_err());
    }
}
