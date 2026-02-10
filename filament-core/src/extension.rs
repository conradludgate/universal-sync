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

use mls_rs::extension::{ExtensionType, MlsCodecExtension};
use mls_rs::group::proposal::MlsCustomProposal;
use mls_rs::mls_rs_codec::{self as mls_rs_codec, MlsDecode, MlsEncode, MlsSize};
use mls_rs_core::group::ProposalType;
use serde::{Deserialize, Serialize};

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
    pub key_rotation_interval_secs: Option<u64>,
}

/// CRDT type and protocol version (group context extension).
///
/// Set at group creation. Joiners use `protocol_name` to select the right CRDT
/// implementation. Compaction uses a hardcoded base threshold.
///
/// The binary encoding uses a fixed 4-byte `u32` version prefix followed by
/// a version-specific postcard payload. The version number doubles as the
/// protocol version for all group-scoped messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupContextExt {
    pub protocol_version: u32,
    pub protocol_name: String,
    pub key_rotation_interval_secs: Option<u64>,
}

impl GroupContextExt {
    #[must_use]
    pub fn new(protocol_name: impl Into<String>, key_rotation_interval_secs: Option<u64>) -> Self {
        Self {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            protocol_name: protocol_name.into(),
            key_rotation_interval_secs,
        }
    }
}

impl From<GroupContextExtV1> for GroupContextExt {
    fn from(v1: GroupContextExtV1) -> Self {
        Self {
            protocol_version: 1,
            protocol_name: v1.protocol_name,
            key_rotation_interval_secs: v1.key_rotation_interval_secs,
        }
    }
}

impl From<&GroupContextExt> for GroupContextExtV1 {
    fn from(ext: &GroupContextExt) -> Self {
        Self {
            protocol_name: ext.protocol_name.clone(),
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

/// Member endpoint identity, supported CRDTs, and supported protocol versions
/// (key package extension).
///
/// Included in key packages so the group leader can send the Welcome
/// directly via iroh discovery, verify CRDT compatibility, and check protocol
/// version support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyPackageExt {
    pub endpoint_id: [u8; 32],
    pub supported_crdts: Vec<String>,
    pub supported_protocol_versions: Vec<u32>,
}

impl KeyPackageExt {
    #[must_use]
    pub fn new(
        endpoint_id: [u8; 32],
        supported_crdts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            endpoint_id,
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

/// Current acceptor list (group info extension).
///
/// Sent in Welcome messages so joiners can discover acceptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupInfoExt {
    pub acceptors: Vec<AcceptorId>,
}

impl GroupInfoExt {
    #[must_use]
    pub fn new(acceptors: impl IntoIterator<Item = AcceptorId>) -> Self {
        Self {
            acceptors: acceptors.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn acceptor_ids(&self) -> &[AcceptorId] {
        &self.acceptors
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
    AcceptorAdd(AcceptorId),
    AcceptorRemove(AcceptorId),
    CompactionComplete {
        level: u8,
        watermark: BTreeMap<MemberFingerprint, u64>,
    },
}

impl SyncProposal {
    #[must_use]
    pub fn acceptor_add(id: AcceptorId) -> Self {
        Self::AcceptorAdd(id)
    }

    #[must_use]
    pub fn acceptor_remove(id: AcceptorId) -> Self {
        Self::AcceptorRemove(id)
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
    use super::*;

    fn test_acceptor_id(seed: u8) -> AcceptorId {
        AcceptorId::from_bytes([seed; 32])
    }

    fn test_endpoint_id(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    #[test]
    fn group_context_ext_roundtrip() {
        let ext = GroupContextExt::new("yjs", None);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupContextExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.protocol_name, "yjs");
    }

    #[test]
    fn key_package_ext_roundtrip() {
        let eid = test_endpoint_id(42);
        let ext = KeyPackageExt::new(eid, ["yjs", "automerge"]);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = KeyPackageExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.endpoint_id, eid);
        assert!(decoded.supports("yjs"));
        assert!(decoded.supports("automerge"));
        assert!(!decoded.supports("unknown"));
    }

    #[test]
    fn group_info_ext_roundtrip() {
        let ids = vec![test_acceptor_id(1), test_acceptor_id(2)];
        let ext = GroupInfoExt::new(ids);

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupInfoExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.acceptors.len(), 2);
    }

    #[test]
    fn group_info_ext_empty() {
        let ext = GroupInfoExt::new(Vec::<AcceptorId>::new());

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupInfoExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
    }

    #[test]
    fn group_info_ext_acceptor_ids() {
        let id1 = test_acceptor_id(1);
        let id2 = test_acceptor_id(2);

        let ext = GroupInfoExt::new(vec![id1, id2]);
        let ids = ext.acceptor_ids();

        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], id1);
        assert_eq!(ids[1], id2);
    }

    #[test]
    fn sync_proposal_acceptor_add_roundtrip() {
        let id = test_acceptor_id(42);
        let proposal = SyncProposal::acceptor_add(id);

        let custom = proposal.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), SYNC_PROPOSAL_TYPE);

        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
        assert!(matches!(decoded, SyncProposal::AcceptorAdd(a) if a == id));
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
        let ext = GroupContextExt::new("test-crdt", Some(86400));
        assert_eq!(ext.key_rotation_interval_secs, Some(86400));

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupContextExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.key_rotation_interval_secs, Some(86400));
        assert_eq!(decoded.protocol_name, "test-crdt");
    }

    #[test]
    fn group_context_ext_key_rotation_disabled() {
        let ext = GroupContextExt::new("test", None);
        assert_eq!(ext.key_rotation_interval_secs, None);
    }

    #[test]
    fn group_context_ext_version_prefix() {
        let ext = GroupContextExt::new("yjs", None);
        let encoded = ext.mls_encode_to_vec().unwrap();

        assert!(encoded.len() >= 4);
        let version = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(version, 1);
        assert_eq!(ext.protocol_version, 1);
    }

    #[test]
    fn group_context_ext_read_protocol_version() {
        let ext = GroupContextExt::new("yjs", None);
        let encoded = ext.mls_encode_to_vec().unwrap();
        let version = super::read_protocol_version(&encoded).unwrap();
        assert_eq!(version, 1);
    }

    #[test]
    fn group_context_ext_unknown_version_rejected() {
        let ext = GroupContextExt::new("yjs", None);
        let mut encoded = ext.mls_encode_to_vec().unwrap();

        encoded[0] = 0;
        encoded[1] = 0;
        encoded[2] = 0;
        encoded[3] = 99;
        assert!(GroupContextExt::mls_decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn key_package_ext_version_support() {
        let eid = test_endpoint_id(42);
        let ext = KeyPackageExt::new(eid, ["yjs"]);

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

        let id = test_acceptor_id(42);
        let proposal = SyncProposal::acceptor_add(id);

        let bytes = proposal.serialize_versioned(1).unwrap();
        let decoded = SyncProposal::deserialize_versioned(1, &bytes).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_proposal_versioned_unknown_version() {
        use crate::codec::Versioned;
        assert!(SyncProposal::deserialize_versioned(99, &[0]).is_err());
    }

    #[test]
    fn read_protocol_version_short_input() {
        assert!(super::read_protocol_version(&[]).is_err());
        assert!(super::read_protocol_version(&[1]).is_err());
        assert!(super::read_protocol_version(&[1, 2]).is_err());
        assert!(super::read_protocol_version(&[1, 2, 3]).is_err());
    }

    #[test]
    fn group_context_ext_decode_short_input() {
        assert!(GroupContextExt::mls_decode(&mut [1u8, 2].as_slice()).is_err());
    }
}
