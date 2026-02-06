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

/// CRDT type registration (group context extension).
///
/// Set at group creation. Joiners check this to select the right CRDT factory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupContextExt {
    pub crdt_type_id: String,
}

impl GroupContextExt {
    #[must_use]
    pub fn new(crdt_type_id: impl Into<String>) -> Self {
        Self {
            crdt_type_id: crdt_type_id.into(),
        }
    }
}

impl MlsSize for GroupContextExt {
    fn mls_encoded_len(&self) -> usize {
        postcard::to_allocvec(self).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for GroupContextExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(self, writer)
    }
}

impl MlsDecode for GroupContextExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        postcard_decode(reader)
    }
}

impl MlsCodecExtension for GroupContextExt {
    fn extension_type() -> ExtensionType {
        SYNC_EXTENSION_TYPE
    }
}

/// Member endpoint address and supported CRDTs (key package extension).
///
/// Included in key packages so the group leader can send the Welcome
/// directly and verify CRDT compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyPackageExt {
    pub addr: EndpointAddr,
    pub supported_crdts: Vec<String>,
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
        }
    }

    #[must_use]
    pub fn supports(&self, type_id: &str) -> bool {
        self.supported_crdts.iter().any(|id| id == type_id)
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
    pub fn new(
        acceptors: impl IntoIterator<Item = EndpointAddr>,
        snapshot: Vec<u8>,
    ) -> Self {
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

    fn test_addr(seed: u8) -> EndpointAddr {
        let secret = SecretKey::from_bytes(&[seed; 32]);
        EndpointAddr::new(secret.public())
    }

    #[test]
    fn group_context_ext_roundtrip() {
        let ext = GroupContextExt::new("yjs");

        let encoded = ext.mls_encode_to_vec().unwrap();
        let decoded = GroupContextExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.crdt_type_id, "yjs");
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
        watermark.insert(MemberFingerprint([1u8; 32]), 42);
        watermark.insert(MemberFingerprint([2u8; 32]), 100);
        let proposal = SyncProposal::compaction_claim(1, watermark, 1_700_000_000);

        let custom = proposal.to_custom_proposal().unwrap();
        let decoded = SyncProposal::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, proposal);
    }

    #[test]
    fn sync_proposal_compaction_complete_roundtrip() {
        let mut watermark = BTreeMap::new();
        watermark.insert(MemberFingerprint([3u8; 32]), 99);
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
}
