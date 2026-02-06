//! Custom MLS group context and key package extensions for the sync protocol.

use std::collections::BTreeMap;

use iroh::EndpointAddr;
use mls_rs::extension::{ExtensionType, MlsCodecExtension};
use mls_rs::group::proposal::MlsCustomProposal;
use mls_rs::mls_rs_codec::{self as mls_rs_codec, MlsDecode, MlsEncode, MlsSize};
use mls_rs_core::group::ProposalType;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::proposal::AcceptorId;
use crate::protocol::MemberFingerprint;

/// Extension type for the full acceptor list (group context extension)
pub const ACCEPTORS_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF795);

/// Proposal type for adding a federated acceptor (private use range)
pub const ACCEPTOR_ADD_PROPOSAL_TYPE: ProposalType = ProposalType::new(0xF796);

/// Proposal type for removing a federated acceptor (private use range)
pub const ACCEPTOR_REMOVE_PROPOSAL_TYPE: ProposalType = ProposalType::new(0xF797);

/// Extension type for member's endpoint address (key package extension)
pub const MEMBER_ADDR_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF798);

/// Extension type for CRDT registration (group context extension)
pub const CRDT_REGISTRATION_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF799);

/// Extension type for supported CRDTs list (key package extension)
pub const SUPPORTED_CRDTS_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF79A);

/// Extension type for CRDT snapshot (group info extension, sent in welcome)
pub const CRDT_SNAPSHOT_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF79B);

/// Proposal type for compaction claim (private use range)
pub const COMPACTION_CLAIM_PROPOSAL_TYPE: ProposalType = ProposalType::new(0xF7A0);

/// Proposal type for compaction completion (private use range)
pub const COMPACTION_COMPLETE_PROPOSAL_TYPE: ProposalType = ProposalType::new(0xF7A1);

const POSTCARD_ERROR: u8 = 1;

fn postcard_encoded_len<T: Serialize>(value: &T) -> usize {
    postcard::to_allocvec(value).map_or(4, |v| 4 + v.len())
}

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

fn postcard_decode<T: DeserializeOwned>(reader: &mut &[u8]) -> Result<T, mls_rs_codec::Error> {
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

/// Full list of acceptor endpoint addresses (group context extension).
///
/// Set at group creation and included in Welcome messages so joiners
/// can discover the acceptor set.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AcceptorsExt(pub Vec<EndpointAddr>);

impl AcceptorsExt {
    pub fn new(acceptors: impl IntoIterator<Item = EndpointAddr>) -> Self {
        Self(acceptors.into_iter().collect())
    }

    #[must_use]
    pub fn acceptors(&self) -> &[EndpointAddr] {
        &self.0
    }

    #[must_use]
    pub fn acceptor_ids(&self) -> Vec<AcceptorId> {
        self.0
            .iter()
            .map(|addr| AcceptorId::from_bytes(*addr.id.as_bytes()))
            .collect()
    }
}

impl MlsSize for AcceptorsExt {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&self.0)
    }
}

impl MlsEncode for AcceptorsExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&self.0, writer)
    }
}

impl MlsDecode for AcceptorsExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        Ok(Self(postcard_decode(reader)?))
    }
}

impl MlsCodecExtension for AcceptorsExt {
    fn extension_type() -> ExtensionType {
        ACCEPTORS_EXTENSION_TYPE
    }
}

/// Signal to add a federated acceptor (MLS custom proposal, unencrypted but signed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorAdd(pub EndpointAddr);

impl AcceptorAdd {
    #[must_use]
    pub fn new(addr: EndpointAddr) -> Self {
        Self(addr)
    }

    #[must_use]
    pub fn addr(&self) -> &EndpointAddr {
        &self.0
    }

    #[must_use]
    pub fn acceptor_id(&self) -> AcceptorId {
        AcceptorId::from_bytes(*self.0.id.as_bytes())
    }
}

impl MlsSize for AcceptorAdd {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&self.0)
    }
}

impl MlsEncode for AcceptorAdd {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&self.0, writer)
    }
}

impl MlsDecode for AcceptorAdd {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        Ok(Self(postcard_decode(reader)?))
    }
}

impl MlsCustomProposal for AcceptorAdd {
    fn proposal_type() -> ProposalType {
        ACCEPTOR_ADD_PROPOSAL_TYPE
    }
}

/// Signal to remove a federated acceptor (MLS custom proposal, unencrypted but signed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorRemove(pub AcceptorId);

impl AcceptorRemove {
    #[must_use]
    pub fn new(acceptor_id: AcceptorId) -> Self {
        Self(acceptor_id)
    }

    #[must_use]
    pub fn acceptor_id(&self) -> AcceptorId {
        self.0
    }
}

impl MlsSize for AcceptorRemove {
    fn mls_encoded_len(&self) -> usize {
        32 // AcceptorId is always 32 bytes
    }
}

impl MlsEncode for AcceptorRemove {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        writer.extend_from_slice(&self.0 .0);
        Ok(())
    }
}

impl MlsDecode for AcceptorRemove {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        if reader.len() < 32 {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&reader[..32]);
        *reader = &reader[32..];
        Ok(Self(AcceptorId(bytes)))
    }
}

impl MlsCustomProposal for AcceptorRemove {
    fn proposal_type() -> ProposalType {
        ACCEPTOR_REMOVE_PROPOSAL_TYPE
    }
}

/// Member's endpoint address (key package extension).
///
/// Allows the group leader to send the Welcome message directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAddrExt(pub EndpointAddr);

impl MemberAddrExt {
    #[must_use]
    pub fn new(addr: EndpointAddr) -> Self {
        Self(addr)
    }

    #[must_use]
    pub fn addr(&self) -> &EndpointAddr {
        &self.0
    }
}

impl MlsSize for MemberAddrExt {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&self.0)
    }
}

impl MlsEncode for MemberAddrExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&self.0, writer)
    }
}

impl MlsDecode for MemberAddrExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        Ok(Self(postcard_decode(reader)?))
    }
}

impl MlsCodecExtension for MemberAddrExt {
    fn extension_type() -> ExtensionType {
        MEMBER_ADDR_EXTENSION_TYPE
    }
}

/// CRDT type registration (group context extension).
///
/// Joiners check this to ensure they have a compatible CRDT factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtRegistrationExt {
    pub type_id: String,
}

impl CrdtRegistrationExt {
    #[must_use]
    pub fn new(type_id: impl Into<String>) -> Self {
        Self {
            type_id: type_id.into(),
        }
    }

    #[must_use]
    pub fn type_id(&self) -> &str {
        &self.type_id
    }
}

impl MlsSize for CrdtRegistrationExt {
    fn mls_encoded_len(&self) -> usize {
        4 + self.type_id.len()
    }
}

impl MlsEncode for CrdtRegistrationExt {
    #[expect(clippy::cast_possible_truncation)]
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        let bytes = self.type_id.as_bytes();
        let len = bytes.len() as u32;
        writer.extend_from_slice(&len.to_be_bytes());
        writer.extend_from_slice(bytes);
        Ok(())
    }
}

impl MlsDecode for CrdtRegistrationExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        let data = read_length_prefixed(reader)?;
        let type_id = String::from_utf8(data.to_vec())
            .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        Ok(Self { type_id })
    }
}

impl MlsCodecExtension for CrdtRegistrationExt {
    fn extension_type() -> ExtensionType {
        CRDT_REGISTRATION_EXTENSION_TYPE
    }
}

/// Supported CRDT types (key package extension).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SupportedCrdtsExt {
    pub type_ids: Vec<String>,
}

impl SupportedCrdtsExt {
    #[must_use]
    pub fn new(type_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            type_ids: type_ids.into_iter().map(Into::into).collect(),
        }
    }

    #[must_use]
    pub fn supports(&self, type_id: &str) -> bool {
        self.type_ids.iter().any(|id| id == type_id)
    }
}

impl MlsSize for SupportedCrdtsExt {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&self.type_ids)
    }
}

impl MlsEncode for SupportedCrdtsExt {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&self.type_ids, writer)
    }
}

impl MlsDecode for SupportedCrdtsExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        Ok(Self {
            type_ids: postcard_decode(reader)?,
        })
    }
}

impl MlsCodecExtension for SupportedCrdtsExt {
    fn extension_type() -> ExtensionType {
        SUPPORTED_CRDTS_EXTENSION_TYPE
    }
}

/// CRDT state snapshot (group info extension, sent in Welcome).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtSnapshotExt(pub Vec<u8>);

impl CrdtSnapshotExt {
    #[must_use]
    pub fn new(snapshot: Vec<u8>) -> Self {
        Self(snapshot)
    }

    #[must_use]
    pub fn snapshot(&self) -> &[u8] {
        &self.0
    }
}

impl MlsSize for CrdtSnapshotExt {
    fn mls_encoded_len(&self) -> usize {
        4 + self.0.len()
    }
}

impl MlsEncode for CrdtSnapshotExt {
    #[expect(clippy::cast_possible_truncation)]
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        let len = self.0.len() as u32;
        writer.extend_from_slice(&len.to_be_bytes());
        writer.extend_from_slice(&self.0);
        Ok(())
    }
}

impl MlsDecode for CrdtSnapshotExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        let data = read_length_prefixed(reader)?;
        Ok(Self(data.to_vec()))
    }
}

impl MlsCodecExtension for CrdtSnapshotExt {
    fn extension_type() -> ExtensionType {
        CRDT_SNAPSHOT_EXTENSION_TYPE
    }
}

/// Compaction claim proposal (MLS custom proposal, unencrypted but signed).
///
/// A member broadcasts this to claim ownership of a compaction range. If the
/// commit is accepted, other members back off. The claimer must complete
/// within `deadline` (unix seconds) or the claim expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionClaim {
    pub level: u8,
    pub watermark: BTreeMap<MemberFingerprint, u64>,
    pub deadline: u64,
}

impl MlsSize for CompactionClaim {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&(self.level, &self.watermark, self.deadline))
    }
}

impl MlsEncode for CompactionClaim {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&(self.level, &self.watermark, self.deadline), writer)
    }
}

impl MlsDecode for CompactionClaim {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        let (level, watermark, deadline): (u8, BTreeMap<MemberFingerprint, u64>, u64) =
            postcard_decode(reader)?;
        Ok(Self {
            level,
            watermark,
            deadline,
        })
    }
}

impl MlsCustomProposal for CompactionClaim {
    fn proposal_type() -> ProposalType {
        COMPACTION_CLAIM_PROPOSAL_TYPE
    }
}

/// Compaction completion proposal (MLS custom proposal, unencrypted but signed).
///
/// Signals that the claimer has stored the compacted entry. Acceptors use
/// the watermark to delete messages covered by the compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionComplete {
    pub level: u8,
    pub watermark: BTreeMap<MemberFingerprint, u64>,
}

impl MlsSize for CompactionComplete {
    fn mls_encoded_len(&self) -> usize {
        postcard_encoded_len(&(self.level, &self.watermark))
    }
}

impl MlsEncode for CompactionComplete {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        postcard_encode(&(self.level, &self.watermark), writer)
    }
}

impl MlsDecode for CompactionComplete {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        let (level, watermark): (u8, BTreeMap<MemberFingerprint, u64>) = postcard_decode(reader)?;
        Ok(Self { level, watermark })
    }
}

impl MlsCustomProposal for CompactionComplete {
    fn proposal_type() -> ProposalType {
        COMPACTION_COMPLETE_PROPOSAL_TYPE
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn test_addr(seed: u8) -> EndpointAddr {
        // Generate a valid public key from a seeded secret key
        let secret = SecretKey::from_bytes(&[seed; 32]);
        EndpointAddr::new(secret.public())
    }

    #[test]
    fn test_acceptors_ext_roundtrip() {
        let addrs = vec![test_addr(1), test_addr(2), test_addr(3)];
        let ext = AcceptorsExt::new(addrs.clone());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = AcceptorsExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.acceptors().len(), 3);
    }

    #[test]
    fn test_acceptors_ext_empty() {
        let ext = AcceptorsExt::default();
        assert!(ext.acceptors().is_empty());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = AcceptorsExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
    }

    #[test]
    fn test_acceptor_add_roundtrip() {
        let addr = test_addr(42);
        let add = AcceptorAdd::new(addr.clone());

        let custom = add.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), ACCEPTOR_ADD_PROPOSAL_TYPE);

        let decoded = AcceptorAdd::from_custom_proposal(&custom).unwrap();
        assert_eq!(add, decoded);
        assert_eq!(decoded.addr(), &addr);
    }

    #[test]
    fn test_acceptor_remove_roundtrip() {
        let id = AcceptorId::from_bytes([42u8; 32]);
        let remove = AcceptorRemove::new(id);

        let custom = remove.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), ACCEPTOR_REMOVE_PROPOSAL_TYPE);

        let decoded = AcceptorRemove::from_custom_proposal(&custom).unwrap();
        assert_eq!(remove, decoded);
        assert_eq!(decoded.acceptor_id(), id);
    }

    #[test]
    fn test_proposal_and_extension_types_are_different() {
        assert_ne!(ACCEPTOR_ADD_PROPOSAL_TYPE, ACCEPTOR_REMOVE_PROPOSAL_TYPE);
        assert_ne!(
            ACCEPTOR_ADD_PROPOSAL_TYPE,
            COMPACTION_CLAIM_PROPOSAL_TYPE
        );
    }

    #[test]
    fn test_extension_types_in_private_range() {
        assert!(AcceptorsExt::extension_type().raw_value() >= 0xF000);
        assert!(ACCEPTOR_ADD_PROPOSAL_TYPE.raw_value() >= 0xF000);
        assert!(ACCEPTOR_REMOVE_PROPOSAL_TYPE.raw_value() >= 0xF000);
        assert!(CrdtRegistrationExt::extension_type().raw_value() >= 0xF000);
        assert!(SupportedCrdtsExt::extension_type().raw_value() >= 0xF000);
        assert!(CrdtSnapshotExt::extension_type().raw_value() >= 0xF000);
    }

    #[test]
    fn test_crdt_snapshot_roundtrip() {
        let snapshot = b"some crdt state".to_vec();
        let ext = CrdtSnapshotExt::new(snapshot.clone());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = CrdtSnapshotExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(decoded.snapshot(), &snapshot);
    }

    #[test]
    fn test_crdt_snapshot_empty() {
        let ext = CrdtSnapshotExt::new(Vec::new());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = CrdtSnapshotExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert!(decoded.snapshot().is_empty());
    }

    #[test]
    fn test_supported_crdts_roundtrip() {
        let ext = SupportedCrdtsExt::new(["yjs", "automerge", "none"]);

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = SupportedCrdtsExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.type_ids.len(), 3);
        assert!(decoded.supports("yjs"));
        assert!(decoded.supports("automerge"));
        assert!(decoded.supports("none"));
        assert!(!decoded.supports("unknown"));
    }

    #[test]
    fn test_supported_crdts_empty() {
        let ext = SupportedCrdtsExt::default();
        assert!(ext.type_ids.is_empty());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = SupportedCrdtsExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
    }

    #[test]
    fn test_crdt_registration_roundtrip() {
        let ext = CrdtRegistrationExt::new("yjs");

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = CrdtRegistrationExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.type_id(), "yjs");
    }

    #[test]
    fn test_crdt_registration_empty() {
        let ext = CrdtRegistrationExt::new("");

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = CrdtRegistrationExt::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.type_id(), "");
    }

    #[test]
    fn test_compaction_claim_roundtrip() {
        let mut watermark = BTreeMap::new();
        watermark.insert(MemberFingerprint([1u8; 32]), 42);
        watermark.insert(MemberFingerprint([2u8; 32]), 100);
        let claim = CompactionClaim {
            level: 1,
            watermark: watermark.clone(),
            deadline: 1_700_000_000,
        };

        let custom = claim.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), COMPACTION_CLAIM_PROPOSAL_TYPE);

        let decoded = CompactionClaim::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, claim);
    }

    #[test]
    fn test_compaction_complete_roundtrip() {
        let mut watermark = BTreeMap::new();
        watermark.insert(MemberFingerprint([3u8; 32]), 99);
        let complete = CompactionComplete {
            level: 2,
            watermark,
        };

        let custom = complete.to_custom_proposal().unwrap();
        assert_eq!(custom.proposal_type(), COMPACTION_COMPLETE_PROPOSAL_TYPE);

        let decoded = CompactionComplete::from_custom_proposal(&custom).unwrap();
        assert_eq!(decoded, complete);
    }

    #[test]
    fn test_compaction_proposal_types_in_private_range() {
        assert!(COMPACTION_CLAIM_PROPOSAL_TYPE.raw_value() >= 0xF000);
        assert!(COMPACTION_COMPLETE_PROPOSAL_TYPE.raw_value() >= 0xF000);
        assert_ne!(
            COMPACTION_CLAIM_PROPOSAL_TYPE,
            COMPACTION_COMPLETE_PROPOSAL_TYPE
        );
    }

    #[test]
    fn test_acceptor_ids_extraction() {
        let addr1 = test_addr(1);
        let addr2 = test_addr(2);
        let expected_id1 = AcceptorId::from_bytes(*addr1.id.as_bytes());
        let expected_id2 = AcceptorId::from_bytes(*addr2.id.as_bytes());

        let ext = AcceptorsExt::new(vec![addr1, addr2]);
        let ids = ext.acceptor_ids();

        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], expected_id1);
        assert_eq!(ids[1], expected_id2);
    }
}
