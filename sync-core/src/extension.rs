//! MLS extensions for Universal Sync
//!
//! Custom MLS group context extensions used by the sync protocol.

use iroh::EndpointAddr;
use mls_rs::extension::{ExtensionType, MlsCodecExtension};
use mls_rs::mls_rs_codec::{self as mls_rs_codec, MlsDecode, MlsEncode, MlsSize};

use crate::proposal::AcceptorId;

/// Extension type for the full acceptor list (group context extension)
pub const ACCEPTORS_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF795);

/// Extension type for adding an acceptor (private use range: 0xF000-0xFFFF)
pub const ACCEPTOR_ADD_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF796);

/// Extension type for removing an acceptor (private use range: 0xF000-0xFFFF)
pub const ACCEPTOR_REMOVE_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF797);

/// MLS group context extension containing the full list of acceptors
///
/// This extension is set when the group is created and updated whenever
/// acceptors are added or removed. New members joining via Welcome can
/// read this extension to discover the acceptor set.
///
/// Each acceptor is stored as a full `EndpointAddr` which includes the
/// public key and network addresses needed to connect.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AcceptorsExt(pub Vec<EndpointAddr>);

impl AcceptorsExt {
    /// Create a new `AcceptorsExt` from a list of endpoint addresses
    pub fn new(acceptors: impl IntoIterator<Item = EndpointAddr>) -> Self {
        Self(acceptors.into_iter().collect())
    }

    /// Get the list of endpoint addresses
    #[must_use]
    pub fn acceptors(&self) -> &[EndpointAddr] {
        &self.0
    }

    /// Get the list of acceptor IDs (public keys) from the addresses
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
        // Serialize with postcard to get actual length
        postcard::to_allocvec(&self.0).map_or(4, |v| 4 + v.len())
    }
}

/// Custom error code for postcard serialization failures
const POSTCARD_ERROR: u8 = 1;

impl MlsEncode for AcceptorsExt {
    #[expect(clippy::cast_possible_truncation)]
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        // Serialize addresses with postcard
        let bytes = postcard::to_allocvec(&self.0)
            .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        // Write length prefix
        let len = bytes.len() as u32;
        writer.extend_from_slice(&len.to_be_bytes());
        writer.extend_from_slice(&bytes);
        Ok(())
    }
}

impl MlsDecode for AcceptorsExt {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        if reader.len() < 4 {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }
        let len = u32::from_be_bytes([reader[0], reader[1], reader[2], reader[3]]) as usize;
        *reader = &reader[4..];

        if reader.len() < len {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }

        let acceptors: Vec<EndpointAddr> = postcard::from_bytes(&reader[..len])
            .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        *reader = &reader[len..];

        Ok(Self(acceptors))
    }
}

impl MlsCodecExtension for AcceptorsExt {
    fn extension_type() -> ExtensionType {
        ACCEPTORS_EXTENSION_TYPE
    }
}

/// MLS group context extension to add a federated acceptor
///
/// When this extension is present in the group context, the acceptor
/// should be added to the set of known acceptors.
///
/// Contains the full `EndpointAddr` so other members know how to connect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorAdd(pub EndpointAddr);

impl AcceptorAdd {
    /// Create a new `AcceptorAdd` extension
    #[must_use]
    pub fn new(addr: EndpointAddr) -> Self {
        Self(addr)
    }

    /// Get the endpoint address being added
    #[must_use]
    pub fn addr(&self) -> &EndpointAddr {
        &self.0
    }

    /// Get the acceptor ID (public key) from the address
    #[must_use]
    pub fn acceptor_id(&self) -> AcceptorId {
        AcceptorId::from_bytes(*self.0.id.as_bytes())
    }
}

impl MlsSize for AcceptorAdd {
    fn mls_encoded_len(&self) -> usize {
        // Serialize with postcard to get actual length
        postcard::to_allocvec(&self.0).map_or(4, |v| 4 + v.len())
    }
}

impl MlsEncode for AcceptorAdd {
    #[expect(clippy::cast_possible_truncation)]
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        // Serialize address with postcard
        let bytes = postcard::to_allocvec(&self.0)
            .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        // Write length prefix
        let len = bytes.len() as u32;
        writer.extend_from_slice(&len.to_be_bytes());
        writer.extend_from_slice(&bytes);
        Ok(())
    }
}

impl MlsDecode for AcceptorAdd {
    fn mls_decode(reader: &mut &[u8]) -> Result<Self, mls_rs_codec::Error> {
        if reader.len() < 4 {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }
        let len = u32::from_be_bytes([reader[0], reader[1], reader[2], reader[3]]) as usize;
        *reader = &reader[4..];

        if reader.len() < len {
            return Err(mls_rs_codec::Error::UnexpectedEOF);
        }

        let addr: EndpointAddr = postcard::from_bytes(&reader[..len])
            .map_err(|_| mls_rs_codec::Error::Custom(POSTCARD_ERROR))?;
        *reader = &reader[len..];

        Ok(Self(addr))
    }
}

impl MlsCodecExtension for AcceptorAdd {
    fn extension_type() -> ExtensionType {
        ACCEPTOR_ADD_EXTENSION_TYPE
    }
}

/// MLS group context extension to remove a federated acceptor
///
/// When this extension is present in the group context, the acceptor
/// should be removed from the set of known acceptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorRemove(pub AcceptorId);

impl AcceptorRemove {
    /// Create a new `AcceptorRemove` extension
    #[must_use]
    pub fn new(acceptor_id: AcceptorId) -> Self {
        Self(acceptor_id)
    }

    /// Get the acceptor ID being removed
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

impl MlsCodecExtension for AcceptorRemove {
    fn extension_type() -> ExtensionType {
        ACCEPTOR_REMOVE_EXTENSION_TYPE
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
        let ext = AcceptorAdd::new(addr.clone());

        let encoded = ext.mls_encode_to_vec().unwrap();

        let decoded = AcceptorAdd::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.addr(), &addr);
    }

    #[test]
    fn test_acceptor_remove_roundtrip() {
        let id = AcceptorId::from_bytes([42u8; 32]);
        let ext = AcceptorRemove::new(id);

        let encoded = ext.mls_encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 32);

        let decoded = AcceptorRemove::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.acceptor_id(), id);
    }

    #[test]
    fn test_extension_types_are_different() {
        assert_ne!(
            AcceptorAdd::extension_type(),
            AcceptorRemove::extension_type()
        );
        assert_ne!(
            AcceptorsExt::extension_type(),
            AcceptorAdd::extension_type()
        );
    }

    #[test]
    fn test_extension_types_in_private_range() {
        assert!(AcceptorsExt::extension_type().raw_value() >= 0xF000);
        assert!(AcceptorAdd::extension_type().raw_value() >= 0xF000);
        assert!(AcceptorRemove::extension_type().raw_value() >= 0xF000);
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
