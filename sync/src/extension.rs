//! MLS extensions for Universal Sync
//!
//! Custom MLS group context extensions used by the sync protocol.

use mls_rs::extension::{ExtensionType, MlsCodecExtension};
use mls_rs::mls_rs_codec::{self as mls_rs_codec, MlsDecode, MlsEncode, MlsSize};

use crate::proposal::AcceptorId;

/// Extension type for adding an acceptor (private use range: 0xF000-0xFFFF)
pub const ACCEPTOR_ADD_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF796);

/// Extension type for removing an acceptor (private use range: 0xF000-0xFFFF)
pub const ACCEPTOR_REMOVE_EXTENSION_TYPE: ExtensionType = ExtensionType::new(0xF797);

/// MLS group context extension to add a federated acceptor
///
/// When this extension is present in the group context, the acceptor
/// should be added to the set of known acceptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorAdd(pub AcceptorId);

impl AcceptorAdd {
    /// Create a new AcceptorAdd extension
    pub fn new(acceptor_id: AcceptorId) -> Self {
        Self(acceptor_id)
    }

    /// Get the acceptor ID being added
    pub fn acceptor_id(&self) -> AcceptorId {
        self.0
    }
}

impl MlsSize for AcceptorAdd {
    fn mls_encoded_len(&self) -> usize {
        32 // AcceptorId is always 32 bytes
    }
}

impl MlsEncode for AcceptorAdd {
    fn mls_encode(&self, writer: &mut Vec<u8>) -> Result<(), mls_rs_codec::Error> {
        writer.extend_from_slice(&self.0 .0);
        Ok(())
    }
}

impl MlsDecode for AcceptorAdd {
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
    /// Create a new AcceptorRemove extension
    pub fn new(acceptor_id: AcceptorId) -> Self {
        Self(acceptor_id)
    }

    /// Get the acceptor ID being removed
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
    use super::*;

    #[test]
    fn test_acceptor_add_roundtrip() {
        let id = AcceptorId::from_bytes([42u8; 32]);
        let ext = AcceptorAdd::new(id);

        let encoded = ext.mls_encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 32);

        let decoded = AcceptorAdd::mls_decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ext, decoded);
        assert_eq!(decoded.acceptor_id(), id);
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
    }

    #[test]
    fn test_extension_types_in_private_range() {
        assert!(AcceptorAdd::extension_type().raw_value() >= 0xF000);
        assert!(AcceptorRemove::extension_type().raw_value() >= 0xF000);
    }
}
