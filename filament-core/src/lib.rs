//! Shared types for proposers and acceptors.

#![warn(clippy::pedantic)]

use std::path::Path;

use error_stack::{Report, ResultExt};

pub mod codec;
pub mod crdt;
pub mod error;
pub mod extension;
pub mod proposal;
pub mod protocol;
pub mod sink_stream;
pub use codec::{Versioned, VersionedCodec, VersionedError};
pub use crdt::{COMPACTION_BASE, Crdt, CrdtError, NoCrdt};
pub use error::{
    AcceptorContext, ConnectorError, EpochContext, GroupContext, MemberContext, OperationContext,
};
pub use extension::{
    CURRENT_PROTOCOL_VERSION, GroupContextExt, GroupContextExtV1, GroupInfoExt, KeyPackageExt,
    LeafNodeExt, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE, SyncProposal,
};
pub use proposal::{AcceptorId, Attempt, Epoch, GroupProposal, MemberId, UnsignedProposal};
pub use protocol::{
    AuthData, ClientId, EncryptedAppMessage, GroupId, GroupMessage, Handshake, HandshakeResponse,
    MemberFingerprint, MessageId, MessageRequest, MessageResponse, StateVector, StreamType,
};
pub use sink_stream::FromIoError;

/// ALPN protocol identifier for Paxos connections
pub const PAXOS_ALPN: &[u8] = b"filament/paxos/1";

#[derive(Debug)]
pub struct KeyLoadError;

impl std::fmt::Display for KeyLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load secret key")
    }
}

impl std::error::Error for KeyLoadError {}

/// Load a 32-byte secret key from a file (raw bytes or base58).
///
/// # Errors
///
/// Returns [`KeyLoadError`] if the file cannot be read or does not contain
/// a valid 32-byte key (raw or base58-encoded).
pub fn load_secret_key(path: impl AsRef<Path>) -> Result<[u8; 32], Report<KeyLoadError>> {
    let path = path.as_ref();
    let contents = std::fs::read(path)
        .change_context(KeyLoadError)
        .attach_with(|| format!("reading key file: {}", path.display()))?;

    // Try parsing as raw bytes first
    if let Ok(bytes) = contents.as_slice().try_into() {
        return Ok(bytes);
    }

    // Try parsing as base58
    let b58_str = String::from_utf8(contents)
        .change_context(KeyLoadError)
        .attach("key file is not valid UTF-8")?;

    let b58_str = b58_str.trim();

    let bytes = bs58::decode(b58_str)
        .into_vec()
        .change_context(KeyLoadError)
        .attach("invalid base58 encoding")?;

    bytes.try_into().map_err(|v: Vec<u8>| {
        Report::new(KeyLoadError).attach(format!("expected 32 bytes, got {}", v.len()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_load_error_display() {
        assert_eq!(KeyLoadError.to_string(), "failed to load secret key");
        let _: &dyn std::error::Error = &KeyLoadError;
    }

    #[test]
    fn load_secret_key_raw_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("key");
        let key = [42u8; 32];
        std::fs::write(&path, key).unwrap();
        let loaded = load_secret_key(&path).unwrap();
        assert_eq!(loaded, key);
    }

    #[test]
    fn load_secret_key_base58() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("key");
        let key = [1u8; 32];
        let encoded = bs58::encode(key).into_string();
        std::fs::write(&path, encoded).unwrap();
        let loaded = load_secret_key(&path).unwrap();
        assert_eq!(loaded, key);
    }

    #[test]
    fn load_secret_key_missing_file() {
        let result = load_secret_key("/tmp/nonexistent_filament_key_file");
        assert!(result.is_err());
    }

    #[test]
    fn load_secret_key_invalid_base58() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("key");
        std::fs::write(&path, "not-valid-base58!!!").unwrap();
        let result = load_secret_key(&path);
        assert!(result.is_err());
    }

    #[test]
    fn load_secret_key_wrong_length_base58() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("key");
        let short = bs58::encode([1u8; 16]).into_string();
        std::fs::write(&path, short).unwrap();
        let result = load_secret_key(&path);
        assert!(result.is_err());
    }
}
