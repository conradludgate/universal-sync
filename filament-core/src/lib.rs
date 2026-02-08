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
pub use codec::{Versioned, VersionedCodec};
pub use crdt::{
    default_compaction_config, CompactionConfig, CompactionLevel, Crdt, CrdtError, NoCrdt,
};
pub use error::{
    AcceptorContext, ConnectorError, EpochContext, GroupContext, MemberContext, OperationContext,
};
pub use extension::{
    GroupContextExt, GroupContextExtV1, GroupInfoExt, KeyPackageExt, LeafNodeExt, SyncProposal,
    CURRENT_PROTOCOL_VERSION, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE,
};
pub use proposal::{AcceptorId, Attempt, Epoch, GroupProposal, MemberId, UnsignedProposal};
pub use protocol::{
    AuthData, EncryptedAppMessage, GroupId, GroupMessage, Handshake, HandshakeResponse,
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
