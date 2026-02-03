//! Connection and stream handshake protocol
//!
//! When a client opens a bidirectional stream to an acceptor, it must first
//! send a handshake message to identify the group and stream type.
//!
//! # Stream Types
//!
//! Each group can have two stream types:
//! - **Proposals**: Paxos consensus for MLS commits
//! - **Messages**: Application message delivery (no consensus)
//!
//! # Connection Multiplexing
//!
//! A single iroh connection to an acceptor can host multiple streams:
//! ```text
//! Connection (to acceptor A)
//! ├── Stream 1: Group X - Proposals
//! ├── Stream 2: Group X - Messages
//! ├── Stream 3: Group Y - Proposals
//! └── Stream 4: Group Y - Messages
//! ```

use serde::{Deserialize, Serialize};

/// Group identifier (32 bytes)
///
/// MLS group IDs are hashed/padded to 32 bytes for consistent key sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(pub [u8; 32]);

impl GroupId {
    /// Create a new group ID from a 32-byte array
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Create a group ID from a slice, padding or truncating as needed
    ///
    /// - If shorter than 32 bytes, pads with zeros
    /// - If longer than 32 bytes, truncates
    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Self {
        let mut id = [0u8; 32];
        let len = bytes.len().min(32);
        id[..len].copy_from_slice(&bytes[..len]);
        Self(id)
    }

    /// Get the bytes of this group ID
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
///
/// Each group can have multiple concurrent streams of different types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamType {
    /// Paxos proposal stream for MLS commits.
    ///
    /// Messages on this stream are `AcceptorRequest` / `AcceptorMessage`.
    Proposals,

    /// Application message stream.
    ///
    /// Messages on this stream are `MessageRequest` / `MessageResponse`.
    Messages,
}

/// Initial handshake message sent by clients on each stream.
///
/// This is the first message on any bidirectional stream. It tells
/// the acceptor which group and stream type the client wants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Handshake {
    /// Join an existing group's proposal stream (Paxos).
    ///
    /// The acceptor must already have this group's state.
    JoinProposals(GroupId),

    /// Create a new group and open its proposal stream.
    ///
    /// The `GroupInfo` is an MLS message that allows external parties
    /// to join the group. This is used when first registering a group
    /// with an acceptor.
    ///
    /// The bytes are a serialized `MlsMessage` containing `GroupInfo`.
    CreateGroup(Vec<u8>),

    /// Join an existing group's message stream.
    ///
    /// Used for receiving and sending application messages.
    JoinMessages(GroupId),
}

/// Handshake response from the acceptor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandshakeResponse {
    /// Handshake accepted, proceed with Paxos protocol
    Ok,

    /// Group not found (for Join)
    GroupNotFound,

    /// Invalid `GroupInfo` (for Create)
    InvalidGroupInfo(String),

    /// Other error
    Error(String),
}
