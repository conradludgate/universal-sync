//! Handshake protocol for multiplexed streams between clients and acceptors.
//!
//! Each stream starts with a [`Handshake`] identifying the group and stream type
//! (proposals for Paxos consensus, or messages for application data).

use serde::{Deserialize, Serialize};

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
    /// Join an existing group's proposal stream.
    JoinProposals(GroupId),
    /// Register a new group with serialized `GroupInfo` bytes.
    CreateGroup(Vec<u8>),
    /// Join an existing group's message stream.
    JoinMessages(GroupId),
    /// Deliver a serialized MLS `Welcome` message.
    SendWelcome(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandshakeResponse {
    Ok,
    GroupNotFound,
    InvalidGroupInfo(String),
    Error(String),
}
