//! Wire types for the client↔acceptor protocol: handshake, group messages,
//! and application message delivery.

use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

use crate::proposal::{Epoch, MemberId};

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

/// Paxos payload wrapping an MLS message (commit or proposal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    #[serde(with = "mls_bytes")]
    pub mls_message: MlsMessage,
}

impl GroupMessage {
    #[must_use]
    pub fn new(mls_message: MlsMessage) -> Self {
        Self { mls_message }
    }
}

/// Unique identifier for an application message: (group, epoch, sender, index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    pub group_id: GroupId,
    pub epoch: Epoch,
    pub sender: MemberId,
    pub index: u32,
}

/// Encrypted application message (MLS `PrivateMessage` ciphertext).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EncryptedAppMessage {
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRequest {
    Send(EncryptedAppMessage),
    Subscribe { since_seq: u64 },
    Backfill { since_seq: u64, limit: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageResponse {
    Stored {
        arrival_seq: u64,
    },
    Message {
        arrival_seq: u64,
        message: EncryptedAppMessage,
    },
    BackfillComplete {
        last_seq: u64,
        has_more: bool,
    },
    Error(String),
}

mod mls_bytes {
    use mls_rs::mls_rs_codec::{MlsDecode, MlsEncode};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: MlsEncode, S: Serializer>(value: &T, ser: S) -> Result<S::Ok, S::Error> {
        let bytes = value
            .mls_encode_to_vec()
            .map_err(|e| serde::ser::Error::custom(format!("{e:?}")))?;
        bytes.serialize(ser)
    }

    pub fn deserialize<'de, T: MlsDecode, D: Deserializer<'de>>(de: D) -> Result<T, D::Error> {
        let bytes = Vec::<u8>::deserialize(de)?;
        T::mls_decode(&mut bytes.as_slice()).map_err(|e| serde::de::Error::custom(format!("{e:?}")))
    }
}
