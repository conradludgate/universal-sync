//! Message types for group operations
//!
//! This module contains:
//! - [`GroupMessage`] - Paxos message payload for commits/proposals
//! - [`MessageId`] - Unique identifier for application messages
//! - [`EncryptedAppMessage`] - Encrypted application message

use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

use crate::handshake::GroupId;
use crate::proposal::{Epoch, MemberId};

/// The actual content being proposed through Paxos
///
/// This contains the MLS message with its cryptographic signature.
/// The signature in the MLS message authenticates both the sender
/// and the content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    /// The MLS message (commit, proposal, or application message)
    #[serde(with = "mls_bytes")]
    pub mls_message: MlsMessage,
}

/// Unique identifier for an application message.
///
/// Messages are uniquely identified by the combination of group, epoch,
/// sender, and per-sender index. The index resets to 0 at each epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    /// The group this message belongs to
    pub group_id: GroupId,
    /// The epoch when this message was encrypted
    pub epoch: Epoch,
    /// The sender's member index in the group tree
    pub sender: MemberId,
    /// Per-sender message index within this epoch (starts at 0)
    pub index: u32,
}

/// An encrypted application message.
///
/// Contains only the MLS `PrivateMessage` ciphertext. The message identity
/// (group, epoch, sender, index) is embedded in the ciphertext and extracted
/// on decryption via `ApplicationMessageDescription`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EncryptedAppMessage {
    /// MLS `PrivateMessage` ciphertext (contains authenticated data with message index)
    pub ciphertext: Vec<u8>,
}

impl GroupMessage {
    /// Create a new group message wrapping an MLS message
    #[must_use]
    pub fn new(mls_message: MlsMessage) -> Self {
        Self { mls_message }
    }
}

/// Request sent on a message stream.
///
/// Clients send these to acceptors to deliver or request messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRequest {
    /// Send an encrypted message to the group.
    ///
    /// The acceptor stores the message and broadcasts to subscribers.
    Send(EncryptedAppMessage),

    /// Subscribe to new messages for this group.
    ///
    /// The acceptor will stream `MessageResponse::Message` for new arrivals.
    /// Includes a cursor to resume from (0 for all new messages).
    Subscribe {
        /// Resume from this arrival sequence (exclusive).
        /// Use 0 to receive only new messages.
        since_seq: u64,
    },

    /// Request historical messages (backfill).
    ///
    /// Returns messages with `arrival_seq > since_seq`, up to `limit`.
    Backfill {
        /// Resume from this arrival sequence (exclusive)
        since_seq: u64,
        /// Maximum number of messages to return
        limit: u32,
    },
}

/// Response sent on a message stream.
///
/// Acceptors send these to clients in response to requests or subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageResponse {
    /// Acknowledgment that a message was stored.
    Stored {
        /// The acceptor-assigned arrival sequence
        arrival_seq: u64,
    },

    /// A message (either from subscription or backfill).
    Message {
        /// The acceptor-assigned arrival sequence
        arrival_seq: u64,
        /// The encrypted message
        message: EncryptedAppMessage,
    },

    /// End of backfill batch.
    BackfillComplete {
        /// Highest `arrival_seq` in this batch (for pagination)
        last_seq: u64,
        /// Whether there are more messages available
        has_more: bool,
    },

    /// Error response.
    Error(String),
}

/// Serde helper for MLS-encoded types
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
