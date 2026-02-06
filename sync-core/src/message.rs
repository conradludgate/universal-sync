//! Message types for group operations.

use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

use crate::handshake::GroupId;
use crate::proposal::{Epoch, MemberId};

/// Paxos payload wrapping an MLS message (commit or proposal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    #[serde(with = "mls_bytes")]
    pub mls_message: MlsMessage,
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

impl GroupMessage {
    #[must_use]
    pub fn new(mls_message: MlsMessage) -> Self {
        Self { mls_message }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageRequest {
    Send(EncryptedAppMessage),
    Subscribe { since_seq: u64 },
    Backfill { since_seq: u64, limit: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageResponse {
    Stored { arrival_seq: u64 },
    Message { arrival_seq: u64, message: EncryptedAppMessage },
    BackfillComplete { last_seq: u64, has_more: bool },
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
