//! `GroupMessage` - the Paxos message payload for group operations

use mls_rs::MlsMessage;
use serde::{Deserialize, Serialize};

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

impl GroupMessage {
    /// Create a new group message wrapping an MLS message
    #[must_use]
    pub fn new(mls_message: MlsMessage) -> Self {
        Self { mls_message }
    }
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
