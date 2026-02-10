//! Postcard codec for length-delimited framing with serde serialization.
//!
//! Provides two codecs:
//! - [`PostcardCodec`]: plain postcard serialization (for unversioned messages like Handshake)
//! - [`VersionedCodec`]: version-dispatched deserialization driven by a `protocol_version`

use std::marker::PhantomData;
use std::{fmt, io};

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// Error from versioned serialize/deserialize.
#[derive(Debug)]
pub enum VersionedError {
    UnknownProtocolVersion(u32),
    Postcard(postcard::Error),
}

impl fmt::Display for VersionedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProtocolVersion(v) => write!(f, "unknown protocol version: {v}"),
            Self::Postcard(e) => fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for VersionedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postcard(e) => Some(e),
            Self::UnknownProtocolVersion(_) => None,
        }
    }
}

impl From<postcard::Error> for VersionedError {
    fn from(e: postcard::Error) -> Self {
        Self::Postcard(e)
    }
}

const SUPPORTED_VERSION: u32 = 1;

/// Serialize with protocol version 1 (postcard). Returns [`VersionedError::UnknownProtocolVersion`] for other versions.
///
/// # Errors
///
/// Returns [`VersionedError::UnknownProtocolVersion`] if `protocol_version != 1`, or [`VersionedError::Postcard`] if serialization fails.
pub fn serialize_v1<T: Serialize>(t: &T, protocol_version: u32) -> Result<Vec<u8>, VersionedError> {
    if protocol_version != SUPPORTED_VERSION {
        return Err(VersionedError::UnknownProtocolVersion(protocol_version));
    }
    postcard::to_allocvec(t).map_err(VersionedError::Postcard)
}

/// Deserialize with protocol version 1 (postcard). Returns [`VersionedError::UnknownProtocolVersion`] for other versions.
///
/// # Errors
///
/// Returns [`VersionedError::UnknownProtocolVersion`] if `protocol_version != 1`, or [`VersionedError::Postcard`] if deserialization fails.
pub fn deserialize_v1<T: for<'de> Deserialize<'de>>(
    protocol_version: u32,
    bytes: &[u8],
) -> Result<T, VersionedError> {
    if protocol_version != SUPPORTED_VERSION {
        return Err(VersionedError::UnknownProtocolVersion(protocol_version));
    }
    postcard::from_bytes(bytes).map_err(VersionedError::Postcard)
}

/// Version-dispatched serialization/deserialization.
///
/// Types that implement this trait can be serialized and deserialized
/// according to a protocol version read from the group's `GroupContextExt`.
pub trait Versioned: Sized {
    /// Serialize using the given protocol version's schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unknown or serialization fails.
    fn serialize_versioned(&self, protocol_version: u32) -> Result<Vec<u8>, VersionedError>;

    /// Deserialize from the given protocol version's schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unknown or deserialization fails.
    fn deserialize_versioned(protocol_version: u32, bytes: &[u8]) -> Result<Self, VersionedError>;
}

fn new_length_delimited_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec()
}

// ---------------------------------------------------------------------------
// PostcardCodec — plain postcard (for unversioned messages)
// ---------------------------------------------------------------------------

/// Wraps [`LengthDelimitedCodec`] with automatic postcard serialization.
#[derive(Debug)]
pub struct PostcardCodec<T> {
    inner: LengthDelimitedCodec,
    _marker: PhantomData<T>,
}

impl<T> Clone for PostcardCodec<T> {
    fn clone(&self) -> Self {
        Self {
            inner: new_length_delimited_codec(),
            _marker: PhantomData,
        }
    }
}

impl<T> Default for PostcardCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PostcardCodec<T> {
    /// Max frame length: 16 MB.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: new_length_delimited_codec(),
            _marker: PhantomData,
        }
    }

    /// Wrap an existing `LengthDelimitedCodec`, reusing its configuration and
    /// internal state. Useful with `FramedRead::map_decoder` to swap the
    /// deserialization layer without losing buffered data.
    #[must_use]
    pub fn wrap(inner: LengthDelimitedCodec) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<T> Decoder for PostcardCodec<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(bytes) => {
                let item = postcard::from_bytes(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }
}

impl<T> Encoder<T> for PostcardCodec<T>
where
    T: Serialize,
{
    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = postcard::to_allocvec(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.inner.encode(Bytes::from(bytes), dst)
    }
}

// ---------------------------------------------------------------------------
// VersionedCodec — version-dispatched serialization
// ---------------------------------------------------------------------------

/// Length-delimited codec that uses [`Versioned`] for version-dispatched
/// serialization/deserialization. The `protocol_version` is read from the
/// group's `GroupContextExt` and can be updated when a commit changes it.
#[derive(Debug)]
pub struct VersionedCodec<T> {
    inner: LengthDelimitedCodec,
    protocol_version: u32,
    _marker: PhantomData<T>,
}

impl<T> VersionedCodec<T> {
    #[must_use]
    pub fn new(protocol_version: u32) -> Self {
        Self {
            inner: new_length_delimited_codec(),
            protocol_version,
            _marker: PhantomData,
        }
    }

    /// Wrap an existing `LengthDelimitedCodec`, reusing its configuration and
    /// internal state. Useful with `FramedRead::map_decoder` to swap the
    /// deserialization layer without losing buffered data.
    #[must_use]
    pub fn wrap(inner: LengthDelimitedCodec, protocol_version: u32) -> Self {
        Self {
            inner,
            protocol_version,
            _marker: PhantomData,
        }
    }

    pub fn set_protocol_version(&mut self, version: u32) {
        self.protocol_version = version;
    }

    #[must_use]
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
}

impl<T> Decoder for VersionedCodec<T>
where
    T: Versioned,
{
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(bytes) => {
                let item = T::deserialize_versioned(self.protocol_version, &bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                Ok(Some(item))
            }
            None => Ok(None),
        }
    }
}

impl<T> Encoder<T> for VersionedCodec<T>
where
    T: Versioned,
{
    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item
            .serialize_versioned(self.protocol_version)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        self.inner.encode(Bytes::from(bytes), dst)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokio_util::codec::{Decoder, Encoder};

    use super::*;
    use crate::protocol::{AuthData, MessageRequest, MessageResponse};

    /// Demonstrates that `FramedRead::read_buffer_mut()` does NOT make
    /// pre-buffered data decodable — `is_readable` stays `false` so
    /// `poll_next` skips `decode()` entirely and waits for new IO that
    /// never arrives. `map_decoder` preserves the internal state correctly.
    #[tokio::test]
    async fn framed_read_buffer_mut_ignores_buffered_data() {
        use futures::StreamExt;
        use tokio_util::codec::FramedRead;

        // Encode a frame with LengthDelimitedCodec into raw bytes.
        let mut raw = BytesMut::new();
        LengthDelimitedCodec::new()
            .encode(Bytes::from("hello"), &mut raw)
            .unwrap();

        // Use a duplex where the write half is kept alive but never written to.
        // This makes poll_read return Pending (not EOF).
        let (client, _server) = tokio::io::duplex(1024);

        // BROKEN: create new FramedRead, inject buffer via read_buffer_mut.
        // poll_next returns Pending forever because is_readable=false.
        let mut reader = FramedRead::new(client, LengthDelimitedCodec::new());
        *reader.read_buffer_mut() = raw.clone();

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), reader.next()).await;
        assert!(
            result.is_err(),
            "read_buffer_mut data is ignored — poll_next hangs"
        );
    }

    /// Shows that `map_decoder` correctly preserves the internal `is_readable`
    /// flag and buffer, so pre-buffered data is decoded immediately.
    #[tokio::test]
    async fn map_decoder_preserves_buffered_data() {
        use futures::StreamExt;
        use tokio::io::AsyncWriteExt;
        use tokio_util::codec::FramedRead;

        // Encode a String via PostcardCodec for a valid frame.
        let mut frame_bytes = BytesMut::new();
        PostcardCodec::<String>::new()
            .encode("hello".to_string(), &mut frame_bytes)
            .unwrap();

        // Phase 1: read one frame to set is_readable=true in the internal state.
        let (client, mut server) = tokio::io::duplex(1024);
        server.write_all(&frame_bytes).await.unwrap();

        let mut phase1 = FramedRead::new(client, LengthDelimitedCodec::new());
        let _frame = phase1.next().await.unwrap().unwrap();

        // Phase 2: inject new buffered data and swap to PostcardCodec via map_decoder.
        *phase1.read_buffer_mut() = frame_bytes;
        let mut phase2: FramedRead<_, PostcardCodec<String>> =
            phase1.map_decoder(PostcardCodec::wrap);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), phase2.next()).await;
        assert!(
            result.is_ok(),
            "map_decoder preserves is_readable — decode succeeds immediately"
        );
        assert_eq!(result.unwrap().unwrap().unwrap(), "hello");
    }

    #[test]
    fn versioned_auth_data_roundtrip() {
        let data = AuthData::update(42);
        let bytes = data.serialize_versioned(1).unwrap();
        let decoded = AuthData::deserialize_versioned(1, &bytes).unwrap();
        assert_eq!(decoded.seq(), 42);
    }

    #[test]
    fn versioned_auth_data_unknown_version() {
        let data = AuthData::update(1);
        assert!(data.serialize_versioned(99).is_err());
        assert!(AuthData::deserialize_versioned(99, &[0]).is_err());
    }

    #[test]
    fn versioned_codec_roundtrip() {
        let mut codec: VersionedCodec<MessageRequest> = VersionedCodec::new(1);
        let request = MessageRequest::Subscribe {
            state_vector: BTreeMap::default(),
        };

        let mut buf = BytesMut::new();
        codec.encode(request, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(decoded, MessageRequest::Subscribe { .. }));
    }

    #[test]
    fn versioned_codec_version_update() {
        let mut codec: VersionedCodec<MessageResponse> = VersionedCodec::new(1);
        assert_eq!(codec.protocol_version(), 1);

        codec.set_protocol_version(2);
        assert_eq!(codec.protocol_version(), 2);
    }

    #[test]
    fn postcard_codec_clone() {
        let codec = PostcardCodec::<String>::new();
        let mut cloned = codec.clone();
        let mut buf = BytesMut::new();
        cloned.encode("hello".to_string(), &mut buf).unwrap();
        let decoded: String = cloned.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn postcard_codec_default() {
        let mut codec = PostcardCodec::<u32>::default();
        let mut buf = BytesMut::new();
        codec.encode(42u32, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn versioned_codec_wrap() {
        use tokio_util::codec::LengthDelimitedCodec;
        let inner = LengthDelimitedCodec::new();
        let mut codec: VersionedCodec<MessageResponse> = VersionedCodec::wrap(inner, 1);
        let mut buf = BytesMut::new();
        codec.encode(MessageResponse::Stored, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(decoded, MessageResponse::Stored));
    }
}
