//! Postcard codec for length-delimited framing with serde serialization
//!
//! This module provides [`PostcardCodec`] which combines length-delimited framing
//! with postcard serialization for efficient binary encoding of Rust types.

use std::io;
use std::marker::PhantomData;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A codec that combines length-delimited framing with postcard serialization.
///
/// This wraps [`LengthDelimitedCodec`] and adds automatic postcard serialization
/// for any type implementing [`Serialize`] (encoding) or [`Deserialize`] (decoding).
///
/// # Type Parameters
/// - `D`: The type to decode (must implement `Deserialize`)
/// - `E`: The type to encode (must implement `Serialize`)
///
/// # Example
///
/// ```ignore
/// use tokio_util::codec::{FramedRead, FramedWrite};
/// use universal_sync_core::codec::PostcardCodec;
///
/// // Create a codec for AcceptorRequest/AcceptorMessage
/// let codec = PostcardCodec::<AcceptorRequest, AcceptorMessage>::new();
/// let reader = FramedRead::new(recv, codec.clone());
/// let writer = FramedWrite::new(send, codec);
/// ```
#[derive(Debug)]
pub struct PostcardCodec<T> {
    inner: LengthDelimitedCodec,
    _marker: PhantomData<T>,
}

impl<T> Clone for PostcardCodec<T> {
    fn clone(&self) -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(16 * 1024 * 1024)
                .new_codec(),
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
    /// Create a new postcard codec with default settings.
    ///
    /// Uses a max frame length of 16 MB.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(16 * 1024 * 1024)
                .new_codec(),
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
