//! Postcard codec for length-delimited framing with serde serialization.

use std::io;
use std::marker::PhantomData;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// Wraps [`LengthDelimitedCodec`] with automatic postcard serialization.
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
    /// Max frame length: 16 MB.
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
