//! Shared utilities.

use std::path::Path;

use error_stack::{Report, ResultExt};

#[derive(Debug)]
pub struct KeyLoadError;

impl std::fmt::Display for KeyLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load secret key")
    }
}

impl std::error::Error for KeyLoadError {}

/// Load a 32-byte secret key from a file (raw bytes or base58).
pub fn load_secret_key(path: impl AsRef<Path>) -> Result<[u8; 32], Report<KeyLoadError>> {
    let path = path.as_ref();
    let contents = std::fs::read(path)
        .change_context(KeyLoadError)
        .attach_opaque_with(|| format!("reading key file: {}", path.display()))?;

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
