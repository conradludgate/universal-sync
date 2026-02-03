//! Utility functions shared across proposers and acceptors

use std::path::Path;

use error_stack::{Report, ResultExt};

/// Error type for key loading operations.
#[derive(Debug)]
pub struct KeyLoadError;

impl std::fmt::Display for KeyLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to load secret key")
    }
}

impl std::error::Error for KeyLoadError {}

/// Load a 32-byte secret key from a file.
///
/// Supports two formats:
/// - Raw bytes: exactly 32 bytes
/// - Hex string: 64 hex characters (with optional `0x` prefix)
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read
/// - The file is not a valid key format
/// - The key is not exactly 32 bytes
///
/// # Example
///
/// ```ignore
/// let key_bytes = load_secret_key("./my-key.hex")?;
/// let iroh_key = iroh::SecretKey::from_bytes(&key_bytes);
/// ```
pub fn load_secret_key(path: impl AsRef<Path>) -> Result<[u8; 32], Report<KeyLoadError>> {
    let path = path.as_ref();
    let contents = std::fs::read(path)
        .change_context(KeyLoadError)
        .attach_printable_lazy(|| format!("reading key file: {}", path.display()))?;

    // Try parsing as raw bytes first
    if let Ok(bytes) = contents.as_slice().try_into() {
        return Ok(bytes);
    }

    // Try parsing as hex
    let hex_str = String::from_utf8(contents)
        .change_context(KeyLoadError)
        .attach_printable("key file is not valid UTF-8")?;

    let hex_str = hex_str.trim();
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);

    if hex_str.len() != 64 {
        return Err(Report::new(KeyLoadError).attach_printable(format!(
            "expected 32 raw bytes or 64 hex characters, got {} chars",
            hex_str.len()
        )));
    }

    let mut bytes = [0u8; 32];
    hex::decode_to_slice(hex_str, &mut bytes)
        .change_context(KeyLoadError)
        .attach_printable("invalid hex encoding")?;

    Ok(bytes)
}
