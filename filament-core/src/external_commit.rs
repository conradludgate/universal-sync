//! Types and helpers for the external commit join flow.
//!
//! An existing group member encrypts a `GroupInfo` message with a random
//! AES-256-GCM key and stores the ciphertext on a spool. The QR code
//! payload carries the spool identity, group ID, and decryption material.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use serde::{Deserialize, Serialize};

use crate::{AcceptorId, GroupId};

/// Decryption material and spool identity encoded in a QR code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QrPayload {
    pub spool_id: AcceptorId,
    pub group_id: GroupId,
    pub key: [u8; 32],
    pub nonce: [u8; 12],
}

impl QrPayload {
    /// Serialize to bytes.
    ///
    /// # Panics
    ///
    /// Panics if postcard serialization fails (should not happen).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("QrPayload serialization should not fail")
    }

    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Encrypt plaintext with AES-256-GCM. Returns the ciphertext (which
/// includes the GCM auth tag appended by the aes-gcm crate).
///
/// # Errors
///
/// Returns an error if encryption fails.
pub fn encrypt_group_info(
    key: &[u8; 32],
    nonce: &[u8; 12],
    plaintext: &[u8],
) -> Result<Vec<u8>, aes_gcm::Error> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher.encrypt(Nonce::from_slice(nonce), plaintext)
}

/// Decrypt ciphertext produced by [`encrypt_group_info`].
///
/// # Errors
///
/// Returns an error if decryption or authentication fails.
pub fn decrypt_group_info(
    key: &[u8; 32],
    nonce: &[u8; 12],
    ciphertext: &[u8],
) -> Result<Vec<u8>, aes_gcm::Error> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher.decrypt(Nonce::from_slice(nonce), ciphertext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [0xAA; 32];
        let nonce = [0xBB; 12];
        let plaintext = b"hello group info";

        let ciphertext = encrypt_group_info(&key, &nonce, plaintext).unwrap();
        assert_ne!(&ciphertext, plaintext);

        let decrypted = decrypt_group_info(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key = [0xAA; 32];
        let nonce = [0xBB; 12];
        let ciphertext = encrypt_group_info(&key, &nonce, b"secret").unwrap();

        let wrong_key = [0xCC; 32];
        assert!(decrypt_group_info(&wrong_key, &nonce, &ciphertext).is_err());
    }

    #[test]
    fn wrong_nonce_fails() {
        let key = [0xAA; 32];
        let nonce = [0xBB; 12];
        let ciphertext = encrypt_group_info(&key, &nonce, b"secret").unwrap();

        let wrong_nonce = [0xCC; 12];
        assert!(decrypt_group_info(&key, &wrong_nonce, &ciphertext).is_err());
    }

    #[test]
    fn qr_payload_roundtrip() {
        let payload = QrPayload {
            spool_id: AcceptorId::from_bytes([42; 32]),
            group_id: GroupId::new([1; 32]),
            key: [2; 32],
            nonce: [3; 12],
        };
        let bytes = payload.to_bytes();
        let decoded = QrPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.group_id, payload.group_id);
        assert_eq!(decoded.key, payload.key);
        assert_eq!(decoded.nonce, payload.nonce);
        assert_eq!(decoded.spool_id, payload.spool_id);
    }
}
