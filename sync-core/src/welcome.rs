//! Welcome bundle for new group members
//!
//! When a new member joins a group, they receive a `WelcomeBundle` containing:
//! - The MLS Welcome message (provides encryption keys)
//! - The CRDT type ID (identifies which CRDT implementation to use)
//! - The CRDT snapshot (provides current application state)
//!
//! The bundle is serialized and sent as a single message to ensure atomicity.

use serde::{Deserialize, Serialize};

/// A bundle sent to new members joining a group.
///
/// Contains everything a new member needs to participate:
/// - MLS Welcome message (for encryption keys and group state)
/// - CRDT type ID (which CRDT implementation to use)
/// - CRDT snapshot (for application state, if the group uses a CRDT)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WelcomeBundle {
    /// Serialized MLS Welcome message
    pub mls_welcome: Vec<u8>,

    /// CRDT type identifier (e.g., "yrs", "none")
    ///
    /// This tells the joining member which CRDT factory to use.
    #[serde(default)]
    pub crdt_type_id: String,

    /// Serialized CRDT snapshot (empty if no CRDT)
    ///
    /// This snapshot represents the current state of the group's CRDT
    /// at the time the welcome was created.
    pub crdt_snapshot: Vec<u8>,
}

impl WelcomeBundle {
    /// Create a new welcome bundle with just an MLS welcome (no CRDT).
    #[must_use]
    pub fn new(mls_welcome: Vec<u8>) -> Self {
        Self {
            mls_welcome,
            crdt_type_id: String::new(),
            crdt_snapshot: Vec::new(),
        }
    }

    /// Create a new welcome bundle with MLS welcome and CRDT info.
    #[must_use]
    pub fn with_crdt(mls_welcome: Vec<u8>, crdt_type_id: String, crdt_snapshot: Vec<u8>) -> Self {
        Self {
            mls_welcome,
            crdt_type_id,
            crdt_snapshot,
        }
    }

    /// Get the CRDT type ID.
    ///
    /// Returns "none" if no CRDT type was specified.
    #[must_use]
    pub fn crdt_type_id(&self) -> &str {
        if self.crdt_type_id.is_empty() {
            "none"
        } else {
            &self.crdt_type_id
        }
    }

    /// Check if this bundle contains a CRDT snapshot.
    #[must_use]
    pub fn has_crdt(&self) -> bool {
        !self.crdt_snapshot.is_empty()
    }

    /// Serialize the bundle to bytes.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_stdvec(self)
    }

    /// Deserialize a bundle from bytes.
    ///
    /// # Errors
    /// Returns an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WelcomeError> {
        postcard::from_bytes(bytes).map_err(|e| WelcomeError::DeserializationFailed(e.to_string()))
    }
}

/// Error type for welcome bundle operations
#[derive(Debug)]
pub enum WelcomeError {
    /// Failed to deserialize the bundle
    DeserializationFailed(String),
}

impl std::fmt::Display for WelcomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WelcomeError::DeserializationFailed(msg) => {
                write!(f, "failed to deserialize welcome bundle: {msg}")
            }
        }
    }
}

impl std::error::Error for WelcomeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_welcome_bundle_no_crdt() {
        let welcome = b"mls welcome message".to_vec();
        let bundle = WelcomeBundle::new(welcome.clone());

        assert!(!bundle.has_crdt());
        assert_eq!(bundle.mls_welcome, welcome);
        assert!(bundle.crdt_snapshot.is_empty());
    }

    #[test]
    fn test_welcome_bundle_with_crdt() {
        let welcome = b"mls welcome message".to_vec();
        let crdt = b"crdt snapshot".to_vec();
        let bundle = WelcomeBundle::with_crdt(welcome.clone(), "yrs".to_owned(), crdt.clone());

        assert!(bundle.has_crdt());
        assert_eq!(bundle.mls_welcome, welcome);
        assert_eq!(bundle.crdt_type_id(), "yrs");
        assert_eq!(bundle.crdt_snapshot, crdt);
    }

    #[test]
    fn test_welcome_bundle_roundtrip() {
        let bundle = WelcomeBundle::with_crdt(
            b"mls welcome message".to_vec(),
            "yrs".to_owned(),
            b"crdt snapshot".to_vec(),
        );

        let bytes = bundle.to_bytes().unwrap();
        let decoded = WelcomeBundle::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.mls_welcome, bundle.mls_welcome);
        assert_eq!(decoded.crdt_snapshot, bundle.crdt_snapshot);
    }

    #[test]
    fn test_welcome_bundle_invalid_bytes() {
        // Invalid bytes should return an error
        let invalid = b"not a valid bundle";
        let result = WelcomeBundle::from_bytes(invalid);
        assert!(result.is_err());
    }
}
