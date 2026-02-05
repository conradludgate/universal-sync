//! Yrs (Yjs) CRDT implementation for testing
//!
//! This module provides a CRDT implementation using the yrs crate,
//! which is a Rust port of the Yjs CRDT library.
//!
//! Yrs provides a document-based CRDT that supports:
//! - Text editing
//! - Arrays
//! - Maps
//! - XML structures
//!
//! This implementation wraps a `yrs::Doc` and exposes it through the [`Crdt`] trait.
//!
//! # Client ID Derivation
//!
//! For collaborative editing, each client needs a stable, unique ID. We derive this
//! from the MLS signing public key using SHA256, taking the first 8 bytes as a u64.
//! This ensures the client ID remains stable across group membership changes (unlike
//! MLS member indexes which can shift when members are removed).

use std::any::Any;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use universal_sync_core::{Crdt, CrdtError, CrdtFactory};
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Transact, Update};

/// Derive a stable yrs client ID from an MLS signing public key.
///
/// Uses SHA256 and takes the first 8 bytes as a big-endian u64.
/// This provides a stable identity that doesn't change when MLS
/// member indexes shift due to membership changes.
#[must_use]
pub fn client_id_from_signing_key(signing_key: &[u8]) -> u64 {
    let hash = Sha256::digest(signing_key);
    u64::from_be_bytes(hash[..8].try_into().expect("sha256 produces 32 bytes"))
}

/// A CRDT implementation backed by a Yrs document.
///
/// This provides access to the full Yrs document API for creating and
/// manipulating shared data types like Text, Array, Map, etc.
pub struct YrsCrdt {
    doc: Doc,
}

impl YrsCrdt {
    /// Create a new empty Yrs document.
    #[must_use]
    pub fn new() -> Self {
        Self { doc: Doc::new() }
    }

    /// Create a Yrs document with a specific client ID.
    ///
    /// Client IDs are used to identify the source of operations.
    /// In a group, each member should have a unique client ID.
    #[must_use]
    pub fn with_client_id(client_id: u64) -> Self {
        Self {
            doc: Doc::with_client_id(client_id),
        }
    }

    /// Get a reference to the underlying Yrs document.
    ///
    /// Use this to access Yrs-specific APIs like creating Text, Array, Map, etc.
    #[must_use]
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Get a mutable reference to the underlying Yrs document.
    pub fn doc_mut(&mut self) -> &mut Doc {
        &mut self.doc
    }

    /// Get the current state vector of the document.
    ///
    /// The state vector can be used to request only the updates that
    /// another peer is missing.
    #[must_use]
    pub fn state_vector(&self) -> StateVector {
        self.doc.transact().state_vector()
    }

    /// Encode an update containing all changes since a given state vector.
    ///
    /// This is useful for syncing specific changes to a peer.
    ///
    /// # Errors
    /// Returns an error if encoding fails.
    pub fn encode_diff(&self, sv: &StateVector) -> Result<Vec<u8>, CrdtError> {
        let txn = self.doc.transact();
        Ok(txn.encode_diff_v1(sv))
    }
}

impl Default for YrsCrdt {
    fn default() -> Self {
        Self::new()
    }
}

impl Crdt for YrsCrdt {
    fn type_id(&self) -> &str {
        "yrs"
    }

    fn apply(&mut self, operation: &[u8]) -> Result<(), CrdtError> {
        let update = Update::decode_v1(operation)
            .map_err(|e| CrdtError::new(format!("decode error: {e}")))?;

        let my_client_id = self.doc.client_id();
        tracing::info!(my_client_id, operation_len = operation.len(), "YrsCrdt::apply");

        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| CrdtError::new(format!("apply error: {e}")))?;

        // Log the resulting text for debugging
        let text = self.doc.get_or_insert_text("content");
        let txn = self.doc.transact();
        let content = text.get_string(&txn);
        tracing::info!(my_client_id, text_len = content.len(), text_after_apply = %content, "YrsCrdt::apply result");

        Ok(())
    }

    fn merge(&mut self, snapshot: &[u8]) -> Result<(), CrdtError> {
        // In Yrs, merging a snapshot is the same as applying an update
        // that contains the full state
        self.apply(snapshot)
    }

    fn snapshot(&self) -> Result<Vec<u8>, CrdtError> {
        let txn = self.doc.transact();
        // Encode all state as an update from empty state vector
        Ok(txn.encode_state_as_update_v1(&StateVector::default()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Factory for creating [`YrsCrdt`] instances.
#[derive(Default, Clone)]
pub struct YrsCrdtFactory {
    /// Optional client ID generator.
    /// If None, Yrs will generate random client IDs.
    client_id: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
}

impl std::fmt::Debug for YrsCrdtFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YrsCrdtFactory")
            .field("client_id", &self.client_id.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl YrsCrdtFactory {
    /// Create a new factory that generates random client IDs.
    #[must_use]
    pub fn new() -> Self {
        Self { client_id: None }
    }

    /// Create a factory with a client ID derived from an MLS signing public key.
    ///
    /// This is the recommended constructor for collaborative editing, as it
    /// produces a stable client ID that doesn't change when MLS member indexes
    /// shift due to membership changes.
    #[must_use]
    pub fn with_signing_key(signing_key: &[u8]) -> Self {
        let client_id = client_id_from_signing_key(signing_key);
        Self::with_fixed_client_id(client_id)
    }

    /// Create a factory with a fixed client ID.
    ///
    /// This is useful for testing or when you want deterministic client IDs.
    #[must_use]
    pub fn with_fixed_client_id(client_id: u64) -> Self {
        Self {
            client_id: Some(Arc::new(move || client_id)),
        }
    }

    /// Create a factory with a custom client ID generator.
    #[must_use]
    pub fn with_client_id_generator<F>(generator: F) -> Self
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        Self {
            client_id: Some(Arc::new(generator)),
        }
    }
}

impl CrdtFactory for YrsCrdtFactory {
    fn type_id(&self) -> &str {
        "yrs"
    }

    fn create(&self) -> Box<dyn Crdt> {
        let crdt = if let Some(ref generator) = self.client_id {
            YrsCrdt::with_client_id(generator())
        } else {
            YrsCrdt::new()
        };
        Box::new(crdt)
    }

    fn from_snapshot(&self, snapshot: &[u8]) -> Result<Box<dyn Crdt>, CrdtError> {
        let mut crdt = if let Some(ref generator) = self.client_id {
            YrsCrdt::with_client_id(generator())
        } else {
            YrsCrdt::new()
        };
        crdt.merge(snapshot)?;
        Ok(Box::new(crdt))
    }
}

#[cfg(test)]
mod tests {
    use yrs::types::ToJson;
    use yrs::{Any, GetString, Map, Text, Transact};

    use super::*;

    #[test]
    fn test_yrs_crdt_basic() {
        let crdt = YrsCrdt::new();
        assert_eq!(crdt.type_id(), "yrs");

        // Empty snapshot should work
        let snapshot = crdt.snapshot().unwrap();
        assert!(!snapshot.is_empty()); // Yrs always has some metadata
    }

    #[test]
    fn test_yrs_crdt_text() {
        let mut crdt1 = YrsCrdt::with_client_id(1);
        let mut crdt2 = YrsCrdt::with_client_id(2);

        // Create and modify text in crdt1
        {
            let text = crdt1.doc().get_or_insert_text("my-text");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello, ");
        }

        // Sync to crdt2
        let update = crdt1.snapshot().unwrap();
        crdt2.merge(&update).unwrap();

        // Verify crdt2 has the text
        {
            let text = crdt2.doc().get_or_insert_text("my-text");
            let txn = crdt2.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello, ");
        }

        // Modify in crdt2
        {
            let text = crdt2.doc().get_or_insert_text("my-text");
            let mut txn = crdt2.doc().transact_mut();
            text.insert(&mut txn, 7, "World!");
        }

        // Sync back to crdt1
        let update2 = crdt2.snapshot().unwrap();
        crdt1.merge(&update2).unwrap();

        // Verify crdt1 has both modifications
        {
            let text = crdt1.doc().get_or_insert_text("my-text");
            let txn = crdt1.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello, World!");
        }
    }

    #[test]
    fn test_yrs_crdt_map() {
        let crdt = YrsCrdt::with_client_id(1);

        // Create a map and add some values
        {
            let map = crdt.doc().get_or_insert_map("my-map");
            let mut txn = crdt.doc().transact_mut();
            map.insert(&mut txn, "key1", "value1");
            map.insert(&mut txn, "key2", 42i64);
        }

        // Snapshot and restore
        let snapshot = crdt.snapshot().unwrap();
        let mut crdt2 = YrsCrdt::with_client_id(2);
        crdt2.merge(&snapshot).unwrap();

        // Verify the map contents
        {
            let map = crdt2.doc().get_or_insert_map("my-map");
            let txn = crdt2.doc().transact();
            let json = map.to_json(&txn);

            if let Any::Map(m) = json {
                assert_eq!(m.get("key1"), Some(&Any::String("value1".into())));
                // Numbers are stored as f64 in Yrs JSON representation
                assert_eq!(m.get("key2"), Some(&Any::Number(42.0)));
            } else {
                panic!("Expected map");
            }
        }
    }

    #[test]
    fn test_yrs_factory() {
        let factory = YrsCrdtFactory::new();
        assert_eq!(factory.type_id(), "yrs");

        let crdt = factory.create();
        assert_eq!(crdt.type_id(), "yrs");

        // Test from_snapshot
        let snapshot = crdt.snapshot().unwrap();
        let crdt2 = factory.from_snapshot(&snapshot).unwrap();
        assert_eq!(crdt2.type_id(), "yrs");
    }

    #[test]
    fn test_yrs_factory_with_client_id() {
        let factory = YrsCrdtFactory::with_fixed_client_id(42);

        let crdt = factory.create();
        assert_eq!(crdt.type_id(), "yrs");
    }

    #[test]
    fn test_client_id_from_signing_key() {
        // Test that the same key produces the same client ID
        let key1 = b"test-signing-key-1";
        let key2 = b"test-signing-key-2";

        let id1a = super::client_id_from_signing_key(key1);
        let id1b = super::client_id_from_signing_key(key1);
        let id2 = super::client_id_from_signing_key(key2);

        assert_eq!(id1a, id1b, "Same key should produce same ID");
        assert_ne!(id1a, id2, "Different keys should produce different IDs");
    }

    #[test]
    fn test_yrs_factory_with_signing_key() {
        let key = b"test-mls-signing-key";
        let factory = YrsCrdtFactory::with_signing_key(key);

        let crdt = factory.create();
        assert_eq!(crdt.type_id(), "yrs");

        // Verify the client ID is derived from the key
        let expected_id = super::client_id_from_signing_key(key);
        let factory2 = YrsCrdtFactory::with_fixed_client_id(expected_id);
        let _crdt2 = factory2.create();
        // Both should work (we can't easily verify the internal client ID,
        // but this confirms the factory construction works)
    }

    #[test]
    fn test_concurrent_edits() {
        // Simulate two users editing concurrently
        let mut crdt1 = YrsCrdt::with_client_id(1);
        let mut crdt2 = YrsCrdt::with_client_id(2);

        // Both start with same initial state
        {
            let text = crdt1.doc().get_or_insert_text("doc");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 0, "ABC");
        }
        let initial = crdt1.snapshot().unwrap();
        crdt2.merge(&initial).unwrap();

        // User 1 inserts at position 1
        {
            let text = crdt1.doc().get_or_insert_text("doc");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 1, "X");
        }

        // User 2 inserts at position 2 (concurrently)
        {
            let text = crdt2.doc().get_or_insert_text("doc");
            let mut txn = crdt2.doc().transact_mut();
            text.insert(&mut txn, 2, "Y");
        }

        // Sync both ways
        let update1 = crdt1.snapshot().unwrap();
        let update2 = crdt2.snapshot().unwrap();
        crdt1.merge(&update2).unwrap();
        crdt2.merge(&update1).unwrap();

        // Both should converge to the same state
        let text1 = {
            let text = crdt1.doc().get_or_insert_text("doc");
            let txn = crdt1.doc().transact();
            text.get_string(&txn)
        };
        let text2 = {
            let text = crdt2.doc().get_or_insert_text("doc");
            let txn = crdt2.doc().transact();
            text.get_string(&txn)
        };

        assert_eq!(text1, text2);
        // The exact order depends on Yrs conflict resolution,
        // but both should have all characters
        assert!(text1.contains('A'));
        assert!(text1.contains('B'));
        assert!(text1.contains('C'));
        assert!(text1.contains('X'));
        assert!(text1.contains('Y'));
    }
}
