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

use std::sync::Arc;

use universal_sync_core::{Crdt, CrdtError, CrdtFactory};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

/// A CRDT implementation backed by a Yrs document.
///
/// This provides access to the full Yrs document API for creating and
/// manipulating shared data types like Text, Array, Map, etc.
pub struct YrsCrdt {
    doc: Doc,
    /// State vector at the time of the last flush (for computing diffs)
    last_flushed_sv: StateVector,
}

impl YrsCrdt {
    /// Create a new empty Yrs document.
    #[must_use]
    pub fn new() -> Self {
        let doc = Doc::new();
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            last_flushed_sv,
        }
    }

    /// Create a Yrs document with a specific client ID.
    ///
    /// Client IDs are used to identify the source of operations.
    /// In a group, each member should have a unique client ID.
    #[must_use]
    pub fn with_client_id(client_id: u64) -> Self {
        let doc = Doc::with_client_id(client_id);
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            last_flushed_sv,
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn apply(&mut self, operation: &[u8]) -> Result<(), CrdtError> {
        let update = Update::decode_v1(operation)
            .map_err(|e| CrdtError::new(format!("decode error: {e}")))?;

        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|e| CrdtError::new(format!("apply error: {e}")))?;

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

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, CrdtError> {
        let txn = self.doc.transact();
        let current_sv = txn.state_vector();
        if current_sv == self.last_flushed_sv {
            return Ok(None);
        }
        let update = txn.encode_diff_v1(&self.last_flushed_sv);
        drop(txn);
        self.last_flushed_sv = current_sv;
        Ok(Some(update))
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
        assert_eq!(Crdt::type_id(&crdt), "yrs");

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
        assert_eq!(CrdtFactory::type_id(&factory), "yrs");

        let crdt = factory.create();
        assert_eq!(Crdt::type_id(&*crdt), "yrs");

        // Test from_snapshot
        let snapshot = crdt.snapshot().unwrap();
        let crdt2 = factory.from_snapshot(&snapshot).unwrap();
        assert_eq!(Crdt::type_id(&*crdt2), "yrs");
    }

    #[test]
    fn test_yrs_factory_with_client_id() {
        let factory = YrsCrdtFactory::with_fixed_client_id(42);

        let crdt = factory.create();
        assert_eq!(Crdt::type_id(&*crdt), "yrs");
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
