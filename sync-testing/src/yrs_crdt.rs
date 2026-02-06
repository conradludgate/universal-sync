//! Yrs (Yjs) CRDT implementation — wraps `yrs::Doc` for the [`Crdt`] trait.

use std::sync::Arc;

use error_stack::{Report, ResultExt};
use universal_sync_core::{CompactionConfig, Crdt, CrdtError, CrdtFactory};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

pub struct YrsCrdt {
    doc: Doc,
    /// For computing diffs since the last flush
    last_flushed_sv: StateVector,
}

impl YrsCrdt {
    #[must_use]
    pub fn new() -> Self {
        let doc = Doc::new();
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            last_flushed_sv,
        }
    }

    /// Each member in a group should have a unique client ID.
    #[must_use]
    pub fn with_client_id(client_id: u64) -> Self {
        let doc = Doc::with_client_id(client_id);
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            last_flushed_sv,
        }
    }

    #[must_use]
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    pub fn doc_mut(&mut self) -> &mut Doc {
        &mut self.doc
    }

    #[must_use]
    pub fn state_vector(&self) -> StateVector {
        self.doc.transact().state_vector()
    }

    pub fn encode_diff(&self, sv: &StateVector) -> Result<Vec<u8>, Report<CrdtError>> {
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

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn apply(&mut self, operation: &[u8]) -> Result<(), Report<CrdtError>> {
        let update = Update::decode_v1(operation).change_context(CrdtError)?;

        self.doc
            .transact_mut()
            .apply_update(update)
            .change_context(CrdtError)?;

        Ok(())
    }

    fn merge(&mut self, snapshot: &[u8]) -> Result<(), Report<CrdtError>> {
        self.apply(snapshot)
    }

    fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        let txn = self.doc.transact();
        Ok(txn.encode_state_as_update_v1(&StateVector::default()))
    }

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
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

#[derive(Default, Clone)]
pub struct YrsCrdtFactory {
    client_id: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    compaction_config: Option<CompactionConfig>,
}

impl std::fmt::Debug for YrsCrdtFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YrsCrdtFactory")
            .field("client_id", &self.client_id.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl YrsCrdtFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            client_id: None,
            compaction_config: None,
        }
    }

    #[must_use]
    pub fn with_fixed_client_id(client_id: u64) -> Self {
        Self {
            client_id: Some(Arc::new(move || client_id)),
            compaction_config: None,
        }
    }

    #[must_use]
    pub fn with_client_id_generator<F>(generator: F) -> Self
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        Self {
            client_id: Some(Arc::new(generator)),
            compaction_config: None,
        }
    }

    /// Override the default compaction config (useful for tests with lower thresholds).
    #[must_use]
    pub fn with_compaction_config(mut self, config: CompactionConfig) -> Self {
        self.compaction_config = Some(config);
        self
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

    fn from_snapshot(&self, snapshot: &[u8]) -> Result<Box<dyn Crdt>, Report<CrdtError>> {
        let mut crdt = if let Some(ref generator) = self.client_id {
            YrsCrdt::with_client_id(generator())
        } else {
            YrsCrdt::new()
        };
        crdt.merge(snapshot)?;
        Ok(Box::new(crdt))
    }

    fn compaction_config(&self) -> CompactionConfig {
        self.compaction_config.clone().unwrap_or_default()
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

        let snapshot = crdt.snapshot().unwrap();
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_yrs_crdt_text() {
        let mut crdt1 = YrsCrdt::with_client_id(1);
        let mut crdt2 = YrsCrdt::with_client_id(2);

        {
            let text = crdt1.doc().get_or_insert_text("my-text");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello, ");
        }

        let update = crdt1.snapshot().unwrap();
        crdt2.merge(&update).unwrap();

        {
            let text = crdt2.doc().get_or_insert_text("my-text");
            let txn = crdt2.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello, ");
        }

        {
            let text = crdt2.doc().get_or_insert_text("my-text");
            let mut txn = crdt2.doc().transact_mut();
            text.insert(&mut txn, 7, "World!");
        }

        let update2 = crdt2.snapshot().unwrap();
        crdt1.merge(&update2).unwrap();

        {
            let text = crdt1.doc().get_or_insert_text("my-text");
            let txn = crdt1.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello, World!");
        }
    }

    #[test]
    fn test_yrs_crdt_map() {
        let crdt = YrsCrdt::with_client_id(1);

        {
            let map = crdt.doc().get_or_insert_map("my-map");
            let mut txn = crdt.doc().transact_mut();
            map.insert(&mut txn, "key1", "value1");
            map.insert(&mut txn, "key2", 42i64);
        }

        let snapshot = crdt.snapshot().unwrap();
        let mut crdt2 = YrsCrdt::with_client_id(2);
        crdt2.merge(&snapshot).unwrap();

        {
            let map = crdt2.doc().get_or_insert_map("my-map");
            let txn = crdt2.doc().transact();
            let json = map.to_json(&txn);

            if let Any::Map(m) = json {
                assert_eq!(m.get("key1"), Some(&Any::String("value1".into())));
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
    fn test_compact_multiple_updates() {
        let factory = YrsCrdtFactory::with_fixed_client_id(1);
        let mut crdt = factory.create();

        let mut updates = Vec::new();
        let insert_and_flush = |crdt: &mut Box<dyn Crdt>, pos: u32, text: &str| {
            let yrs = crdt.as_any_mut().downcast_mut::<YrsCrdt>().unwrap();
            let t = yrs.doc().get_or_insert_text("doc");
            let mut txn = yrs.doc().transact_mut();
            t.insert(&mut txn, pos, text);
            drop(txn);
            drop(t);
            crdt.flush_update().unwrap().unwrap()
        };

        updates.push(insert_and_flush(&mut crdt, 0, "aaa"));
        updates.push(insert_and_flush(&mut crdt, 3, "bbb"));
        updates.push(insert_and_flush(&mut crdt, 6, "ccc"));

        let refs: Vec<&[u8]> = updates.iter().map(|u| u.as_slice()).collect();
        let compacted = factory.compact(None, &refs).unwrap();

        let fresh = factory.from_snapshot(&compacted).unwrap();
        let yrs = fresh.as_any().downcast_ref::<YrsCrdt>().unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let txn = yrs.doc().transact();
        assert_eq!(text.get_string(&txn), "aaabbbccc");
    }

    #[test]
    fn test_compact_with_base_snapshot() {
        let factory = YrsCrdtFactory::with_fixed_client_id(1);
        let mut crdt = factory.create();

        // Create base content
        {
            let yrs = crdt.as_any_mut().downcast_mut::<YrsCrdt>().unwrap();
            let t = yrs.doc().get_or_insert_text("doc");
            let mut txn = yrs.doc().transact_mut();
            t.insert(&mut txn, 0, "base");
        }
        let base = crdt.snapshot().unwrap();

        // Create an incremental update
        {
            let yrs = crdt.as_any_mut().downcast_mut::<YrsCrdt>().unwrap();
            let t = yrs.doc().get_or_insert_text("doc");
            let mut txn = yrs.doc().transact_mut();
            t.insert(&mut txn, 4, "+inc");
        }
        let inc = crdt.flush_update().unwrap().unwrap();

        let compacted = factory.compact(Some(&base), &[&inc]).unwrap();

        let fresh = factory.from_snapshot(&compacted).unwrap();
        let yrs = fresh.as_any().downcast_ref::<YrsCrdt>().unwrap();
        let text = yrs.doc().get_or_insert_text("doc");
        let txn = yrs.doc().transact();
        assert_eq!(text.get_string(&txn), "base+inc");
    }

    #[test]
    fn test_compact_empty_updates() {
        let factory = YrsCrdtFactory::new();
        let compacted = factory.compact(None, &[]).unwrap();
        let _fresh = factory.from_snapshot(&compacted).unwrap();
    }

    #[test]
    fn test_from_snapshot_empty_bytes_fails() {
        let factory = YrsCrdtFactory::new();
        assert!(
            factory.from_snapshot(&[]).is_err(),
            "empty bytes should fail to decode as a yrs update"
        );
    }

    #[test]
    fn test_concurrent_edits() {
        let mut crdt1 = YrsCrdt::with_client_id(1);
        let mut crdt2 = YrsCrdt::with_client_id(2);

        {
            let text = crdt1.doc().get_or_insert_text("doc");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 0, "ABC");
        }
        let initial = crdt1.snapshot().unwrap();
        crdt2.merge(&initial).unwrap();

        {
            let text = crdt1.doc().get_or_insert_text("doc");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 1, "X");
        }

        {
            let text = crdt2.doc().get_or_insert_text("doc");
            let mut txn = crdt2.doc().transact_mut();
            text.insert(&mut txn, 2, "Y");
        }

        let update1 = crdt1.snapshot().unwrap();
        let update2 = crdt2.snapshot().unwrap();
        crdt1.merge(&update2).unwrap();
        crdt2.merge(&update1).unwrap();

        // Both should converge
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
        assert!(text1.contains('A'));
        assert!(text1.contains('B'));
        assert!(text1.contains('C'));
        assert!(text1.contains('X'));
        assert!(text1.contains('Y'));
    }
}
