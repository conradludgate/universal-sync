//! Yrs (Yjs) CRDT implementation — wraps `yrs::Doc` for the [`Crdt`] trait.
//!
//! Wire messages use a prefix byte to distinguish document updates (`0x00`)
//! from ephemeral awareness updates (`0x01`). Snapshots and merges use raw
//! (unprefixed) yrs bytes. See [`DOC_PREFIX`] and [`AWARENESS_PREFIX`].

use std::collections::HashMap;

use error_stack::{Report, ResultExt};
use universal_sync_core::{Crdt, CrdtError};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

pub const DOC_PREFIX: u8 = 0x00;
pub const AWARENESS_PREFIX: u8 = 0x01;

pub struct YrsCrdt {
    doc: Doc,
    client_id: u64,
    /// For computing diffs since the last flush
    last_flushed_sv: StateVector,
    dirty: bool,
    /// Ephemeral per-peer state keyed by client ID.
    awareness_states: HashMap<u64, String>,
    awareness_dirty: bool,
}

impl YrsCrdt {
    #[must_use]
    pub fn new() -> Self {
        let doc = Doc::new();
        let client_id = doc.client_id();
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            client_id,
            last_flushed_sv,
            dirty: false,
            awareness_states: HashMap::new(),
            awareness_dirty: false,
        }
    }

    /// Each member in a group should have a unique client ID.
    #[must_use]
    pub fn with_client_id(client_id: u64) -> Self {
        let doc = Doc::with_client_id(client_id);
        let last_flushed_sv = doc.transact().state_vector();
        Self {
            doc,
            client_id,
            last_flushed_sv,
            dirty: false,
            awareness_states: HashMap::new(),
            awareness_dirty: false,
        }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
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
        Ok(txn.encode_diff_v2(sv))
    }

    /// # Errors
    ///
    /// Returns [`CrdtError`] if the snapshot bytes cannot be decoded.
    pub fn from_snapshot(snapshot: &[u8], client_id: u64) -> Result<Self, Report<CrdtError>> {
        let mut crdt = Self::with_client_id(client_id);
        crdt.merge(snapshot)?;
        Ok(crdt)
    }

    pub fn set_local_state(&mut self, json: &str) {
        self.awareness_states
            .insert(self.client_id, json.to_string());
        self.awareness_dirty = true;
    }

    pub fn clear_local_state(&mut self) {
        self.awareness_states.remove(&self.client_id);
        self.awareness_dirty = true;
    }

    #[must_use]
    pub fn awareness_states(&self) -> &HashMap<u64, String> {
        &self.awareness_states
    }
}

impl Default for YrsCrdt {
    fn default() -> Self {
        Self::new()
    }
}

impl Crdt for YrsCrdt {
    fn protocol_name(&self) -> &str {
        "yrs"
    }

    fn apply(&mut self, operation: &[u8]) -> Result<(), Report<CrdtError>> {
        match operation.first() {
            Some(&DOC_PREFIX) => {
                let update = Update::decode_v2(&operation[1..]).change_context(CrdtError)?;
                self.doc
                    .transact_mut()
                    .apply_update(update)
                    .change_context(CrdtError)?;
            }
            Some(&AWARENESS_PREFIX) => {
                if operation.len() >= 9 {
                    let client_id =
                        u64::from_le_bytes(operation[1..9].try_into().expect("8 bytes"));
                    let state = std::str::from_utf8(&operation[9..]).unwrap_or("");
                    if state.is_empty() {
                        self.awareness_states.remove(&client_id);
                    } else {
                        self.awareness_states.insert(client_id, state.to_string());
                    }
                }
            }
            _ => return Err(Report::new(CrdtError).attach("unknown CRDT message prefix")),
        }
        Ok(())
    }

    fn merge(&mut self, snapshot: &[u8]) -> Result<(), Report<CrdtError>> {
        let update = Update::decode_v2(snapshot).change_context(CrdtError)?;
        self.doc
            .transact_mut()
            .apply_update(update)
            .change_context(CrdtError)?;
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        let txn = self.doc.transact();
        Ok(txn.encode_state_as_update_v2(&StateVector::default()))
    }

    fn wire_snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        let snapshot = self.snapshot()?;
        let mut result = Vec::with_capacity(1 + snapshot.len());
        result.push(DOC_PREFIX);
        result.extend_from_slice(&snapshot);
        Ok(result)
    }

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
        if self.dirty {
            self.dirty = false;
            let txn = self.doc.transact();
            let current_sv = txn.state_vector();
            let update = txn.encode_diff_v2(&self.last_flushed_sv);
            drop(txn);
            self.last_flushed_sv = current_sv;
            let mut result = Vec::with_capacity(1 + update.len());
            result.push(DOC_PREFIX);
            result.extend_from_slice(&update);
            return Ok(Some(result));
        }
        if self.awareness_dirty {
            self.awareness_dirty = false;
            let state = self
                .awareness_states
                .get(&self.client_id)
                .cloned()
                .unwrap_or_default();
            let mut result = Vec::with_capacity(1 + 8 + state.len());
            result.push(AWARENESS_PREFIX);
            result.extend_from_slice(&self.client_id.to_le_bytes());
            result.extend_from_slice(state.as_bytes());
            return Ok(Some(result));
        }
        Ok(None)
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
        assert_eq!(Crdt::protocol_name(&crdt), "yrs");

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
    fn test_from_snapshot_inherent() {
        let crdt = YrsCrdt::with_client_id(1);
        {
            let text = crdt.doc().get_or_insert_text("doc");
            let mut txn = crdt.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }
        let snapshot = crdt.snapshot().unwrap();

        let crdt2 = YrsCrdt::from_snapshot(&snapshot, 2).unwrap();
        let text = crdt2.doc().get_or_insert_text("doc");
        let txn = crdt2.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello");
    }

    #[test]
    fn test_compact_multiple_updates() {
        let mut crdt = YrsCrdt::with_client_id(1);

        let mut updates = Vec::new();
        let insert_and_flush = |crdt: &mut YrsCrdt, pos: u32, text: &str| {
            let t = crdt.doc().get_or_insert_text("doc");
            let mut txn = crdt.doc().transact_mut();
            t.insert(&mut txn, pos, text);
            drop(txn);
            crdt.mark_dirty();
            crdt.flush_update().unwrap().unwrap()
        };

        updates.push(insert_and_flush(&mut crdt, 0, "aaa"));
        updates.push(insert_and_flush(&mut crdt, 3, "bbb"));
        updates.push(insert_and_flush(&mut crdt, 6, "ccc"));

        let snapshot = crdt.snapshot().unwrap();
        let fresh = YrsCrdt::from_snapshot(&snapshot, 99).unwrap();
        let text = fresh.doc().get_or_insert_text("doc");
        let txn = fresh.doc().transact();
        assert_eq!(text.get_string(&txn), "aaabbbccc");
    }

    #[test]
    fn test_from_snapshot_empty_bytes_fails() {
        assert!(
            YrsCrdt::from_snapshot(&[], 0).is_err(),
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

    #[test]
    fn test_echo_idempotent() {
        let mut alice = YrsCrdt::with_client_id(12345);

        {
            let text = alice.doc().get_or_insert_text("doc");
            let mut txn = alice.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }

        alice.mark_dirty();
        let diff = alice.flush_update().unwrap().unwrap();
        alice.apply(&diff).unwrap();

        {
            let text = alice.doc().get_or_insert_text("doc");
            let txn = alice.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello", "echo should be idempotent");
        }
    }

    #[test]
    fn test_two_peer_with_deterministic_ids() {
        let mut alice_crdt = YrsCrdt::with_client_id(111);
        let mut bob_crdt = YrsCrdt::with_client_id(222);

        {
            let text = alice_crdt.doc().get_or_insert_text("doc");
            let mut txn = alice_crdt.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }
        alice_crdt.mark_dirty();
        let alice_update = alice_crdt.flush_update().unwrap().unwrap();

        bob_crdt.apply(&alice_update).unwrap();
        {
            let text = bob_crdt.doc().get_or_insert_text("doc");
            let txn = bob_crdt.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello");
        }

        alice_crdt.apply(&alice_update).unwrap();
        {
            let text = alice_crdt.doc().get_or_insert_text("doc");
            let txn = alice_crdt.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello", "echo should not duplicate");
        }

        {
            let text = bob_crdt.doc().get_or_insert_text("doc");
            let mut txn = bob_crdt.doc().transact_mut();
            text.insert(&mut txn, 5, " World");
        }
        bob_crdt.mark_dirty();
        let bob_update = bob_crdt.flush_update().unwrap().unwrap();

        alice_crdt.apply(&bob_update).unwrap();
        {
            let text = alice_crdt.doc().get_or_insert_text("doc");
            let txn = alice_crdt.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello World");
        }

        let mut alice2 = YrsCrdt::with_client_id(111);
        alice2.apply(&bob_update).unwrap();
        alice2.apply(&alice_update).unwrap();
        {
            let text = alice2.doc().get_or_insert_text("doc");
            let txn = alice2.doc().transact();
            assert_eq!(text.get_string(&txn), "Hello World");
        }
    }

    #[test]
    fn test_prefix_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        {
            let text = alice.doc().get_or_insert_text("doc");
            let mut txn = alice.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }
        alice.mark_dirty();
        let update = alice.flush_update().unwrap().unwrap();
        assert_eq!(update[0], DOC_PREFIX);
        bob.apply(&update).unwrap();

        let text = bob.doc().get_or_insert_text("doc");
        let txn = bob.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello");
    }

    #[test]
    fn test_awareness_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        alice.set_local_state(r#"{"cursor":5}"#);
        let awareness_msg = alice.flush_update().unwrap().unwrap();
        assert_eq!(awareness_msg[0], AWARENESS_PREFIX);

        bob.apply(&awareness_msg).unwrap();
        assert_eq!(bob.awareness_states().get(&1).unwrap(), r#"{"cursor":5}"#);
    }

    #[test]
    fn test_awareness_clear() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        alice.set_local_state(r#"{"cursor":0}"#);
        let msg1 = alice.flush_update().unwrap().unwrap();
        bob.apply(&msg1).unwrap();
        assert!(bob.awareness_states().contains_key(&1));

        alice.clear_local_state();
        let msg2 = alice.flush_update().unwrap().unwrap();
        bob.apply(&msg2).unwrap();
        assert!(!bob.awareness_states().contains_key(&1));
    }

    #[test]
    fn test_flush_returns_doc_then_awareness_then_none() {
        let mut crdt = YrsCrdt::with_client_id(1);

        {
            let text = crdt.doc().get_or_insert_text("doc");
            let mut txn = crdt.doc().transact_mut();
            text.insert(&mut txn, 0, "hi");
        }
        crdt.mark_dirty();
        crdt.set_local_state(r#"{"cursor":2}"#);

        let first = crdt.flush_update().unwrap().unwrap();
        assert_eq!(first[0], DOC_PREFIX);

        let second = crdt.flush_update().unwrap().unwrap();
        assert_eq!(second[0], AWARENESS_PREFIX);

        assert!(crdt.flush_update().unwrap().is_none());
    }

    #[test]
    fn test_awareness_excluded_from_compaction() {
        let mut alice = YrsCrdt::with_client_id(1);

        {
            let text = alice.doc().get_or_insert_text("doc");
            let mut txn = alice.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }
        alice.mark_dirty();
        let doc_update = alice.flush_update().unwrap().unwrap();

        alice.set_local_state(r#"{"cursor":5}"#);
        let awareness_update = alice.flush_update().unwrap().unwrap();

        // Simulate compaction: create a temporary CRDT, apply both updates,
        // then wire_snapshot(). The result should contain no awareness data.
        let mut temp = YrsCrdt::with_client_id(0);
        temp.apply(&doc_update).unwrap();
        temp.apply(&awareness_update).unwrap();
        // Awareness was parsed into temp, but snapshot ignores it
        assert!(temp.awareness_states().contains_key(&1));

        let compacted = temp.wire_snapshot().unwrap();
        assert_eq!(compacted[0], DOC_PREFIX);

        // Apply compacted snapshot to a fresh CRDT — no awareness leaks
        let mut fresh = YrsCrdt::with_client_id(99);
        fresh.apply(&compacted).unwrap();
        assert!(fresh.awareness_states().is_empty());

        let text = fresh.doc().get_or_insert_text("doc");
        let txn = fresh.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello");
    }

    #[test]
    fn test_wire_snapshot_is_prefixed() {
        let crdt = YrsCrdt::with_client_id(1);
        let raw = crdt.snapshot().unwrap();
        let wire = crdt.wire_snapshot().unwrap();
        assert_eq!(wire[0], DOC_PREFIX);
        assert_eq!(&wire[1..], &raw);
    }

    #[test]
    fn test_merge_takes_raw_bytes() {
        let crdt1 = YrsCrdt::with_client_id(1);
        {
            let text = crdt1.doc().get_or_insert_text("doc");
            let mut txn = crdt1.doc().transact_mut();
            text.insert(&mut txn, 0, "raw");
        }
        let raw = crdt1.snapshot().unwrap();

        let mut crdt2 = YrsCrdt::with_client_id(2);
        crdt2.merge(&raw).unwrap();
        let text = crdt2.doc().get_or_insert_text("doc");
        let txn = crdt2.doc().transact();
        assert_eq!(text.get_string(&txn), "raw");
    }

    #[test]
    fn test_apply_rejects_unknown_prefix() {
        let mut crdt = YrsCrdt::with_client_id(1);
        assert!(crdt.apply(&[0x42, 0x00]).is_err());
    }
}
