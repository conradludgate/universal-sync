//! Yrs (Yjs) CRDT implementation — wraps `yrs::Doc` for the [`Crdt`] trait.
//!
//! Wire messages use `postcard` serialisation of [`CrdtMessage`], which pairs
//! a [`PeerAwareness`] header with an optional Yrs V2 doc update.
//! Snapshots (`snapshot` / `merge`) use raw (unwrapped) Yrs bytes.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use error_stack::{Report, ResultExt};
use filament_core::{Crdt, CrdtError};
use serde::{Deserialize, Serialize};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerAwareness {
    pub client_id: u64,
    pub cursor: Option<u32>,
    pub selection_end: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct CrdtMessage {
    pub awareness: PeerAwareness,
    #[serde(with = "option_yrs_update")]
    pub doc_update: Option<Vec<u8>>,
}

mod option_yrs_update {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use yrs::updates::decoder::Decode;

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        v.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let v: Option<Vec<u8>> = Option::deserialize(d)?;
        if let Some(ref bytes) = v {
            yrs::Update::decode_v2(bytes).map_err(serde::de::Error::custom)?;
        }
        Ok(v)
    }
}

pub const AWARENESS_TIMEOUT: Duration = Duration::from_secs(10);

pub struct YrsCrdt {
    doc: Doc,
    client_id: u64,
    last_flushed_sv: StateVector,
    dirty: bool,
    awareness_states: HashMap<u64, PeerAwareness>,
    awareness_last_seen: HashMap<u64, Instant>,
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
            awareness_last_seen: HashMap::new(),
            awareness_dirty: false,
        }
    }

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
            awareness_last_seen: HashMap::new(),
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

    pub fn set_cursor(&mut self, cursor: u32, selection_end: u32) {
        self.awareness_states.insert(
            self.client_id,
            PeerAwareness {
                client_id: self.client_id,
                cursor: Some(cursor),
                selection_end: Some(selection_end),
            },
        );
        self.awareness_dirty = true;
    }

    pub fn clear_local_state(&mut self) {
        self.awareness_states.insert(
            self.client_id,
            PeerAwareness {
                client_id: self.client_id,
                cursor: None,
                selection_end: None,
            },
        );
        self.awareness_dirty = true;
    }

    #[must_use]
    pub fn awareness_states(&self) -> &HashMap<u64, PeerAwareness> {
        &self.awareness_states
    }

    /// Remove peers that haven't sent an awareness update within `timeout`.
    /// Returns `true` if any peers were removed.
    pub fn expire_stale_peers(&mut self, timeout: Duration) -> bool {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .awareness_last_seen
            .iter()
            .filter(|(cid, seen)| **cid != self.client_id && now.duration_since(**seen) > timeout)
            .map(|(cid, _)| *cid)
            .collect();
        let changed = !stale.is_empty();
        for cid in stale {
            self.awareness_states.remove(&cid);
            self.awareness_last_seen.remove(&cid);
        }
        changed
    }

    fn local_awareness(&self) -> PeerAwareness {
        self.awareness_states
            .get(&self.client_id)
            .cloned()
            .unwrap_or(PeerAwareness {
                client_id: self.client_id,
                cursor: None,
                selection_end: None,
            })
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
        let msg: CrdtMessage = postcard::from_bytes(operation).change_context(CrdtError)?;

        if let Some(update_bytes) = msg.doc_update {
            let update = Update::decode_v2(&update_bytes).change_context(CrdtError)?;
            self.doc
                .transact_mut()
                .apply_update(update)
                .change_context(CrdtError)?;
        }

        self.awareness_last_seen
            .insert(msg.awareness.client_id, Instant::now());
        self.awareness_states
            .insert(msg.awareness.client_id, msg.awareness);

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
        let msg = CrdtMessage {
            awareness: self.local_awareness(),
            doc_update: Some(snapshot),
        };
        postcard::to_allocvec(&msg).change_context(CrdtError)
    }

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
        if !self.dirty && !self.awareness_dirty {
            return Ok(None);
        }

        let doc_update = if self.dirty {
            self.dirty = false;
            let txn = self.doc.transact();
            let current_sv = txn.state_vector();
            let update = txn.encode_diff_v2(&self.last_flushed_sv);
            drop(txn);
            self.last_flushed_sv = current_sv;
            Some(update)
        } else {
            None
        };

        self.awareness_dirty = false;

        let msg = CrdtMessage {
            awareness: self.local_awareness(),
            doc_update,
        };
        let encoded = postcard::to_allocvec(&msg).change_context(CrdtError)?;
        Ok(Some(encoded))
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

        let insert_and_flush = |crdt: &mut YrsCrdt, pos: u32, text: &str| {
            let t = crdt.doc().get_or_insert_text("doc");
            let mut txn = crdt.doc().transact_mut();
            t.insert(&mut txn, pos, text);
            drop(txn);
            crdt.mark_dirty();
            crdt.flush_update().unwrap().unwrap()
        };

        insert_and_flush(&mut crdt, 0, "aaa");
        insert_and_flush(&mut crdt, 3, "bbb");
        insert_and_flush(&mut crdt, 6, "ccc");

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
    fn test_postcard_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        {
            let text = alice.doc().get_or_insert_text("doc");
            let mut txn = alice.doc().transact_mut();
            text.insert(&mut txn, 0, "Hello");
        }
        alice.mark_dirty();
        let update = alice.flush_update().unwrap().unwrap();

        let msg: CrdtMessage = postcard::from_bytes(&update).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.client_id, 1);

        bob.apply(&update).unwrap();
        let text = bob.doc().get_or_insert_text("doc");
        let txn = bob.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello");
    }

    #[test]
    fn test_awareness_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        alice.set_cursor(5, 5);
        let awareness_msg = alice.flush_update().unwrap().unwrap();

        let msg: CrdtMessage = postcard::from_bytes(&awareness_msg).unwrap();
        assert!(msg.doc_update.is_none());
        assert_eq!(msg.awareness.cursor, Some(5));

        bob.apply(&awareness_msg).unwrap();
        let peer = bob.awareness_states().get(&1).unwrap();
        assert_eq!(peer.cursor, Some(5));
        assert_eq!(peer.selection_end, Some(5));
    }

    #[test]
    fn test_awareness_clear() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        alice.set_cursor(0, 0);
        let msg1 = alice.flush_update().unwrap().unwrap();
        bob.apply(&msg1).unwrap();
        assert!(bob.awareness_states().get(&1).unwrap().cursor.is_some());

        alice.clear_local_state();
        let msg2 = alice.flush_update().unwrap().unwrap();
        bob.apply(&msg2).unwrap();
        assert!(bob.awareness_states().get(&1).unwrap().cursor.is_none());
    }

    #[test]
    fn test_flush_returns_combined_then_none() {
        let mut crdt = YrsCrdt::with_client_id(1);

        {
            let text = crdt.doc().get_or_insert_text("doc");
            let mut txn = crdt.doc().transact_mut();
            text.insert(&mut txn, 0, "hi");
        }
        crdt.mark_dirty();
        crdt.set_cursor(2, 2);

        let first = crdt.flush_update().unwrap().unwrap();
        let msg: CrdtMessage = postcard::from_bytes(&first).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.cursor, Some(2));

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
        alice.set_cursor(5, 5);
        let update = alice.flush_update().unwrap().unwrap();

        let mut temp = YrsCrdt::with_client_id(0);
        temp.apply(&update).unwrap();
        assert!(temp.awareness_states().contains_key(&1));

        let compacted = temp.wire_snapshot().unwrap();

        let mut fresh = YrsCrdt::with_client_id(99);
        fresh.apply(&compacted).unwrap();
        // wire_snapshot carries temp's awareness (client 0), not alice's
        assert!(!fresh.awareness_states().contains_key(&1));

        let text = fresh.doc().get_or_insert_text("doc");
        let txn = fresh.doc().transact();
        assert_eq!(text.get_string(&txn), "Hello");
    }

    #[test]
    fn test_wire_snapshot_is_postcard() {
        let crdt = YrsCrdt::with_client_id(1);
        let wire = crdt.wire_snapshot().unwrap();
        let msg: CrdtMessage = postcard::from_bytes(&wire).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.client_id, 1);
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
    fn test_apply_rejects_garbage() {
        let mut crdt = YrsCrdt::with_client_id(1);
        assert!(crdt.apply(&[0x42, 0x00]).is_err());
    }

    /// yrs 0.25: V2 delete markers silently truncate the *referenced item's*
    /// client ID to 32 bits. Inserts, full-state diffs, and the deleting
    /// client's own ID are all fine — only the delete target reference breaks.
    ///
    /// - Items created by a >32-bit client can be inserted and synced correctly.
    /// - Deleting those items produces a diff that decodes without error but
    ///   silently refers to the wrong (truncated) client ID, so the delete
    ///   is a no-op on the receiving peer.
    ///
    /// Workaround: mask `as_client_id()` to 32 bits (`& 0xFFFF_FFFF`).
    #[test]
    fn test_v2_delete_truncates_large_client_id() {
        let large_cid: u64 = 0x0000_0001_CAFE_BABE; // 33 bits
        let small_cid: u64 = 42;

        // Insert with large CID works
        let doc = Doc::with_client_id(large_cid);
        let t = doc.get_or_insert_text("doc");
        t.insert(&mut doc.transact_mut(), 0, "Hello");
        let full = doc.transact().encode_diff_v2(&StateVector::default());
        let doc2 = Doc::new();
        doc2.get_or_insert_text("doc");
        doc2.transact_mut()
            .apply_update(Update::decode_v2(&full).unwrap())
            .unwrap();
        assert_eq!(
            doc2.get_or_insert_text("doc").get_string(&doc2.transact()),
            "Hello",
            "insert with large CID should work"
        );

        // Delete of items created by large CID: diff decodes but delete is no-op
        let sv = doc.transact().state_vector();
        t.remove_range(&mut doc.transact_mut(), 0, 5);
        let del = doc.transact().encode_diff_v2(&sv);
        // Decodes successfully — no error
        let del_update = Update::decode_v2(&del).unwrap();
        doc2.transact_mut().apply_update(del_update).unwrap();
        // BUG: delete silently failed — text is still "Hello"
        assert_eq!(
            doc2.get_or_insert_text("doc").get_string(&doc2.transact()),
            "Hello",
            "yrs bug: delete of >32-bit CID items is silently ignored"
        );

        // When a small-CID client deletes items created by a large-CID client,
        // the same bug occurs: the delete target reference is truncated.
        let bob = Doc::with_client_id(small_cid);
        bob.get_or_insert_text("doc");
        bob.transact_mut()
            .apply_update(Update::decode_v2(&full).unwrap())
            .unwrap();
        let sv_b = bob.transact().state_vector();
        bob.get_or_insert_text("doc")
            .remove_range(&mut bob.transact_mut(), 0, 5);
        let del_b = bob.transact().encode_diff_v2(&sv_b);
        let charlie = Doc::with_client_id(99);
        charlie.get_or_insert_text("doc");
        charlie
            .transact_mut()
            .apply_update(Update::decode_v2(&full).unwrap())
            .unwrap();
        charlie
            .transact_mut()
            .apply_update(Update::decode_v2(&del_b).unwrap())
            .unwrap();
        assert_eq!(
            charlie
                .get_or_insert_text("doc")
                .get_string(&charlie.transact()),
            "Hello",
            "yrs bug: small-CID client deleting large-CID items also silently fails"
        );

        // Sanity: 32-bit CID deletes work correctly
        let doc32 = Doc::with_client_id(0xCAFE_BABE);
        let t32 = doc32.get_or_insert_text("doc");
        t32.insert(&mut doc32.transact_mut(), 0, "Hello");
        let full32 = doc32.transact().encode_diff_v2(&StateVector::default());
        let sv32 = doc32.transact().state_vector();
        t32.remove_range(&mut doc32.transact_mut(), 0, 5);
        let del32 = doc32.transact().encode_diff_v2(&sv32);

        let peer32 = Doc::new();
        peer32.get_or_insert_text("doc");
        peer32
            .transact_mut()
            .apply_update(Update::decode_v2(&full32).unwrap())
            .unwrap();
        peer32
            .transact_mut()
            .apply_update(Update::decode_v2(&del32).unwrap())
            .unwrap();
        assert_eq!(
            peer32
                .get_or_insert_text("doc")
                .get_string(&peer32.transact()),
            "",
            "32-bit CID deletes should work"
        );
    }
}
