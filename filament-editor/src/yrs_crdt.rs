//! Yrs (Yjs) CRDT implementation — wraps `yrs::Doc` for the [`Crdt`] trait.
//!
//! Wire messages use `postcard` serialisation of [`CrdtMessage`], which pairs
//! a [`PeerAwareness`] header with an optional Yrs V2 doc update.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use error_stack::{Report, ResultExt};
use filament_core::{Crdt, CrdtError};
use serde::{Deserialize, Serialize};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, Snapshot, StateVector, Transact, Update};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerAwareness {
    pub client_id: u64,
    pub cursor: Option<u32>,
    pub selection_end: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CrdtMessage {
    pub(crate) awareness: PeerAwareness,
    #[serde(with = "option_yrs_update")]
    pub(crate) doc_update: Option<Vec<u8>>,
}

mod option_yrs_update {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use yrs::updates::decoder::Decode;

    pub(crate) fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        v.serialize(s)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        let v: Option<Vec<u8>> = Option::deserialize(d)?;
        if let Some(ref bytes) = v {
            yrs::Update::decode_v2(bytes).map_err(serde::de::Error::custom)?;
        }
        Ok(v)
    }
}

pub struct YrsCrdt {
    doc: Doc,
    client_id: u64,
    /// Snapshots confirmed received by a spool, per level.
    /// A `Snapshot` tracks both inserts (state vector) and deletes (delete set).
    flushed: Vec<Snapshot>,
    /// Snapshots of the last flush attempt, per level.
    /// Equal to flushed when nothing is in-flight.
    inflight: Vec<Snapshot>,
    awareness_states: HashMap<u64, PeerAwareness>,
    awareness_last_seen: HashMap<u64, Instant>,
    awareness_dirty: bool,
}

impl YrsCrdt {
    #[must_use]
    pub fn new() -> Self {
        let doc = Doc::new();
        let client_id = doc.client_id();
        let initial = doc.transact().snapshot();
        Self {
            doc,
            client_id,
            flushed: vec![initial.clone()],
            inflight: vec![initial],
            awareness_states: HashMap::new(),
            awareness_last_seen: HashMap::new(),
            awareness_dirty: false,
        }
    }

    #[must_use]
    pub fn with_client_id(client_id: u64) -> Self {
        let doc = Doc::with_client_id(client_id);
        let initial = doc.transact().snapshot();
        Self {
            doc,
            client_id,
            flushed: vec![initial.clone()],
            inflight: vec![initial],
            awareness_states: HashMap::new(),
            awareness_last_seen: HashMap::new(),
            awareness_dirty: false,
        }
    }

    #[must_use]
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// Encode the full current state as raw Yrs V2 bytes (no framing).
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError`] if encoding fails.
    pub fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        let txn = self.doc.transact();
        Ok(txn.encode_state_as_update_v2(&StateVector::default()))
    }

    /// Apply a raw Yrs V2 snapshot (no framing).
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError`] if the bytes cannot be decoded or applied.
    pub fn merge(&mut self, snapshot: &[u8]) -> Result<(), Report<CrdtError>> {
        let update = Update::decode_v2(snapshot).change_context(CrdtError)?;
        self.doc
            .transact_mut()
            .apply_update(update)
            .change_context(CrdtError)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns [`CrdtError`] if the snapshot bytes cannot be decoded.
    pub fn from_snapshot(snapshot: &[u8], client_id: u64) -> Result<Self, Report<CrdtError>> {
        let mut crdt = Self::with_client_id(client_id);
        crdt.merge(snapshot)?;
        Ok(crdt)
    }

    fn ensure_level(&mut self, level: usize) {
        while self.flushed.len() <= level {
            self.flushed.push(Snapshot::default());
            self.inflight.push(Snapshot::default());
        }
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

    fn flush(&mut self, level: usize) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
        self.ensure_level(level);
        let txn = self.doc.transact();
        let current = txn.snapshot();

        let has_doc_changes = current != self.flushed[level];
        if level == 0 && !has_doc_changes && !self.awareness_dirty {
            return Ok(None);
        }
        if level > 0 && !has_doc_changes {
            return Ok(None);
        }

        let doc_update = if has_doc_changes {
            Some(txn.encode_diff_v2(&self.flushed[level].state_map))
        } else {
            None
        };
        drop(txn);

        self.inflight[level] = current;
        if level == 0 {
            self.awareness_dirty = false;
        }

        let msg = CrdtMessage {
            awareness: self.local_awareness(),
            doc_update,
        };
        let encoded = postcard::to_allocvec(&msg).change_context(CrdtError)?;
        Ok(Some(encoded))
    }

    fn confirm_flush(&mut self, level: usize) {
        if level < self.flushed.len() {
            self.flushed[level] = self.inflight[level].clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use yrs::types::ToJson;
    use yrs::{Any, GetString, Map, Text, Transact};

    use super::*;

    fn insert_text(crdt: &YrsCrdt, pos: u32, content: &str) {
        let text = crdt.doc().get_or_insert_text("doc");
        let mut txn = crdt.doc().transact_mut();
        text.insert(&mut txn, pos, content);
    }

    fn get_text(crdt: &YrsCrdt) -> String {
        let text = crdt.doc().get_or_insert_text("doc");
        let txn = crdt.doc().transact();
        text.get_string(&txn)
    }

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
        insert_text(&crdt, 0, "Hello");
        let snapshot = crdt.snapshot().unwrap();

        let crdt2 = YrsCrdt::from_snapshot(&snapshot, 2).unwrap();
        assert_eq!(get_text(&crdt2), "Hello");
    }

    #[test]
    fn test_compact_multiple_updates() {
        let mut crdt = YrsCrdt::with_client_id(1);

        let insert_and_flush = |crdt: &mut YrsCrdt, pos: u32, text: &str| {
            insert_text(crdt, pos, text);
            let data = crdt.flush(0).unwrap().unwrap();
            crdt.confirm_flush(0);
            data
        };

        insert_and_flush(&mut crdt, 0, "aaa");
        insert_and_flush(&mut crdt, 3, "bbb");
        insert_and_flush(&mut crdt, 6, "ccc");

        let snapshot = crdt.snapshot().unwrap();
        let fresh = YrsCrdt::from_snapshot(&snapshot, 99).unwrap();
        assert_eq!(get_text(&fresh), "aaabbbccc");
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

        insert_text(&crdt1, 0, "ABC");
        let initial = crdt1.snapshot().unwrap();
        crdt2.merge(&initial).unwrap();

        insert_text(&crdt1, 1, "X");
        insert_text(&crdt2, 2, "Y");

        let update1 = crdt1.snapshot().unwrap();
        let update2 = crdt2.snapshot().unwrap();
        crdt1.merge(&update2).unwrap();
        crdt2.merge(&update1).unwrap();

        let text1 = get_text(&crdt1);
        let text2 = get_text(&crdt2);

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
        insert_text(&alice, 0, "Hello");

        let diff = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);
        alice.apply(&diff).unwrap();

        assert_eq!(get_text(&alice), "Hello", "echo should be idempotent");
    }

    #[test]
    fn test_two_peer_with_deterministic_ids() {
        let mut alice_crdt = YrsCrdt::with_client_id(111);
        let mut bob_crdt = YrsCrdt::with_client_id(222);

        insert_text(&alice_crdt, 0, "Hello");
        let alice_update = alice_crdt.flush(0).unwrap().unwrap();
        alice_crdt.confirm_flush(0);

        bob_crdt.apply(&alice_update).unwrap();
        assert_eq!(get_text(&bob_crdt), "Hello");

        alice_crdt.apply(&alice_update).unwrap();
        assert_eq!(get_text(&alice_crdt), "Hello", "echo should not duplicate");

        insert_text(&bob_crdt, 5, " World");
        let bob_update = bob_crdt.flush(0).unwrap().unwrap();
        bob_crdt.confirm_flush(0);

        alice_crdt.apply(&bob_update).unwrap();
        assert_eq!(get_text(&alice_crdt), "Hello World");

        let mut alice2 = YrsCrdt::with_client_id(111);
        alice2.apply(&bob_update).unwrap();
        alice2.apply(&alice_update).unwrap();
        assert_eq!(get_text(&alice2), "Hello World");
    }

    #[test]
    fn test_postcard_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        insert_text(&alice, 0, "Hello");
        let update = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);

        let msg: CrdtMessage = postcard::from_bytes(&update).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.client_id, 1);

        bob.apply(&update).unwrap();
        assert_eq!(get_text(&bob), "Hello");
    }

    #[test]
    fn test_awareness_roundtrip() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        alice.set_cursor(5, 5);
        let awareness_msg = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);

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
        let msg1 = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);
        bob.apply(&msg1).unwrap();
        assert!(bob.awareness_states().get(&1).unwrap().cursor.is_some());

        alice.clear_local_state();
        let msg2 = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);
        bob.apply(&msg2).unwrap();
        assert!(bob.awareness_states().get(&1).unwrap().cursor.is_none());
    }

    #[test]
    fn test_flush_returns_combined_then_none() {
        let mut crdt = YrsCrdt::with_client_id(1);

        insert_text(&crdt, 0, "hi");
        crdt.set_cursor(2, 2);

        let first = crdt.flush(0).unwrap().unwrap();
        crdt.confirm_flush(0);
        let msg: CrdtMessage = postcard::from_bytes(&first).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.cursor, Some(2));

        assert!(crdt.flush(0).unwrap().is_none());
    }

    #[test]
    fn test_merge_takes_raw_bytes() {
        let crdt1 = YrsCrdt::with_client_id(1);
        insert_text(&crdt1, 0, "raw");
        let raw = crdt1.snapshot().unwrap();

        let mut crdt2 = YrsCrdt::with_client_id(2);
        crdt2.merge(&raw).unwrap();
        assert_eq!(get_text(&crdt2), "raw");
    }

    #[test]
    fn test_apply_rejects_garbage() {
        let mut crdt = YrsCrdt::with_client_id(1);
        assert!(crdt.apply(&[0x42, 0x00]).is_err());
    }

    #[test]
    fn test_flush_noop_when_no_changes() {
        let mut crdt = YrsCrdt::with_client_id(1);
        assert!(crdt.flush(0).unwrap().is_none());
        assert!(crdt.flush(1).unwrap().is_none());
    }

    #[test]
    fn test_retry_no_new_edits() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        insert_text(&alice, 0, "hello");

        let _lost = alice.flush(0).unwrap().unwrap();
        // Don't confirm — simulating send failure

        let retry = alice.flush(0).unwrap().unwrap();
        bob.apply(&retry).unwrap();
        assert_eq!(get_text(&bob), "hello");
    }

    #[test]
    fn test_retry_with_new_edits() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        insert_text(&alice, 0, "hello");
        let _lost = alice.flush(0).unwrap().unwrap();

        insert_text(&alice, 5, " world");
        let retry = alice.flush(0).unwrap().unwrap();

        bob.apply(&retry).unwrap();
        assert_eq!(get_text(&bob), "hello world");
    }

    #[test]
    fn test_batching() {
        let mut alice = YrsCrdt::with_client_id(1);
        let mut bob = YrsCrdt::with_client_id(2);

        insert_text(&alice, 0, "hello");
        let first = alice.flush(0).unwrap().unwrap();
        bob.apply(&first).unwrap();
        alice.confirm_flush(0);

        insert_text(&alice, 5, " world");
        let second = alice.flush(0).unwrap().unwrap();
        bob.apply(&second).unwrap();
        alice.confirm_flush(0);

        assert_eq!(get_text(&bob), "hello world");
    }

    #[test]
    fn test_incremental_compaction() {
        let mut alice = YrsCrdt::with_client_id(1);

        insert_text(&alice, 0, &"a".repeat(1000));
        let l1_first = alice.flush(1).unwrap().unwrap();
        alice.confirm_flush(1);

        insert_text(&alice, 1000, "b");
        let l1_second = alice.flush(1).unwrap().unwrap();
        alice.confirm_flush(1);

        assert!(
            l1_second.len() < l1_first.len(),
            "second compaction should be smaller (incremental, not full snapshot)"
        );

        // Both together reconstruct full state
        let mut full = YrsCrdt::with_client_id(2);
        full.apply(&l1_first).unwrap();
        full.apply(&l1_second).unwrap();
        assert_eq!(get_text(&full), get_text(&alice));

        // Order doesn't matter (yrs resolves pending items)
        let mut reversed = YrsCrdt::with_client_id(3);
        reversed.apply(&l1_second).unwrap();
        reversed.apply(&l1_first).unwrap();
        assert_eq!(get_text(&reversed), get_text(&alice));
    }

    #[test]
    fn test_l0_and_l1_independent() {
        let mut alice = YrsCrdt::with_client_id(1);

        insert_text(&alice, 0, "hello");
        let _l0 = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);

        insert_text(&alice, 5, " world");
        let _l0 = alice.flush(0).unwrap().unwrap();
        alice.confirm_flush(0);

        // L1 hasn't been flushed yet, so it covers everything
        let l1 = alice.flush(1).unwrap().unwrap();
        alice.confirm_flush(1);

        let mut bob = YrsCrdt::with_client_id(2);
        bob.apply(&l1).unwrap();
        assert_eq!(get_text(&bob), "hello world");
    }

    #[test]
    fn test_compaction_at_level_includes_awareness() {
        let mut alice = YrsCrdt::with_client_id(1);
        insert_text(&alice, 0, "Hello");
        alice.set_cursor(5, 5);

        let update = alice.flush(1).unwrap().unwrap();
        alice.confirm_flush(1);

        let msg: CrdtMessage = postcard::from_bytes(&update).unwrap();
        assert!(msg.doc_update.is_some());
        assert_eq!(msg.awareness.client_id, 1);
        assert_eq!(msg.awareness.cursor, Some(5));

        let mut bob = YrsCrdt::with_client_id(2);
        bob.apply(&update).unwrap();
        assert_eq!(get_text(&bob), "Hello");
    }

    /// yrs 0.25: V2 delete markers silently truncate the *referenced item's*
    /// client ID to 32 bits.
    ///
    /// Workaround: mask `as_client_id()` to 32 bits (`& 0xFFFF_FFFF`).
    #[test]
    fn test_v2_delete_truncates_large_client_id() {
        let large_cid: u64 = 0x0000_0001_CAFE_BABE; // 33 bits
        let small_cid: u64 = 42;

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

        let sv = doc.transact().state_vector();
        t.remove_range(&mut doc.transact_mut(), 0, 5);
        let del = doc.transact().encode_diff_v2(&sv);
        let del_update = Update::decode_v2(&del).unwrap();
        doc2.transact_mut().apply_update(del_update).unwrap();
        assert_eq!(
            doc2.get_or_insert_text("doc").get_string(&doc2.transact()),
            "Hello",
            "yrs bug: delete of >32-bit CID items is silently ignored"
        );

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
