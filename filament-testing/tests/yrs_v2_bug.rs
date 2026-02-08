//! yrs 0.25.0: V2 encode/decode roundtrip fails when client ID >= 2^63.
//!
//! The V2 signed varint encoder (`write_var_i64`) interprets the u64 client ID
//! as i64. Values with the MSB set (bit 63) become negative and break:
//!
//! - Exactly `i64::MIN` (0x8000_0000_0000_0000) panics with
//!   "attempt to negate with overflow" during encoding.
//! - All other values >= 2^63 encode but produce output that
//!   `Update::decode_v2` cannot parse (EndOfBuffer).
//!
//! Effective client ID range: 0 ..= i64::MAX (0 ..= 2^63 - 1).
//! `Doc::with_client_id` accepts u64, so this limit is not obvious.
//!
//! V1 encoding has a separate issue: it silently truncates client IDs to u32.
//!
//! Run: cargo test -p filament-testing --test yrs_v2_bug

use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

fn v2_roundtrip(client_id: u64) {
    let doc = Doc::with_client_id(client_id);
    let text = doc.get_or_insert_text("t");
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "hello");
    }

    let encoded = doc.transact().encode_diff_v2(&StateVector::default());
    let update = Update::decode_v2(&encoded)
        .unwrap_or_else(|e| panic!("decode_v2 failed for client_id {client_id:#x}: {e}"));

    let doc2 = Doc::new();
    let text2 = doc2.get_or_insert_text("t");
    doc2.transact_mut().apply_update(update).unwrap();
    assert_eq!(text2.get_string(&doc2.transact()), "hello");
}

#[test]
fn v2_works_below_i64_max() {
    v2_roundtrip(0);
    v2_roundtrip(1);
    v2_roundtrip(u64::from(u32::MAX));
    v2_roundtrip(i64::MAX as u64); // 2^63 - 1 — largest working value
}

/// Panics during encoding: `write_var_i64` does `-value` on `i64::MIN`.
#[test]
#[should_panic]
fn v2_encode_panics_at_i64_min() {
    v2_roundtrip(i64::MIN as u64); // 2^63
}

/// Encodes successfully but decode_v2 returns EndOfBuffer.
#[test]
#[should_panic(
    expected = "decode_v2 failed for client_id 0x8000000000000001: while trying to read more data (expected: 1 bytes), an unexpected end of buffer was reached"
)]
fn v2_decode_fails_above_i64_max() {
    v2_roundtrip(i64::MIN as u64 + 1); // 2^63 + 1
}
