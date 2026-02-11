//! Erasure coding for compacted message shards.
//!
//! Fixed parity map (count → `parity_shards`): 0..=1 → 0, 2..=5 → 1, 6..=9 → 2, 10+ → 3.
//! When parity is 0, no erasure coding; when parity > 0, data is encoded into count shards
//! (`data_shards` = count − parity, `parity_shards` = parity).

use bytes::{Bytes, BytesMut};
use reed_solomon_erasure::ReedSolomon;
use reed_solomon_erasure::galois_8::Field;

/// Parity shards for a given acceptor count (number we send this layer to).
/// 0..=1 → 0 (full copy), 2..=5 → 1, 6..=9 → 2, 10+ → 3.
#[must_use]
pub(crate) fn parity_for_count(count: usize) -> usize {
    match count {
        0..=1 => 0,
        2..=5 => 1,
        6..=9 => 2,
        _ => 3,
    }
}

/// Encode `data` into `count` shards with the given parity. Returns `None` if count is 0 or
/// parity >= count (invalid). Otherwise returns `Some(Vec<Bytes>)` with `count` shards;
/// concatenating the first `data_shards` shards and stripping the length prefix recovers `data`.
pub(crate) fn encode(data: &[u8], count: usize, parity: usize) -> Option<Vec<BytesMut>> {
    if count == 0 || parity >= count {
        return None;
    }
    let data_shards = count - parity;
    let rs = ReedSolomon::<Field>::new(data_shards, parity).ok()?;
    let len_prefix = u32::try_from(data.len())
        .expect("snapshot length fits u32")
        .to_be_bytes();
    let mut payload = BytesMut::with_capacity(4 + data.len());
    payload.extend_from_slice(&len_prefix);
    payload.extend_from_slice(data);
    let shard_len = payload.len().div_ceil(data_shards);
    let padded_len = shard_len * data_shards;
    payload.resize(padded_len, 0);

    let mut shards = Vec::with_capacity(count);
    while !payload.is_empty() {
        shards.push(payload.split_to(shard_len));
    }
    for _ in 0..parity {
        shards.push(BytesMut::zeroed(shard_len));
    }
    rs.encode(&mut shards).ok()?;
    Some(shards)
}

/// Decode when we have at least `data_shards` shards. `shards` is a map from shard index to shard
/// bytes. Returns the reconstructed original data (after stripping length prefix), or `None` if
/// reconstruction fails or we don't have enough shards.
pub(crate) fn decode(
    mut shards: Vec<Option<BytesMut>>,
    data_shards: usize,
    parity_shards: usize,
) -> Option<Bytes> {
    if data_shards == 0 || parity_shards == 0 {
        return None;
    }
    // let total = data_shards + parity_shards;
    if shards.iter().filter(|x| x.is_some()).count() < data_shards {
        return None;
    }
    let shard_len = shards.iter().find_map(|x| x.as_ref())?.len();
    if !shards
        .iter()
        .filter_map(|x| x.as_ref())
        .all(|s| s.len() == shard_len)
    {
        return None;
    }
    let rs = ReedSolomon::<Field>::new(data_shards, parity_shards).ok()?;
    rs.reconstruct(&mut shards).ok()?;
    let data: Vec<u8> = shards
        .into_iter()
        .take(data_shards)
        .flatten()
        .flatten()
        .collect();
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if 4 + len > data.len() {
        return None;
    }
    Some(Bytes::copy_from_slice(&data[4..4 + len]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parity_map() {
        assert_eq!(parity_for_count(0), 0);
        assert_eq!(parity_for_count(1), 0);
        assert_eq!(parity_for_count(2), 1);
        assert_eq!(parity_for_count(5), 1);
        assert_eq!(parity_for_count(6), 2);
        assert_eq!(parity_for_count(9), 2);
        assert_eq!(parity_for_count(10), 3);
        assert_eq!(parity_for_count(100), 3);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let data = b"hello world";
        let count = 4;
        let parity = 1;
        let data_shards = count - parity;
        let shards = encode(data, count, parity).unwrap();
        assert_eq!(shards.len(), count);
        let map = vec![
            Some(shards[0].clone()),
            Some(shards[1].clone()),
            Some(shards[2].clone()),
            None,
        ];
        let decoded = decode(map, data_shards, parity).unwrap();
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn decode_from_parity_only() {
        let data = b"xyz";
        let count = 3;
        let parity = 1;
        let shards = encode(data, count, parity).unwrap();
        let map = vec![None, Some(shards[1].clone()), Some(shards[2].clone())];
        let decoded = decode(map, count - parity, parity).unwrap();
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn encode_skip_zero_count() {
        assert!(encode(b"x", 0, 0).is_none());
    }

    #[test]
    fn encode_skip_parity_ge_count() {
        assert!(encode(b"x", 2, 2).is_none());
    }

    #[test]
    fn parity_map_matches_delivery_count_ranges() {
        use crate::rendezvous;
        let counts_len = 4;
        for num_acceptors in [1, 2, 5, 6, 9, 10, 15] {
            for level in 0..=counts_len {
                let count = rendezvous::delivery_count(level, counts_len, num_acceptors);
                let parity = parity_for_count(count);
                if count <= 1 {
                    assert_eq!(parity, 0, "count={count} should have 0 parity");
                } else if count <= 5 {
                    assert_eq!(parity, 1, "count={count} should have 1 parity");
                } else if count <= 9 {
                    assert_eq!(parity, 2, "count={count} should have 2 parity");
                } else {
                    assert_eq!(parity, 3, "count={count} should have 3 parity");
                }
            }
        }
    }
}
