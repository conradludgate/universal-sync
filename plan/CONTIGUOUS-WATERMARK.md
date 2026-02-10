# Contiguous Watermark

**Status**: Implemented

## Problem

The `StateVector` was updated as `max(seen_seq)`, not contiguous coverage. If a compactor missed messages due to acceptor disconnection:

1. Compactor receives seq 1, 2, 5 (missing 3, 4)
2. `state_vector[sender] = 5` (claims coverage 1-5)
3. Compaction snapshot only contains data from 1, 2, 5
4. `delete_before_watermark` deletes messages 1-5 on all spools
5. Messages 3, 4 are deleted but weren't in the snapshot - **data loss**

## Current Design

Replaced `state_vector: StateVector` in `GroupActor` with `received_seqs: ReceivedSeqs`.

### `SenderSeqs` struct

Uses `rangemap::RangeInclusiveSet<u64>` for efficient interval tracking:

```rust
struct SenderSeqs {
    confirmed: u64,
    ranges: RangeInclusiveSet<u64>,
}
```

- `record(seq)`: Insert seq; adjacent ranges auto-merge
- `watermark()`: Return contiguous prefix (first range end if it starts at confirmed+1)
- `confirm_compaction(new_confirmed)`: Prune ranges below new watermark
- `contains(seq)`: Check if seq is covered

### `ReceivedSeqs` struct

```rust
struct ReceivedSeqs {
    senders: HashMap<MemberFingerprint, SenderSeqs>,
}
```

- `record(sender, seq)`: Record received message
- `watermark()`: Return `StateVector` with contiguous prefix per sender
- `confirm_compaction(watermark)`: Prune confirmed ranges to free memory
- `contains(sender, seq)`: Check for deduplication

### Behaviour

If compactor received seq 1, 2, 5 (missing 3, 4):

- `watermark()` returns `{sender: 2}` (contiguous up to 2)
- `delete_before_watermark` only deletes seq 1, 2
- Messages 3, 4, 5 are preserved until a future compaction that has them

### Complexity

- O(1) watermark lookup
- O(log ranges) insert with automatic merging
- O(num_ranges) memory - typically 1-3 ranges for mostly in-order arrivals

## Files Modified

- `filament-weave/Cargo.toml`: Added `rangemap` dependency
- `filament-weave/src/group/group_actor.rs`: Added `SenderSeqs`, `ReceivedSeqs`, replaced `state_vector` field
