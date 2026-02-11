# Message Store Key Format

**Status**: Implemented

## Problem

The message store in `filament-spool/src/state_store.rs` must support:

1. **Level tracking**: Store `level: u8` (0 for L0 updates, 1+ for compactions)
2. **Efficient deletion**: "delete all messages where level < N and seq <= watermark[sender]"
3. **Streaming**: Real-time notification of new messages
4. **Backfill**: "get all messages where seq > state_vector[sender]" for all senders

**Current format** (no level support):

- Key: `sender_fingerprint (8 bytes) || seq (8 bytes)` = 16 bytes
- Value: `EncryptedAppMessage`
- `get_messages_after` and `delete_before_watermark` both full-scan the keyspace

**Constraints**:

- fjall (LSM-tree, sorted keys, lexicographic order)
- Efficient range scans and prefix scans
- Multiple senders with different fingerprints

**Operations**:

| Operation | Frequency | Description |
|-----------|-----------|-------------|
| `store_app_message(sender, seq, level, msg)` | High | Insert |
| `get_messages_after(state_vector)` | High | Backfill: messages where seq > sv[sender] |
| `delete_before_watermark(watermark, level)` | Low | Delete where msg.level < level AND seq <= watermark[sender] |
| `subscribe_messages()` | High | Notify on new (in-memory, no storage change) |

---

## Option Evaluation

### Option A: `sender || level || seq`

**Key**: `sender_fingerprint (8) || level (1) || seq (8)` = 17 bytes

| Criterion | Assessment |
|-----------|-------------|
| **Insert** | O(1) single write |
| **Backfill** | For each sender, must scan `sender || 0 || (seq > sv)`, `sender || 1 || (seq > sv)`, … → O(senders × levels) range scans |
| **Deletion** | For each level &lt; N, for each sender: prefix `sender || level`, delete where seq ≤ watermark[sender] → O(levels × senders) scans, but can batch per level |
| **Storage** | +1 byte/key vs current |
| **Complexity** | Medium |

### Option B: `sender || seq` with level in value

**Key**: `sender_fingerprint (8) || seq (8)` = 16 bytes  
**Value**: `level (1) || EncryptedAppMessage`

| Criterion | Assessment |
|-----------|-------------|
| **Insert** | O(1) single write |
| **Backfill** | For each sender: `range(sender || (seq > sv[sender]))` → O(senders) range scans ✓ |
| **Deletion** | Must iterate all keys, read value to check level, then delete → O(all messages) full scan ✗ |
| **Storage** | Same key size; +1 byte in value |
| **Complexity** | Low |

### Option C: `level || sender || seq`

**Key**: `level (1) || sender_fingerprint (8) || seq (8)` = 17 bytes

| Criterion | Assessment |
|-----------|-------------|
| **Insert** | O(1) single write |
| **Backfill** | For each sender, must scan `0 || sender || (seq > sv)`, `1 || sender || …`, … → O(senders × levels) scans ✗ |
| **Deletion** | For each level &lt; N: prefix `level`, iterate, delete where seq ≤ watermark[sender] → O(levels) scans ✓ |
| **Storage** | +1 byte/key |
| **Complexity** | Medium |

### Option D: Two keyspaces (primary + index)

**Primary**: `sender || seq` → `level (1) || EncryptedAppMessage`  
**Index**: `level || sender || seq` → `()` (empty, for iteration)

| Criterion | Assessment |
|-----------|-------------|
| **Insert** | O(2) writes (primary + index) |
| **Backfill** | Primary only: O(senders) range scans ✓ |
| **Deletion** | Index: iterate `level || *` for levels 0..N-1, delete from both keyspaces where seq ≤ watermark[sender] → O(levels) scans ✓ |
| **Storage** | ~2× keys (index keys are longer; values empty) |
| **Complexity** | High (two keyspaces, consistency) |

### Option E: `sender || seq`, level in value, accept full-scan deletion

Same as B. Deletion does a full scan. Given deletion is infrequent (post-compaction), this may be acceptable.

| Criterion | Assessment |
|-----------|-------------|
| **Insert** | O(1) ✓ |
| **Backfill** | O(senders) ✓ |
| **Deletion** | O(all messages) full scan — acceptable if infrequent |
| **Storage** | Minimal |
| **Complexity** | Low |

---

## Recommendation: **Option B (Level in Value)**

**Rationale**:

1. **Deletion is infrequent** — runs after compaction, typically deleting most messages at the target level. Full scan is acceptable.
2. **Deletion touches most messages anyway** — compaction typically removes a large percentage of messages at lower levels, so the scan overhead is amortized.
3. **No write amplification** — Option D requires 2 writes per insert; Option B requires only 1.
4. **Simpler implementation** — single keyspace, no index consistency concerns.
5. **Backfill is efficient** — single range scan with filtering by seq > sv[sender].

**Trade-off**: Deletion reads values to check level. For large message values, this has I/O cost. However, since deletion is rare and fjall's LSM-tree efficiently caches hot data, this is acceptable.

---

## Implementation Design

### Key format

Messages remain in `{group_prefix}.messages` (one keyspace per group).

**Key**:

```
sender_fingerprint (8) || seq (8)
```

Total: 16 bytes. Same as current format.

### Value format

Values are either a full message or a single erasure-coded shard (for compacted messages when parity > 0):

```rust
#[derive(Serialize, Deserialize)]
enum StoredMessage {
    Full(StoredAppMessage),
    Shard(StoredShard),
}

struct StoredAppMessage {
    level: u8,
    msg: EncryptedAppMessage,
}

struct StoredShard {
    level: u8,
    shard_index: u8,
    data_shards: u8,
    total_shards: u8,
    data: Vec<u8>,
}
```

Serialized with postcard; storage envelope is unchanged. Level is in the value; deletion reads the value to check level. See [COMPACTED-ERASURE-CODING.md](COMPACTED-ERASURE-CODING.md).

### Operation implementations

#### 1. `store_app_message(sender, seq, level, msg)`

```rust
fn build_key(sender: MemberFingerprint, seq: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&sender.0);
    key[8..16].copy_from_slice(&seq.to_be_bytes());
    key
}

let key = build_key(sender, seq);
let stored = StoredAppMessage { level, msg };
let value = postcard::to_allocvec(&stored)?;
ks.messages.insert(key, &value)?;
```

Single insert. Key format unchanged from current implementation.

#### 2. `get_messages_after(state_vector)`

Full scan of keyspace, filter by seq > sv[sender]:

```rust
for guard in ks.messages.iter() {
    let (key, value) = guard.into_inner()?;
    let (sender, seq) = parse_key(&key);
    
    if state_vector.get(&sender).is_some_and(|&hw| seq <= hw) {
        continue;  // Already covered by state vector
    }
    
    let stored: StoredAppMessage = postcard::from_bytes(&value)?;
    messages.push((MessageId { sender, seq }, stored.msg));
}
```

Same as current implementation. fjall range scans are efficient.

#### 3. `delete_before_watermark(watermark, compaction_level)`

Full scan, check level in value, delete matching entries:

```rust
for guard in ks.messages.iter() {
    let (key, value) = guard.into_inner()?;
    let (sender, seq) = parse_key(&key);
    
    // Check watermark
    if !watermark.get(&sender).is_some_and(|&hw| seq <= hw) {
        continue;
    }
    
    // Check level
    let stored: StoredAppMessage = postcard::from_bytes(&value)?;
    if stored.level >= compaction_level {
        continue;  // Don't delete messages at or above compaction level
    }
    
    ks.messages.remove(&*key)?;
    deleted += 1;
}
```

Full scan with value reads. Acceptable since deletion is infrequent and typically touches most messages at lower levels.

#### 4. `subscribe_messages()`

Unchanged. In-memory `watch::Sender<StateVector>`. No storage changes.

---

## Migration

Current format has no level. All existing messages are L0 (level 0). Migration:

1. New value format wraps message in `StoredAppMessage { level, msg }`.
2. Old messages (without level prefix) should be treated as level 0.
3. On deserialization, detect old format and default level to 0.

Since we're not maintaining backward compatibility, a clean break is acceptable: clear messages keyspace on upgrade if needed.

---

## Summary

| Aspect | Choice |
|--------|--------|
| **Key format** | `sender (8) \|\| seq (8)` = 16 bytes (unchanged) |
| **Value format** | `postcard(StoredMessage::Full or Shard)` |
| **Insert** | Single write |
| **Backfill** | Full scan, filter by seq > sv[sender] |
| **Deletion** | Full scan, read value to check level < compaction_level AND seq <= watermark[sender] |
| **Storage** | +1 byte per value for level |
