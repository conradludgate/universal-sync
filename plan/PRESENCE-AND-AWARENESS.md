# Presence Detection and Cursor Awareness

## Status: BUGGY — editors getting out of sync. Not yet reverted.

The original plan called for a prefix-byte framing approach (`0x00` for
doc, `0x01` for awareness) with `CrdtFactory::wrap_snapshot_for_wire()`
and a looping `flush_update()`. The actual implementation diverged:

- Messages use `postcard`-serialized `CrdtMessage` containing both
  `awareness: PeerAwareness` and `doc_update: Option<Vec<u8>>`. No
  prefix bytes.
- `apply()` deserializes `CrdtMessage` directly — no prefix routing.
- `wrap_snapshot_for_wire()` does not exist.
- `Group::send_update()` calls `crdt.flush(0)` once, does not loop.

The awareness code was never reverted despite the "revert" instruction.
It remains active in its current (non-prefix-byte) form.

---

## Current Implementation

### Message format

`CrdtMessage` (`filament-editor/src/yrs_crdt.rs`):

```rust
struct CrdtMessage {
    awareness: PeerAwareness,
    doc_update: Option<Vec<u8>>,
}
```

Serialized via `postcard`. Awareness and doc updates are combined into a
single message rather than sent separately.

### Awareness state

`YrsCrdt` manages awareness manually via:
- `PeerAwareness` struct with cursor position and status
- `HashMap<ClientID, AwarenessUpdateEntry>` for remote peer state
- `set_cursor()`, `clear_local_state()`, `expire_stale_peers()`,
  `awareness_states()` accessors

### Editor integration

- `DocumentActor` tracks cursor position, includes it in the awareness
  state JSON.
- Monaco editor renders remote cursors via `deltaDecorations` API.
- `ignoreNextChange` guard prevents echoing remote text changes.

### Client ID

Derived from public key: `xxh64(identity_bytes)` for cross-platform
stability.

---

## Known Bug: Editors getting out of sync

Symptom: Two editors in the same document drift apart — edits from one
peer stop appearing in the other, or text diverges.

### Suspected root causes to investigate

1. **`ignoreNextChange` guard in Monaco integration**: `setEditorText()`
   uses `pushEditOperations()` which fires `onDidChangeModelContent`. The
   flag is set/cleared synchronously but the callback might fire
   asynchronously, creating a feedback loop.

2. **`flush_update()` state vector drift**: If `apply()` is called
   between the state vector read and the update of `last_flushed_sv`, the
   next diff could skip or duplicate operations.

3. **Pre-existing race in delta application**: The frontend computes
   deltas by diffing `lastSentText` against `getEditorText()`. A remote
   update arriving between diff computation and `apply_delta` invoke
   could apply the wrong edit.

### Debugging approach

1. Check if the bug reproduces WITHOUT awareness (strip awareness from
   `CrdtMessage`, test sync). If sync works without awareness, narrow
   down which change introduced the regression.
2. Check if the bug reproduces without Monaco (use textarea). If sync
   works with textarea, the `ignoreNextChange` guard or Monaco event
   timing is the issue.
3. Write a deterministic test: two peers, interleave doc edits with
   awareness updates, verify text stays in sync after each round.
