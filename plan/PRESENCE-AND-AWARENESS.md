# Presence Detection and Cursor Awareness

## Status: BUGGY — editors getting out of sync. Revert code, fix later.

---

## Original Plan Design

Awareness is entirely encapsulated inside the CRDT implementation. No other
layer (filament-core, filament-spool, GroupActor) knows about it.

### Message framing

`flush_update()` and `apply()` use a prefix byte to distinguish message types.
`snapshot()` / `merge()` / `from_snapshot()` are unprefixed (doc-only).

- `0x00` + yrs update bytes = document update
- `0x01` + awareness update bytes = awareness update

### Data flow

1. `YrsCrdt::flush_update()` returns doc diffs first (prefixed `0x00`), then
   awareness heartbeats (prefixed `0x01`) when the heartbeat timer (15s) elapses,
   then `None`.
2. `Group::send_update()` loops calling `flush_update()` until `None`, sending
   each result as a normal MLS-encrypted message through acceptors.
3. On the receiving side, `YrsCrdt::apply()` reads the prefix byte and routes:
   - `0x00` → `Update::decode_v1` → `doc.transact_mut().apply_update()`
   - `0x01` → `AwarenessUpdate::decode_v1` → merge into awareness state map
4. `YrsCrdt::snapshot()` only serializes the `Doc` (no awareness), so awareness
   is naturally excluded from compacted snapshots.
5. Compacted snapshots sent over the wire are wrapped with `0x00` prefix via
   `CrdtFactory::wrap_snapshot_for_wire()` so receiving peers' `apply()` can
   decode them.

### Compaction behavior

Awareness messages participate in compaction naturally:
- Stored at L0 by acceptors like any message
- Counted toward compaction thresholds (triggering compaction cleans them up)
- Applied during `compact()` — awareness entries go to a throwaway state
- Excluded from the compacted snapshot — `snapshot()` only serializes `Doc`
- Deleted from acceptors when the watermark advances

### Heartbeat tuning

`YrsCrdt` has an internal 15s heartbeat. `DocumentActor` has a matching
`tokio::time::interval(15s)` tick in its `select!` loop that calls
`group.send_update()`.

---

## What was implemented (prior session)

### filament-testing/src/yrs_crdt.rs

- `YrsCrdt` was restructured. Originally planned to wrap `yrs::sync::awareness::Awareness`,
  but `Awareness` is `!Send + !Sync`, violating the `Crdt` trait bounds.
- Instead: manual awareness state management using `HashMap<ClientID, AwarenessUpdateEntry>`.
  Reuses `yrs::sync::awareness::{AwarenessUpdate, AwarenessUpdateEntry}` for encoding/decoding.
- Added: `DOC_PREFIX`/`AWARENESS_PREFIX` constants, prefix-based dispatch in `apply()`,
  heartbeat logic in `flush_update()`, `set_local_state()`, `clean_local_state()`,
  `awareness_states()` accessor, `client_id()` accessor.
- `merge()` decodes without prefix (raw yrs update).
- `snapshot()` returns doc-only state (no prefix, no awareness).

### filament-core/src/crdt.rs

- Added `CrdtFactory::wrap_snapshot_for_wire()` with default identity impl.
  `YrsCrdtFactory` overrides it to prepend `DOC_PREFIX`.

### filament-weave/src/group/mod.rs

- `Group::send_update()` changed to loop calling `flush_update()` until `None`.

### filament-weave/src/group/group_actor.rs

- `perform_compaction` calls `wrap_snapshot_for_wire()` before sending compacted
  snapshots to acceptors.

### filament-editor/src/types.rs

- Added `AwarenessPeer`, `AwarenessPayload`, `EventEmitter::emit_awareness_changed()`.

### filament-editor/src/document.rs

- `DocumentActor` sets local awareness state on startup, has heartbeat tick in
  `select!` loop, emits awareness events after `wait_for_update()`.
- Added `last_emitted_text` field to deduplicate text update events.
- On shutdown: cleans local awareness state, sends final `send_update()`.

### filament-editor UI

- Added presence bar in toolbar (later moved to share modal).

### Tests

- 19 unit tests in filament-testing (prefix roundtrip, awareness timing, etc.)
- Integration test `awareness_heartbeat_received_by_peer` in filament-editor.

---

## What was changed (this session)

### 1. Cursor position tracking

**Backend:**
- Added `DocRequest::UpdateCursor { anchor: u32, head: u32 }` (fire-and-forget).
- `DocumentActor` tracks `cursor_anchor`/`cursor_head`, includes them in the
  awareness state JSON: `{"status":"online","cursor":5,"selection_end":12}`.
- New Tauri command `update_cursor` registered.
- `AwarenessPeer` gained `identity`, `cursor`, `selection_end` fields.
- `emit_awareness` parses cursor/selection_end/identity from the JSON state.

**Frontend:**
- Tracks `selectionStart`/`selectionEnd` on cursor events, sends debounced
  (200ms) `update_cursor` invokes.

### 2. Online status moved to member list

- `DocumentActor` fetches its own identity from group context at startup,
  includes it in the awareness JSON.
- `renderPeerList()` builds a set of online identities from awareness peers,
  shows green/grey dots next to members.
- Removed the separate "Online Now" presence section.

### 3. Client ID derived from public key

- `YrsCrdtFactory::with_fixed_client_id(xxh64(identity_bytes))` in `main.rs`.
- Uses `xxhash-rust` crate (xxh64) instead of `DefaultHasher` for cross-platform
  stability.

### 4. Monaco editor

Replaced the plain `<textarea>` with Monaco editor (loaded from CDN):
- `<div id="editor" class="monaco-container">` instead of textarea.
- All `el.editor.value` / `selectionStart` replaced with Monaco API calls
  (`getValue()`, `getSelection()`, `model.getOffsetAt()`, `pushEditOperations()`).
- Remote cursors rendered via `deltaDecorations` API (colored border-left for
  caret, background highlight for selection, `::after` pseudo-element for name
  label) instead of the mirror div hack.
- `ignoreNextChange` guard prevents echoing remote text changes back as deltas.
- Removed: cursor-overlay div, cursor-mirror div, `getOffsetCoords()`,
  `renderCursorOverlay()`, all related CSS.

---

## Known Bug: Editors getting out of sync

Symptom: After the awareness feature was added, two editors in the same document
drift apart — edits from one peer stop appearing in the other, or text diverges.

### Suspected root causes to investigate

1. **`ignoreNextChange` guard in Monaco integration**: `setEditorText()` uses
   `pushEditOperations()` which fires `onDidChangeModelContent`. The
   `ignoreNextChange` flag is set/cleared synchronously, but `onDidChangeModelContent`
   might fire asynchronously. If the flag clears before the callback fires, the
   remote text update gets re-sent as a local delta, creating a feedback loop or
   duplicate operations.

2. **`flush_update()` state vector drift**: `flush_update()` snapshots
   `last_flushed_sv` after encoding a diff. If `apply()` is called between the
   state vector read and the update of `last_flushed_sv`, the next diff could
   skip or duplicate operations.

3. **Awareness messages consumed as doc updates during compaction**: Although
   `compact()` calls `apply()` which correctly routes awareness messages to the
   awareness map (not the doc), the `update_buffer` in `GroupActor` accumulates
   ALL messages. If a bug causes awareness bytes to be misinterpreted, it could
   corrupt the doc state.

4. **`wrap_snapshot_for_wire` double-prefixing**: Compacted snapshots are
   wrapped with `DOC_PREFIX` via `wrap_snapshot_for_wire()`. If a snapshot
   already has a prefix (e.g., from being stored after a prior wrap), applying
   it would fail or corrupt state.

5. **Heartbeat tick racing with doc updates**: The `select!` loop has both
   `wait_for_update()` and `heartbeat.tick()`. If `send_update()` from the
   heartbeat branch flushes a pending doc diff AND an awareness heartbeat, but
   the doc diff was partially applied or the state vector wasn't updated
   correctly, subsequent diffs could be wrong.

6. **Pre-existing race in delta application**: The frontend computes deltas by
   diffing `lastSentText` against `getEditorText()`. If a remote update arrives
   between the diff computation and the `apply_delta` invoke, the delta could be
   based on stale text, applying the wrong edit.

### Debugging approach

1. Add tracing to `apply()` and `flush_update()` logging prefix byte and payload
   size on each call.
2. Add tracing to `GroupActor` message handling showing message type (doc vs
   awareness) as it enters `update_buffer`.
3. Write a deterministic test: two `DocumentActor`s, interleave doc edits with
   awareness heartbeats, verify text stays in sync after each round.
4. Check if the bug reproduces WITHOUT awareness (revert awareness changes, test
   sync). If sync works without awareness, narrow down which awareness change
   introduced the regression.
5. Check if the bug reproduces without Monaco (use textarea with awareness). If
   sync works with textarea, the `ignoreNextChange` guard or Monaco event timing
   is the issue.
