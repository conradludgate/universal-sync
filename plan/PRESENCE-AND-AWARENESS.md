# Presence Detection and Cursor Awareness

## Status: Implemented

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

## Previously Known Bug (Fixed)

Editors were getting out of sync due to race conditions in the Monaco
integration and `flush_update()` state vector handling. This has been
resolved.
