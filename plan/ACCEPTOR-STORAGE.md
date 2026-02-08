# Acceptor Storage

**Status**: Implemented

## Problem

The acceptor stored too much redundant data:

1. **Accepted proposals** included `epoch` (redundant with the key) and
   `message_hash` (derivable from the message via SHA256).
2. **Promised proposals** were stored per-epoch in a separate `promised`
   keyspace, but only the last promise per group matters.
3. **Epoch rosters** (`EpochRoster`) stored full member lists at every
   epoch, growing linearly with group history. Crash recovery replayed
   all commits from epoch 0.
4. **Group info** was stored in a separate `groups` keyspace, duplicating
   data recoverable from snapshots.

Four keyspaces existed: `accepted`, `promised`, `groups`, `epoch_rosters`.

## Current Design

Three keyspaces: `accepted`, `snapshots`, `messages`.

### Keyspace: `accepted`

Keys: `group_id (32 bytes) || epoch (8 bytes, big-endian)`.

Stores both accepted proposals and the single last-promised proposal.

#### Accepted values

Serialized as `SlimAccepted` via `postcard`, wrapped in a storage version
envelope (`[0xFF, 0xFE, version] || payload`):

```rust
struct SlimAccepted {
    member_id: MemberId,
    attempt: Attempt,
    signature: Vec<u8>,
    message: GroupMessage,
}
```

On read, `epoch` is recovered from the key and `message_hash` is
recomputed as `SHA256(postcard::to_allocvec(&message))`.

The `signature` length is not explicitly encoded — `postcard` handles
`Vec<u8>` with variable-length prefix, keeping the storage layer agnostic
to cipher suite details.

#### Promised sentinel

The single last-promised proposal per group is stored at
`epoch = u64::MAX` (sentinel key). On read, the stored proposal's epoch
is compared against the requested epoch; a mismatch returns `None`.

This consolidates all fsync-critical writes into one keyspace. Both
`set_promised_sync` and `set_accepted_sync` call
`db.persist(PersistMode::SyncAll)` on the `accepted` keyspace.

Range queries (`get_accepted_from_sync`, `highest_accepted_round_sync`)
explicitly filter out the sentinel epoch.

### Keyspace: `snapshots`

Keys: `group_id (32 bytes) || epoch (8 bytes, big-endian)`.

Stores serialized `mls_rs::external_client::ExternalSnapshot` bytes.
These capture the full `ExternalGroup` state (roster, tree, context)
needed to reconstruct the group without replaying from epoch 0.

Writes do **not** fsync — snapshots are recoverable from accepted values
and can be lazily rebuilt.

#### When snapshots are stored

- **Initial**: `store_initial_snapshot()` stores a snapshot at the
  acceptor's join epoch (which may not be epoch 0 if the acceptor was
  added mid-lifecycle).
- **On each learned commit**: `store_snapshot()` is called after
  `process_incoming_message()` succeeds in `GroupAcceptor::apply()`.

#### Logarithmic pruning

After each `store_snapshot`, `prune_snapshots_sync` runs. The kept set
relative to current epoch E:

- E (current)
- E−1, E−2 (recent)
- E−4, E−8, E−16, ... (powers of 2)
- Oldest snapshot (always preserved — may be the join epoch)

Everything else is deleted. For epoch 20, the kept set is
{0, 4, 12, 16, 18, 19, 20}.

#### Group enumeration

`list_groups_sync()` scans the `snapshots` keyspace to discover known
groups, replacing the removed `groups` keyspace.

### Keyspace: `messages`

Unchanged. Stores encrypted application messages (CRDT updates).

Keys: `group_id (32) || sender_fingerprint (8) || seq (8)`.

### Storage versioning

The version envelope (`[0xFF, 0xFE, version]`) is retained for accepted
values. The storage format changed within version 1 — old on-disk data
from the previous format is not compatible (clean break, no migration).

## Crash Recovery

`AcceptorRegistry::get_group()`:

1. Load the latest `ExternalSnapshot` from `snapshots`.
2. Deserialize via `ExternalSnapshot::from_bytes()`, reconstruct
   `ExternalGroup` via `external_client.load_group()`.
3. Replay accepted values from `snapshot_epoch` onwards via
   `Learner::apply()`.

A snapshot at epoch N means the group is AT epoch N. The accepted value
at epoch N is the commit that advances FROM epoch N. So replay starts
at `snapshot_epoch` (inclusive), not `snapshot_epoch + 1`.

This avoids replaying all commits from epoch 0. With logarithmic
snapshots, at most O(log N) epochs need replaying in the worst case
(if the latest snapshot is an old logarithmic one), but typically only
the most recent few.

## Acceptor Runner Catch-Up

The paxos acceptor runner (`run_acceptor_with_epoch_waiter`) loads a
`GroupAcceptor` once per proposal stream connection. As the learner
processes commits in the background, the runner's internal
`ExternalGroup` can fall behind.

The runner tracks `handler_applied_epoch` and, before processing each
prepare or accept request, replays any learned values between
`handler_applied_epoch` and the learner's current epoch. This ensures
the in-memory roster is current for proposal validation.

### Roster lookup

`get_member_public_key_for_epoch(member_id, epoch)`:

- `epoch >= current_epoch`: uses the in-memory roster (membership changes
  only take effect after commit application, so the current roster is
  valid for upcoming rounds).
- `epoch < current_epoch`: returns `None`. Historical reconstruction from
  snapshots is not yet implemented.

**TODO**: For past-epoch validation, load the nearest `ExternalSnapshot`,
replay accepted values to the target epoch, and extract the roster.
Throttle/cache this to avoid expense under load.

### Sender roster check

`GroupAcceptor::is_fingerprint_in_roster()` iterates the `ExternalGroup`
roster, computes each member's `MemberFingerprint` (from
`signing_identity.signature_key` and `LeafNodeExt.binding_id`), and
checks for a match. Called from `handle_message_request` in `server.rs`
using the `GroupAcceptor` loaded at message stream connection time.

The `ExternalClient` must register `SYNC_EXTENSION_TYPE` (and
`SYNC_PROPOSAL_TYPE`) so that leaf node custom extensions are preserved
in the roster. Without registration, mls-rs strips unknown extensions,
causing `LeafNodeExt.binding_id` to be missing and fingerprint mismatches.

## Not Yet Implemented

- **Historical roster reconstruction** for past-epoch proposal
  validation. Currently returns `None` for `epoch < current_epoch`.

## File Changes

| File | Change |
|------|--------|
| `sync-acceptor/src/state_store.rs` | Removed `promised`, `groups`, `epoch_rosters` keyspaces. Added `snapshots`. `SlimAccepted` struct. Sentinel promised. Logarithmic pruning. |
| `sync-acceptor/src/acceptor.rs` | `store_snapshot` / `store_initial_snapshot` using `ExternalSnapshot`. Roster lookup `>=` for future epochs. `is_fingerprint_in_roster` for message sender validation. |
| `sync-acceptor/src/registry.rs` | `create_acceptor_from_snapshot`. `get_group` loads from snapshot + replay (from `snapshot_epoch` inclusive). Removed `check_sender_in_roster` stub. |
| `sync-acceptor/src/server.rs` | `handle_message_request` takes `GroupAcceptor` reference for roster check. |
| `sync-acceptor/src/main.rs` | `ExternalClient` registers `SYNC_EXTENSION_TYPE` and `SYNC_PROPOSAL_TYPE`. |
| `sync-acceptor/Cargo.toml` | Added `sha2` dependency. |
| `paxos/src/acceptor/runner.rs` | `handler_applied_epoch` tracking for catch-up. |
| `paxos/src/acceptor/handler.rs` | Added `acceptor()` immutable accessor. |
| `sync-testing/tests/integration.rs` | `test_state_store_snapshot_persistence` replacing old group persistence test. |
