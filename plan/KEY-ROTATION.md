# Key Rotation

**Status**: Implemented

## Problem

There was no time-based key rotation floor. Key rotation occurred only
when triggered manually or as a side effect of compaction commits. In a
low-activity group, keys could go indefinitely without rotating.

MLS provides post-compromise security (PCS) through epoch advancement.
Without time-based rotation, the PCS window was unbounded for quiet
groups — an attacker who compromised a device key could silently read
all future messages until activity resumed.

## Current Design

A periodic key rotation timer in `GroupActor` automatically proposes an
`UpdateKeys` commit when no epoch advance has occurred within a
configurable duration.

### Configuration

Stored in `GroupContextExt` (`filament-core/src/extension.rs`):

```rust
pub struct GroupContextExt {
    pub crdt_type_id: String,
    pub compaction_config: CompactionConfig,
    pub key_rotation_interval_secs: Option<u64>,
}
```

- `Some(secs)` — enable time-based rotation with the given interval
- `None` — disable time-based rotation
- Default: `Some(86400)` (24 hours), set in `Group::create()`

The interval is part of the MLS group context, ensuring all members share
the same configuration.

### Timer Mechanism

`GroupActor` (`filament-weave/src/group/group_actor.rs`) has two fields:

```rust
last_epoch_advance: std::time::Instant,
key_rotation_interval: Option<std::time::Duration>,
```

The event loop includes a `key_rotation_check` timer branch, guarded by
`if self.key_rotation_interval.is_some()`. The timer interval matches
the configured rotation interval (minimum 1 second).

### `maybe_trigger_key_rotation()`

Called on each timer tick. Mirrors the `maybe_trigger_compaction()` pattern:

1. Return early if rotation is disabled (`key_rotation_interval` is `None`)
2. Return early if an active proposal is in flight
3. Return early if no acceptors are connected
4. Return early if `last_epoch_advance.elapsed() < interval`
5. Call `handle_update_keys()` via a fire-and-forget oneshot channel

### Timer Reset

`last_epoch_advance` is reset to `Instant::now()` in `apply_proposal()`
immediately after emitting `GroupEvent::EpochAdvanced`. This means any
epoch advance — manual `UpdateKeys`, member add/remove, compaction, or
time-based rotation — resets the timer.

### Contention Handling

No special coordination needed. If multiple devices independently trigger
rotation:

- Only one proposal wins via Paxos consensus
- All devices observe the epoch advance via `apply_proposal()`, resetting
  their timers
- The losing device's proposal is rejected; its timer resets from the
  winning commit's epoch advance

### Threading

The interval is threaded through the stack:

1. `GroupContextExt::new()` accepts `key_rotation_interval_secs: Option<u64>`
2. `Group::create()` passes `Some(86400)` (default 24h)
3. `Group::join()` extracts from `group_ctx.key_rotation_interval_secs`
4. `Group::spawn_actors()` converts to `Option<Duration>` and passes to
   `GroupActor::new()`

### Edge Cases

- **Device offline**: Timer continues based on local `Instant`. On
  reconnect, if another device already rotated, `apply_proposal()`
  resets the timer. If not, the next tick triggers rotation.
- **Proposal already in flight**: `maybe_trigger_key_rotation()` returns
  early. The next tick retries.
- **No acceptors**: Returns early. Rotation triggers once acceptors are
  added.
- **Concurrent with compaction**: Both are safe — worst case is two epoch
  advances in quick succession (harmless extra rotation).
- **Configuration changes**: `GroupContextExt` is immutable once set in
  the MLS group context. Changing the interval requires recreating the
  group or a future config-update proposal mechanism.
