# Compaction Leader Election

**Status**: In Progress

## Problem

When the compaction threshold is reached, every group member independently
emits `CompactionNeeded` and the application calls `compact()`. With
multiple members active, this causes concurrent Paxos proposals for the
same compaction round. Only one proposal can win; the rest contend, retry,
and waste resources.

### Previous Behaviour

`handle_send_message` checked `cascade_target()` and `has_active_claim()`
but did not consider whether *this* member should be the one to drive
compaction. Every member that saw the threshold would emit the event.

Force-compaction paths (`Add`, `ExternalInit`) emitted `CompactionNeeded`
on **all** weavers unconditionally.

### Consequences

- N-way Paxos contention on every compaction round
- Under high throughput (stress test with 10 members), concurrent
  compaction proposals caused epoch divergence — members advanced to
  different epochs and could no longer decrypt each other's messages
- Acceptor panics from the promise sentinel race
  (see [ACCEPTOR-PROMISE-RACE.md](ACCEPTOR-PROMISE-RACE.md))

## Current Design

Location: `filament-weave/src/group/group_actor.rs`

### Leader Election (threshold compaction)

Before emitting `CompactionNeeded`, the group actor calls
`should_drive_compaction(level)` which deterministically picks one member
per (epoch, level) pair:

```rust
fn should_drive_compaction(&self, level: u8) -> bool {
    let member_count = roster().members().len();
    let my_index = current_member_index() as usize;
    let epoch = mls_epoch().0;
    let hash = xxhash_rust::xxh3::xxh3_64(&[&epoch.to_le_bytes()[..], &[level]].concat());
    hash % (member_count as u64) == (my_index % member_count) as u64
}
```

- Uses xxh3 over `(epoch, level)` to pick a deterministic slot
- Compares against the member's own MLS roster index
- Different epochs and levels elect different leaders

### ExternalInit compaction (invited_by)

When a member joins via external commit, the inviter's fingerprint is
embedded in the MLS commit's `authenticated_data` so only the inviter
compacts:

1. `GroupInfoExt` carries `invited_by: Option<MemberFingerprint>`, set
   when generating GroupInfo for external commit
2. The joiner extracts it and passes it via
   `ExternalCommitBuilder::with_authenticated_data()`
3. Existing weavers read `CommitMessageDescription::authenticated_data`
   and decode the inviter fingerprint
4. Only the inviter emits `CompactionNeeded { force: true }`

If `authenticated_data` is empty or unparsable (old clients), no weaver
compacts on that `ExternalInit` — the threshold path catches up eventually.

### Properties

- Expected 1 member drives each compaction round
- No coordination or extra messages required
- Deterministic: all members agree on who should compact (assuming
  consistent roster view at the same epoch)
- Degrades gracefully: if the elected member is offline, compaction is
  delayed until the next epoch/level change elects a different member

## Not Yet Implemented

- **Add compaction gating**: `Add` proposals still emit `CompactionNeeded`
  on all weavers. Should be gated on `committer_fingerprint` matching own.
- **Cascade guard**: `handle_compact_snapshot` should re-check
  `cascade_target()` before proceeding, to skip work if another weaver's
  `CompactionComplete` arrived in the interim.
- **Rotating leader election**: `should_drive_compaction` always picks the
  same member for a given (epoch, level). Should hash over
  `state_vector / COMPACTION_BASE` to rotate the chosen compactor.
- **Dead code cleanup**: Remove unused `CompactionClaim` scaffolding
  (`submit_compaction_claim`, `ActiveCompactionClaim`, `active_claims`,
  `has_active_claim`, `COMPACTION_DEADLINE_SECS`).
- **Fallback timeout**: If the elected leader is offline and doesn't
  compact, other members don't currently step in.
