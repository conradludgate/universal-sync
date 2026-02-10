# Compaction Leader Election

**Status**: Implemented

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

Force-compaction paths (`Add`) emitted `CompactionNeeded`
on **all** weavers unconditionally.

An unused `CompactionClaim` scaffolding (`submit_compaction_claim`,
`ActiveCompactionClaim`, `active_claims`, `has_active_claim`,
`COMPACTION_DEADLINE_SECS`) existed but was never called.

### Consequences

- N-way Paxos contention on every compaction round
- Under high throughput (stress test with 10 members), concurrent
  compaction proposals caused epoch divergence — members advanced to
  different epochs and could no longer decrypt each other's messages
- Acceptor panics from the promise sentinel race
  (see [ACCEPTOR-PROMISE-RACE.md](ACCEPTOR-PROMISE-RACE.md))

## Current Design

Location: `filament-weave/src/group/group_actor.rs`, `filament-weave/src/group/mod.rs`, `filament-core/src/extension.rs`

### Leader Election (threshold compaction)

Before emitting `CompactionNeeded`, the group actor calls
`should_drive_compaction(level)` which uses rendezvous hashing (Highest
Random Weight) to elect a single compactor:

- A key is built from `(level, state_vector / COMPACTION_BASE)` where the
  state vector is a sorted `BTreeMap`, so all weavers produce the same key
- Each roster member is scored: `xxh3(member_fingerprint || key)`
- The member with the highest score wins
- The chosen compactor rotates roughly every `COMPACTION_BASE` messages
  from any member, with no extra bookkeeping

This follows the same rendezvous hashing pattern used for acceptor
selection in `rendezvous.rs`.

### Add compaction gating

`Add` proposals only emit `CompactionNeeded { force: true }` on the
committer (the weaver that issued the Add commit), checked via
`committer_fingerprint == Some(self.own_fingerprint)`.

### Cascade guard in handle_compact_snapshot

Before executing compaction, `handle_compact_snapshot` re-checks
`cascade_target()`. If another weaver's `CompactionComplete` was processed
since `CompactionNeeded` was emitted, the counts will have been reset and
the compaction is skipped. This guard is bypassed for forced compactions
(Add).

### Dead code cleanup

Removed unused `CompactionClaim` scaffolding:
- `submit_compaction_claim`, `ActiveCompactionClaim`, `active_claims`,
  `has_active_claim`, `prune_expired_claims`, `COMPACTION_DEADLINE_SECS`,
  `CompactionClaimed` event variant

`SyncProposal::CompactionClaim` has been removed from `filament-core`,
`filament-weave`, and `filament-spool`.

### Properties

- Expected 1 member drives each compaction round
- No coordination or extra messages required
- Deterministic: all members agree on who should compact (assuming
  consistent state vector view)
- Rotates: the elected compactor changes as the state vector advances
- Degrades gracefully: if the elected member is offline, compaction is
  delayed until the state vector changes elect a different member

## Not Yet Implemented

- **Fallback timeout**: If the elected leader is offline and doesn't
  compact, other members don't currently step in.
