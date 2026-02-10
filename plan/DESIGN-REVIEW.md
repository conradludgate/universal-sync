# Design Review — Findings Summary

Review of the filament architecture against the README design claims,
cross-referenced with the actual codebase.

## Trust Model

The safety/liveness separation is clean: MLS cryptography (signatures + AAD)
guarantees message integrity and confidentiality. Acceptors affect liveness only.
A colluding quorum can cause DoS but cannot decrypt, forge, or reorder messages.

**Caveat**: A colluding quorum _can_ cause a split-brain in the MLS group epoch
by accepting two different values for the same Paxos slot. This permanently
breaks the group (devices fork onto different epoch chains) but does not
compromise message secrecy. There is no detection or recovery mechanism for
this scenario — it requires manual intervention (creating a new group).

Metadata privacy (who is in a group, message timing/frequency) is leaked to
acceptors. This is an accepted trade-off (owner confirmed not a concern).

Members within a group are fully trusted. This means compaction results are
not independently verified — the compactor is trusted to produce a correct
merged snapshot.

## Paxos Consensus

### Ballot Scheme

Ballots are `(Epoch, Attempt, MemberId)` with lexicographic ordering.
`MemberId` is the MLS leaf index (`u32`), guaranteed unique by MLS.

### No Multi-Paxos Optimization

Every Accept must match an exact prior Promise. No leader election, no
skipping Prepare phase. This is a deliberate safety-over-latency choice
documented in `.cursor/rules/paxos.mdc`.

### Contention Resolution

See [BACKOFF-AND-CONTENTION.md](BACKOFF-AND-CONTENTION.md).

### Acceptor Promise Race

See [ACCEPTOR-PROMISE-RACE.md](ACCEPTOR-PROMISE-RACE.md).

### Proposer Crash Recovery

See [COMMIT-CATCHUP.md](COMMIT-CATCHUP.md).

## MLS Integration

### Commit Races

When two devices race to commit at the same epoch, the loser's path is cheap:
clear the pending commit (O(1)), process the winning commit via MLS
`process_incoming_message()`. No re-derivation needed.

### Epoch Ordering

Commits must be processed sequentially. There is no queuing for out-of-order
commits. If a device misses epoch N and receives epoch N+1, it cannot process
it and must receive N first. This interacts with the commit catch-up gap.

### Forward/Backward Decryption

MLS retains old epoch keys, so messages encrypted with epoch N can be
decrypted after advancing to epoch N+1. Messages that fail to decrypt are
buffered (up to 10 attempts) and retried after epoch advances.

### Out-of-Order Application Messages

The `out_of_order` feature is enabled on mls-rs. This allows the MLS
secret tree to store intermediate keys when messages arrive out of order
within the same epoch (up to `MAX_RATCHET_BACK_HISTORY = 1024` skipped
generations per sender). Without this feature, receiving generation N
before generation M (where M < N) permanently destroys key M.

## CRDT & Application Messages

### Routing

Application messages (CRDT updates) bypass Paxos and are routed to
`ceil(sqrt(n))` acceptors via rendezvous hashing. Paxos messages go to all
acceptors. Devices connect to all acceptors and deduplicate via
`(MemberFingerprint, seq)`.

### Removed Member Edits

See [REMOVED-MEMBER-EDITS.md](REMOVED-MEMBER-EDITS.md).

### Ordering

No causal ordering for CRDT updates. Messages are processed as "apply
whatever you can decrypt." CRDTs handle convergence. Backfill returns
messages in storage order (sender + seq), not causal order.

## Acceptor Management

### AcceptorAdd/AcceptorRemove

Already implemented as MLS custom proposals (`SyncProposal`). The
COMPACTION.md Phase 5 migration note is stale — this is done.

### New Acceptor Bootstrapping

New acceptors receive `GroupInfo` and store an `ExternalSnapshot` at their
join epoch. On reconnect (`get_group()`), they load the nearest
`ExternalSnapshot` and replay only subsequent accepted commits — not from
epoch 0. See [ACCEPTOR-STORAGE.md](ACCEPTOR-STORAGE.md).

Proposal validation uses the in-memory `ExternalGroup` roster (current or
future epochs). Historical epoch roster lookup has been implemented.

### Quorum

`(n / 2) + 1`, updated atomically in `apply_proposal()` when acceptors change.

### Acceptor Failure

No replication or gossip between acceptors for application messages.
Permanent acceptor failure can lose messages stored only on that acceptor.
With `ceil(sqrt(n))` routing, messages are typically on multiple acceptors,
but the risk is non-zero.

## Key Rotation

See [KEY-ROTATION.md](KEY-ROTATION.md).

## Compaction & Offline Recovery

### Compaction Leader Election

See [COMPACTION-LEADER-ELECTION.md](COMPACTION-LEADER-ELECTION.md).

Compacted snapshots are stored as regular messages with `AuthData::Compaction`
metadata. When old messages are deleted via `delete_before_watermark`, the
compacted snapshot remains. Devices backfill with their `StateVector`, receive
the snapshot plus newer updates, and reconstruct state.

Snapshots are re-encrypted at the current epoch for offline/new members.

### Offline Duration Limits

There is no explicit offline duration limit, but practical limits exist:

- Messages that fail to decrypt after 10 attempts (`MAX_MESSAGE_ATTEMPTS`)
  are dropped.
- Messages that fail to decrypt after 3 attempts when the device is 5+
  epochs past its join epoch are also dropped.
- If compaction deletes old messages from acceptors, the compacted snapshot
  covers the deleted data, so offline devices can still catch up via
  snapshot + newer updates.
- The critical gap is commit catch-up: if a device is offline for multiple
  epochs and can't learn the intermediate commits, it gets stuck. See
  [COMMIT-CATCHUP.md](COMMIT-CATCHUP.md).

## Deferred / Not Yet Designed

### Read-Only Membership

The old README mentioned read-only members whose messages are "ignored
unless a group admin allows that member to send messages." This has not
been designed or implemented. The enforcement mechanism (cryptographic vs
policy-based) is undecided. Deferred for future consideration.

### Acceptor Message Replication

No replication or gossip between acceptors for application messages.
Permanent acceptor failure can lose messages stored only on that acceptor.
With `ceil(sqrt(n))` routing, messages are typically on multiple acceptors,
reducing but not eliminating risk. No plan to address this yet — accepted
as a trade-off of the `ceil(sqrt(n))` routing design.

## Stale Documentation

- **Old README**: Claimed p2p initial sync via iroh for new members. The
  current implementation uses compaction-based catch-up from acceptors
  instead. The README has been rewritten to reflect this.

## Issues

All issues identified in this review have been implemented.

| Issue | Severity | Status | Document |
|-------|----------|--------|----------|
| No proactive commit catch-up on proposer reconnect | **High** | **Implemented** | [COMMIT-CATCHUP.md](COMMIT-CATCHUP.md) |
| Acceptor promise sentinel TOCTOU panic | **High** | **Implemented** | [ACCEPTOR-PROMISE-RACE.md](ACCEPTOR-PROMISE-RACE.md) |
| Incremental L1 compaction deletes prior L1 | **High** | **Implemented** | [MESSAGE-STORE-KEY-FORMAT.md](MESSAGE-STORE-KEY-FORMAT.md) |
| Watermark over-claims due to message gaps | **High** | **Implemented** | [CONTIGUOUS-WATERMARK.md](CONTIGUOUS-WATERMARK.md) |
| All members drive compaction simultaneously | **Medium** | **Implemented** | [COMPACTION-LEADER-ELECTION.md](COMPACTION-LEADER-ELECTION.md) |
| Removed member edits silently dropped | **Medium** | **Implemented** | [REMOVED-MEMBER-EDITS.md](REMOVED-MEMBER-EDITS.md) |
| Out-of-order app messages cause permanent key loss | **Medium** | **Implemented** | mls-rs `out_of_order` feature enabled |
| Pending message retries only on epoch advance | **Low** | **Implemented** | — |
| Backoff is linear, no jitter, max 3 retries | **Low** | **Implemented** | [BACKOFF-AND-CONTENTION.md](BACKOFF-AND-CONTENTION.md) |
| No time-based key rotation floor | **Low** | **Implemented** | [KEY-ROTATION.md](KEY-ROTATION.md) |
