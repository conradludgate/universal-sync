# Removed Member Edits

**Status**: Implemented

## Problem

When a member is removed from the group via Paxos commit, their CRDT updates
sent before removal may be silently dropped by other devices.

### Scenario

1. Device A sends CRDT updates at epoch 4 (encrypted with epoch 4 keys).
2. Device B proposes removing Device A. This wins Paxos consensus.
3. The group advances to epoch 5. Device A is no longer in the roster.
4. Device C comes online, backfills from acceptors, receives Device A's
   epoch 4 messages.
5. Device C checks the roster (now epoch 5) — Device A is not found.
6. Device A's pre-removal edits are dropped.

Whether this is a problem depends on the design choice below.

## Design Decision: Drop Pre-Removal Edits

Removal reverts a member's unprocessed contributions. This is a deliberate
security posture: removing a member revokes both future access and any
in-flight messages that haven't yet been processed. Compaction provides
the reconciliation path for edits that were processed by at least one
device before the removal.

### Three-Layer Enforcement

1. **Acceptor gating** — Acceptors check `MessageId.sender` against the
   latest epoch roster before storing a message. If the sender is no longer
   in the roster, the message is rejected. This is a best-effort filter
   (acceptors may have stale rosters), not a security boundary.

2. **Device rejection** — Devices reject decrypted messages whose
   `sender_index` is not in their current roster. This is the authoritative
   check. MLS decryption reveals the true sender identity; MLS signatures
   prevent forgery.

3. **Compaction reconciliation** — Compacted snapshots are signed by the
   compacting member (a current member). Devices accept them regardless of
   whether the snapshot's contents include removed members' edits. If any
   device processed a removed member's edit before the removal, the edit
   will appear in that device's next compaction, propagating it to all
   other devices.

### Prerequisite: Reliable Commit Catch-Up

Devices must actively backfill and update their group consensus before
reading new messages. If a device has a stale roster it may accept messages
from a member that has already been removed, creating temporary divergence.
Compaction resolves this, but reliable commit catch-up
(see [COMMIT-CATCHUP.md](COMMIT-CATCHUP.md)) minimises the window.

## Eventual Consistency

The design maintains eventual consistency through compaction:

- If at least one device processed a removed member's edit before the
  removal, the edit will appear in that device's next compaction snapshot,
  propagating to all other devices.
- If no device processed the edit before the removal, it is permanently
  lost. This is intentional.
- CRDT merge is commutative, associative, and idempotent. Compaction order
  does not matter — all devices converge after enough rounds.

**Key invariant**: a pre-removal edit is either lost (no device ever
processed it) or eventually propagated to all devices (at least one did).
There is no state where some devices permanently have it and others
permanently don't.

## Security

- **Acceptor check is a soft filter**: `MessageId.sender` is self-reported;
  the acceptor cannot verify MLS authorship. A removed member could forge
  the fingerprint to match a current member, but devices detect this after
  MLS decryption. Consistent with the trust model (acceptors are untrusted
  for integrity).
- **Device check is the real enforcement**: MLS decryption reveals the true
  `sender_index`, and MLS signatures prevent forgery of sender identity.
- **Compaction trust is unchanged**: Members are already fully trusted. A
  compacting member could include arbitrary data in a snapshot — this is a
  pre-existing accepted risk.
- **Stronger post-compromise posture**: Dropping removed members' pending
  messages means a compromised-then-removed member's in-flight messages are
  rejected by up-to-date devices.

## Trade-Offs

- **Non-deterministic data survival**: Whether pre-removal edits survive
  depends on whether any device processed them before the removal commit.
  This is timing-dependent. The same edit-then-removal sequence can have
  different outcomes depending on network timing.
- **Acceptor roster staleness**: Different acceptors may have different
  roster views, so a message might be accepted by some and rejected by
  others. The device-level check is authoritative.
- **Compaction before message deletion**: `delete_before_watermark` only
  removes messages covered by a completed compaction's watermark. If no
  device processed a removed member's messages, no compaction covers them,
  so they persist on acceptors until a future compaction does. No silent
  loss beyond the intentional drop.

## Implementation

### Acceptor: sender roster check

**Implemented** via `GroupAcceptor::is_fingerprint_in_roster()`. When a
message stream is opened, `handle_message_stream` loads the `GroupAcceptor`
(which contains the `ExternalGroup` with the current roster). Each
incoming `MessageRequest::Send` is checked against the roster by computing
`MemberFingerprint` for each roster member and comparing.

The check uses the roster at connection time — it does not update as new
commits are learned during the stream's lifetime. This is a best-effort
filter (see "Security" section above). See
[ACCEPTOR-STORAGE.md](ACCEPTOR-STORAGE.md) for storage details.

### Device: log level change

Change the `warn!("sender not found in roster, dropping message")` in
`handle_decrypted_message` (`filament-weave/src/group/group_actor.rs`) to
`debug!`. Dropping messages from removed members is now expected behaviour,
not a bug.

No logic changes — the existing device-side behaviour already matches
the design.

### Compaction: no changes

Compaction messages are signed by the compacting member (a current member),
so they pass the roster check. The compacted snapshot's contents are
opaque — removed members' edits inside are accepted implicitly.
