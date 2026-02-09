# Commit Catch-Up

**Status**: Implemented

## Problem

When a proposer (device) reconnects after being offline, it had no
efficient mechanism to learn about commits that occurred while it was
away.

### Previous Behaviour

1. On reconnect, the acceptor subscribed to commits starting from epoch 0
   (`Default::default()`), not the proposer's current epoch. This caused
   wasteful resends of all historical commits.
2. The handshake (`Handshake::JoinProposals(GroupId)`) did not include
   epoch information, so the acceptor had no way to know the proposer's
   current state.
3. `message_seq` was in-memory only. On crash, it reset to 0, risking
   sequence number reuse and message overwrites on the acceptor (keyed
   by `(group_id, sender_fingerprint, seq)`).

### Consequences

- Wasteful resend of all historical commits on every reconnect
- `message_seq` reuse on crash could overwrite messages not yet received
  by all peers
- Devices stuck at stale epochs if they couldn't learn intermediate
  commits from a quorum

## Current Design

Two changes: epoch-aware subscription and `message_seq` persistence.

### Part A: Epoch-Aware Subscription

#### Handshake

`Handshake::JoinProposals` is now a struct variant with `since_epoch`:

```rust
pub enum Handshake {
    JoinProposals { group_id: GroupId, since_epoch: Epoch },
    // ...
}
```

This is a breaking protocol change (no backward compatibility shim).

#### Proposer Side

`AcceptorActor` stores `current_epoch: Epoch`, set from
`self.learner.mls_epoch()` when the actor is spawned in
`GroupActor::spawn_acceptor_actor()`.

`ConnectionManager::open_proposal_stream()` accepts `since_epoch: Epoch`
and constructs the struct-variant handshake.

#### Acceptor Side

`handle_proposal_stream()` accepts `since_epoch: Epoch` and passes it to
`run_acceptor_with_epoch_waiter()`.

The acceptor runner (`filament-warp/src/acceptor/runner.rs`) accepts
`since_round: RoundId` and uses it for the initial subscription:

```rust
let initial_subscription = handler.state().subscribe_from(since_round).await;
```

Previously this was `Default::default()` (epoch 0). Now it starts from
the proposer's known epoch, avoiding resend of already-processed commits.

#### Acceptor-to-Acceptor Learning

`filament-spool/src/learner.rs` also uses the struct-variant handshake,
passing `current_round` as `since_epoch`.

### Part B: `message_seq` Persistence

#### Storage

`FjallGroupStateStorage` (`filament-weave/src/group_state.rs`) has a
`proposer_meta` keyspace:

```rust
struct FjallGroupStateStorageInner {
    db: Database,
    keyspace: Keyspace,
    proposer_meta: Keyspace,
}
```

Two methods operate on it:

- `get_message_seq(group_id) -> Option<u64>` — returns the persisted
  sequence number, or `None` if never set
- `set_message_seq(group_id, seq)` — persists the sequence number

Keys are prefixed with `seq:` followed by the group ID bytes. Values are
8-byte big-endian `u64`.

#### Recovery

On restart, `message_seq` is loaded from storage before sending any
messages. If no persisted value exists, it defaults to 0 (new group or
pre-migration state). This prevents sequence number reuse after crashes.

### File Changes

| File | Change |
|------|--------|
| `filament-core/src/protocol.rs` | `JoinProposals` changed from tuple to struct variant with `since_epoch` |
| `filament-weave/src/connection.rs` | `open_proposal_stream` accepts `since_epoch: Epoch` |
| `filament-weave/src/connector.rs` | `register_group`, `ProposalRequest/Response`, `make_proposal_streams` |
| `filament-weave/src/group/acceptor_actor.rs` | `AcceptorActor` stores `current_epoch: Epoch` |
| `filament-weave/src/group/group_actor.rs` | Passes `learner.mls_epoch()` when spawning `AcceptorActor` |
| `filament-spool/src/server.rs` | Extracts `since_epoch`, passes to runner |
| `filament-spool/src/learner.rs` | Uses `current_round` as `since_epoch` |
| `filament-warp/src/acceptor/runner.rs` | `run_acceptor_with_epoch_waiter` accepts `since_round` |
| `filament-weave/src/group_state.rs` | `proposer_meta` keyspace with `get/set_message_seq` |
| `filament-weave/src/lib.rs` | Added `group_state` module |

### Not Yet Implemented

- **`state_vector` persistence**: Currently reset to empty on restart,
  causing full re-backfill. Wasteful but correct (CRDTs are idempotent).
  Could be persisted in `proposer_meta` for efficiency.
- **`message_seq` loading in `GroupActor::run()`**: The storage
  infrastructure is in place but `GroupActor` does not yet load or
  persist `message_seq` during operation. The `get_message_seq` /
  `set_message_seq` methods are available and tested.

### Remaining In-Memory State

The following state is still lost on crash (acceptable):

- `active_proposal` — pending proposal (restarted on next attempt)
- `attempt` — proposal attempt number (reset to default)
- `quorum_tracker` — quorum state (recreated)
- `seen_messages` — deduplication set (reset; CRDTs handle duplicates)
