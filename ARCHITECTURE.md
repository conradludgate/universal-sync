# Architecture Details

Implementation details for the filament protocol. For a high-level
overview, see [README.md](README.md).

## MLS Extensions and Proposals

Custom MLS extensions carry protocol-specific data. All share a single
extension type (`0xF796`) in the private-use range, disambiguated by which
MLS context they appear in.

| Extension / Proposal | Context | Purpose |
|-----------------------|---------|---------|
| `GroupContextExt` | Group context | CRDT type identifier and compaction config |
| `KeyPackageExt` | Key package | Member iroh address and supported CRDT types |
| `GroupInfoExt` | Group info | Current acceptor list and optional CRDT snapshot |

Custom proposals share a single proposal type (`0xF796`) via the
`SyncProposal` enum:

| Proposal | Purpose |
|----------|---------|
| `SyncProposal::AcceptorAdd` | Add an acceptor server to the group |
| `SyncProposal::AcceptorRemove` | Remove an acceptor server from the group |
| `SyncProposal::CompactionClaim` | Claim a compaction lease |
| `SyncProposal::CompactionComplete` | Mark compaction as finished |

## Paxos Protocol Details

### Ballot Scheme

Ballots are `(Epoch, Attempt, MemberId)` with lexicographic ordering.
`MemberId` is the MLS leaf index, guaranteed unique within the group.

### Two-Phase Protocol

Every round requires two phases (Prepare → Promise, Accept → Accepted).
Multi-Paxos leader optimisation is explicitly disabled — every Accept must
match an exact prior Promise. Contention between concurrent proposers is
resolved by incrementing the attempt number and retrying.

```mermaid
sequenceDiagram
    participant D as Device (Proposer)
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    participant A3 as Acceptor 3

    Note over D: Prepare MLS commit

    D->>A1: Prepare(epoch, attempt, member_id)
    D->>A2: Prepare(epoch, attempt, member_id)
    D->>A3: Prepare(epoch, attempt, member_id)

    A1-->>D: Promise
    A2-->>D: Promise

    Note over D: Quorum (2/3) reached

    D->>A1: Accept(proposal, mls_commit)
    D->>A2: Accept(proposal, mls_commit)
    D->>A3: Accept(proposal, mls_commit)

    A1-->>D: Accepted
    A2-->>D: Accepted

    Note over D: Quorum accepted — apply commit

    A1-->>A2: Accepted (gossip)
    A1-->>A3: Accepted (gossip)
    A2-->>A1: Accepted (gossip)
    A2-->>A3: Accepted (gossip)

    Note over A1,A3: All acceptors learn and advance epoch
```

### Acceptor Membership Changes

Acceptor membership changes (`AcceptorAdd` / `AcceptorRemove`) are themselves
MLS custom proposals agreed through Paxos. Quorum size updates atomically
when the commit is applied.

## iroh Connection Architecture

Each device opens two bidirectional streams per acceptor via iroh:

1. **Proposal stream** — Paxos Prepare/Accept messages.
2. **Message stream** — application message send, subscribe, and backfill.

```mermaid
graph TB
    subgraph device [Device]
        GA[GroupActor]
        AA1[AcceptorActor 1]
        AA2[AcceptorActor 2]
        GA --> AA1
        GA --> AA2
    end

    subgraph acc1 [Acceptor 1]
        PS1[Proposal Stream]
        MS1[Message Stream]
    end

    subgraph acc2 [Acceptor 2]
        PS2[Proposal Stream]
        MS2[Message Stream]
    end

    AA1 -- "iroh BiDi" --> PS1
    AA1 -- "iroh BiDi" --> MS1
    AA2 -- "iroh BiDi" --> PS2
    AA2 -- "iroh BiDi" --> MS2
```

## CRDT Message Identity and Deduplication

CRDT updates are MLS application messages identified by
`(MemberFingerprint, seq)` where `MemberFingerprint` is a SHA-256 of the
sender's MLS signing key. This identity is stable across MLS epochs.

Messages are deduplicated at two levels:

- **Acceptor**: storage key is `(group_id, sender_fingerprint, seq)` —
  duplicate writes overwrite silently.
- **Device**: in-memory `HashSet<(MemberFingerprint, seq)>` filters
  duplicates before CRDT application.

A per-sender state vector tracks the highest received sequence number,
used for backfill requests.

## Message Routing

Not every application message needs to reach every acceptor. Messages are
routed to a subset of acceptors selected by rendezvous hashing (Highest
Random Weight, using xxh3). The number of target acceptors is determined
by an exponential delivery function:

```
delivery_count(level, counts_len, n) = ceil(e^(level - counts_len) * n)
```

For regular application messages (`level=0`, default `counts_len=2`),
this gives `ceil(e^-2 * n)` ≈ 13.5% of acceptors (minimum 1). Higher
compaction levels replicate to progressively more acceptors, approaching
all acceptors at the highest level. This distributes storage load while
maintaining deterministic routing — any device can compute which
acceptors hold a given message.

Devices connect to all acceptors and subscribe to live broadcasts plus
backfill, so they receive the full message set across all acceptors.

```mermaid
sequenceDiagram
    participant D as Device A
    participant A1 as Acceptor 1
    participant A2 as Acceptor 2
    participant A3 as Acceptor 3
    participant E as Device B

    Note over D: Edit document locally

    D->>D: Encrypt CRDT update (MLS)
    D->>A1: Send(message_id, ciphertext)
    D->>A2: Send(message_id, ciphertext)
    Note right of A3: Not selected by rendezvous hash

    A1-->>E: Broadcast(message_id, ciphertext)
    E->>E: Decrypt and apply CRDT update

    Note over E: Comes online later, backfills

    E->>A1: Backfill(state_vector)
    E->>A2: Backfill(state_vector)
    E->>A3: Backfill(state_vector)
    A1-->>E: Messages not in state_vector
    A2-->>E: Messages not in state_vector
    A3-->>E: (none stored)
    E->>E: Deduplicate and apply
```

## Acceptor Storage

Acceptors use [fjall](https://github.com/fjall-rs/fjall) (an LSM-tree
storage engine) with three keyspaces:

| Keyspace | Key | Value |
|----------|-----|-------|
| `accepted` | `(group_id, epoch)` | `SlimAccepted` (proposal + message); sentinel at `epoch=u64::MAX` stores the last promised proposal |
| `snapshots` | `(group_id, epoch)` | `ExternalSnapshot` bytes (full group state for crash recovery) |
| `messages` | `(group_id, sender_fingerprint, seq)` | Encrypted application message |

Accepted and promised writes are fsynced before acknowledging. Snapshot
writes do not fsync (recoverable from accepted values). On server restart,
the latest snapshot is loaded and only subsequent accepted commits are
replayed — not from epoch 0. Snapshots are pruned logarithmically.

See [plan/ACCEPTOR-STORAGE.md](plan/ACCEPTOR-STORAGE.md) for details.
