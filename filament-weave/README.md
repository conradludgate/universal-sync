# filament-weave

Device/client library for the [filament](../README.md) sync engine.

`filament-weave` manages group membership, consensus, and encrypted CRDT
synchronisation on behalf of a single device. All network I/O goes through
[iroh](https://iroh.computer) QUIC connections to federated spool servers.

## Architecture

Each `Weaver` spawns a background actor that maintains connections to every
spool, drives consensus rounds for commits, and decrypts incoming application
messages. The caller interacts with the weaver through an async handle.

```text
Weaver (handle)
  ├─► GroupActor (owns WeaverLearner, Proposer, QuorumTracker)
  │     ├─► AcceptorActor[0] ──► iroh connection to spool
  │     ├─► AcceptorActor[1] ──► iroh connection to spool
  │     └─► AcceptorActor[n] ──► iroh connection to spool
  └─► app_message_rx (decrypted messages)
```

Per spool, a device opens two streams:

- **Proposal stream** — consensus for commits (membership changes, key
  rotation, compaction).
- **Message stream** — CRDT application messages, routed to a deterministic
  subset of spools via rendezvous hashing.

## Key types

| Type | Purpose |
|------|---------|
| `WeaverClient` | Entry point — creates and joins groups |
| `Weaver` | Async handle to a live group with background actors |
| `WeaverEvent` | Notifications emitted by a weaver (membership, connectivity, compaction) |
| `WeaverContext` | Point-in-time snapshot of group metadata |
| `JoinInfo` | Returned when joining a group; includes the protocol name and optional CRDT snapshot |
| `WeaverLearner` | Paxos learner wrapping an MLS group (implements `filament_warp::Learner`) |
| `IrohConnector` | `Connector` implementation for iroh-based p2p QUIC |
| `ConnectionManager` | Shared connection cache across groups |
| `WeaverError` / `LearnerError` / `ConnectorError` | Error markers for `error_stack::Report` |

## Usage

```rust,ignore
use filament_weave::{WeaverClient, Weaver, WeaverEvent};

// 1. Build an MLS client and iroh endpoint (application-specific setup).
let client: mls_rs::Client<C> = /* ... */;
let signer: SignatureSecretKey = /* ... */;
let cipher_suite: CS = /* ... */;
let endpoint: iroh::Endpoint = /* ... */;

// 2. Create a WeaverClient.
let mut wc = WeaverClient::new(client, signer, cipher_suite, endpoint);

// 3. Create a new group with one or more spool addresses.
let mut weaver = wc.create(&[spool_addr], "my-crdt-v1").await?;

// 4. Send and receive CRDT updates.
weaver.send_update(&mut my_crdt).await?;
weaver.wait_for_update(&mut my_crdt).await;

// 5. Handle events (e.g. compaction).
let mut events = weaver.subscribe();
loop {
    match events.recv().await {
        Ok(WeaverEvent::CompactionNeeded { level }) => {
            weaver.compact(&my_crdt, level).await?;
        }
        _ => {}
    }
}
```

### Joining an existing group

```rust,ignore
// On the new device, generate a key package and share it out-of-band.
let kp = wc.generate_key_package()?;

// On an existing member's device:
weaver.add_member(kp).await?;

// Back on the new device, receive the welcome:
let welcome_bytes = wc.recv_welcome().await.unwrap();
let join_info = wc.join(&welcome_bytes).await?;
let mut weaver = join_info.group;
// Initialise the CRDT from join_info.snapshot if present.
```

## Related crates

| Crate | Role |
|-------|------|
| `filament-core` | Shared types, protocol messages, CRDT traits, MLS extensions |
| `filament-warp` | Core Multi-Paxos state machines |
| `filament-spool` | Server library — Paxos acceptor, message storage, backfill |
