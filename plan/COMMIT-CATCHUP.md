# Commit Catch-Up Gap

## Problem

When a proposer (device) reconnects after being offline, it has no mechanism
to proactively learn about commits that occurred while it was away.

### Current Behaviour

1. On reconnect, `AcceptorActor` opens a proposal stream and a message stream.
2. The message stream requests backfill with an empty `StateVector` — so
   application messages are recovered.
3. The proposal stream has a subscription mechanism, but with limitations:
   - The acceptor automatically subscribes to commits starting from epoch 0
     and forwards them to the proposer as `ProposalResponse` messages.
   - However, the subscription starts from epoch 0 (not the proposer's current
     epoch), causing wasteful resends of old commits.
   - The proposer only learns commits if it receives them from a quorum of
     acceptors — if only one acceptor is available, commits won't be learned.
   - There's no mechanism for the proposer to communicate its current epoch to
     the acceptor.
   - The proposer can also learn about past commits indirectly:
     - By attempting its own proposal and receiving `accepted` values in the
       Paxos response.
     - By failing to decrypt application messages (triggering the pending
       message buffer), which is an indirect signal.

### Consequences

- A device that restarts and has no reason to propose may fail to learn
  about commits that happened while it was offline, depending on:
  - Whether it connects to a quorum of acceptors (if only one acceptor is
    available, commits won't be learned due to quorum requirement)
  - Whether the acceptor subscription mechanism successfully delivers commits
    (it starts from epoch 0, causing wasteful resends but potentially working)
- Its MLS epoch is stale (persisted to disk via `FjallGroupStateStorage`).
- Application messages encrypted with newer epoch keys will fail to decrypt.
- These are buffered in `pending_messages` but dropped after 10 attempts or
  if the device is 5+ epochs past join epoch with 3+ failed attempts.
- The device may become stuck — it can't process new messages and can't
  advance its epoch if the subscription mechanism fails to deliver commits
  (e.g., due to insufficient acceptors for quorum).

### Additional State Loss on Crash

The following proposer state is in-memory only (lost on crash):

- `active_proposal` — pending proposal state
- `attempt` — current proposal attempt number
- `quorum_tracker` — quorum tracking state
- `state_vector` — per-sender sequence numbers (reset to empty, causing
  full re-backfill — wasteful but correct)
- `seen_messages` — deduplication set (reset, causing duplicate processing
  — idempotent due to CRDTs)
- `message_seq` — next outgoing sequence number

The `message_seq` reset is potentially problematic: if the device sends a
message with a seq that was already used pre-crash, the acceptor would
overwrite the old message (same key in storage). This could cause data loss
if the old message hasn't been received by all peers yet.

## Proposed Solution

### Option A: Commit Backfill on Proposal Stream

Add a `SubscribeCommits { since_epoch: Epoch }` handshake/request on the
proposal stream. Acceptors already store all accepted proposals
(`get_accepted_from(epoch)` exists in `FjallStateStore`). The acceptor
sends all committed proposals from `since_epoch` onwards, then switches to
live subscription.

Advantages:
- Mirrors the existing message backfill pattern
- Acceptor already has the query (`get_accepted_from_sync`)
- Device knows its last epoch from persisted MLS state

Implementation:
1. Add `ProposalRequest::Subscribe { since_epoch: Epoch }` variant
2. On proposal stream open, send subscribe request before any proposals
3. Acceptor responds with historical accepted values
4. Proposer processes them sequentially via `apply_proposal()`

### Option B: Derive Epoch from Application Messages

When application messages fail to decrypt, use this as a signal that the
epoch is stale and trigger a "probe" proposal (e.g., an empty commit or
UpdateKeys) to force learning via Paxos responses.

Advantages:
- No protocol changes needed

Disadvantages:
- Indirect, fragile, depends on receiving undecryptable messages
- Adds unnecessary Paxos rounds
- Doesn't work if no new messages are being sent

### Option C: Persist Proposer State

Persist `message_seq`, `state_vector`, and epoch-related state to disk.
This doesn't solve the catch-up problem but reduces waste on restart.

**Recommendation**: Option A, possibly combined with Option C for
`message_seq` persistence to prevent sequence number reuse.

## `message_seq` Reuse Risk

On crash, `message_seq` resets to 0. If the device had already sent messages
with seq 0, 1, 2, ... pre-crash, the new messages would reuse those sequence
numbers. Since the acceptor key is `(group_id, sender_fingerprint, seq)`,
this overwrites the old messages.

Fix: persist `message_seq` to disk, or derive it from the acceptor during
backfill (request the highest seq for this sender).

## Code Verification

### Current Behaviour - Partially Accurate

**Claim**: "The proposal stream does **not** request historical commits."

**Status**: **INACCURATE** — There IS a subscription mechanism, but it has limitations.

**Findings**:
- The acceptor DOES subscribe to commits and forwards them to proposers automatically
- In `paxos/src/acceptor/runner.rs:47`, the acceptor subscribes immediately: `handler.state().subscribe_from(Default::default()).await`
- Commits are forwarded as `AcceptorMessage` with `accepted: Some((proposal, message))` (lines 54-59)
- The proposer receives these as `ProposalResponse` messages via `AcceptorActor` (sync-proposer/src/group/acceptor_actor.rs:167)
- The proposer DOES handle them in `handle_proposal_response` (sync-proposer/src/group/group_actor.rs:1025-1100)

**Limitations**:
1. Subscription starts from epoch 0 (`Default::default()`), not the proposer's current epoch — wasteful resends
2. Requires quorum to learn — if only one acceptor is available, commits won't be learned
3. No mechanism for proposer to communicate its current epoch to the acceptor
4. The subscription is active immediately, but proposer only learns if it receives commits from a quorum of acceptors

**Files**:
- `paxos/src/acceptor/runner.rs:47` — subscription starts from epoch 0
- `sync-proposer/src/group/acceptor_actor.rs:164-177` — receives ProposalResponse
- `sync-proposer/src/group/group_actor.rs:1025-1100` — handles ProposalResponse and tracks quorum

### Consequences - Accurate

**Claim**: "A device that restarts and has no reason to propose will **never** learn about commits."

**Status**: **MOSTLY ACCURATE** — With the current subscription mechanism, a device that never proposes might learn IF:
- It connects to a quorum of acceptors
- Those acceptors send commits via the subscription
- The commits reach quorum threshold

However, this is unreliable because:
- The subscription starts from epoch 0 (wasteful, but works)
- Requires quorum (fails if < quorum acceptors available)
- No explicit request from proposer

**Claim**: "pending_messages dropped after 10 attempts or if device is 5+ epochs past join epoch with 3+ failed attempts."

**Status**: **ACCURATE** — Verified in `sync-proposer/src/group/group_actor.rs:1374-1422`:
- Line 1380: `if pending_msg.attempts >= MAX_MESSAGE_ATTEMPTS` (MAX_MESSAGE_ATTEMPTS = 10, defined in sync-proposer/src/group/mod.rs:128)
- Line 1406: `if current_epoch.0 > self.join_epoch.0 + 5 && pending_msg.attempts > 3`

**Files**:
- `sync-proposer/src/group/group_actor.rs:1374-1422` — `try_process_pending_messages()`
- `sync-proposer/src/group/mod.rs:128` — `MAX_MESSAGE_ATTEMPTS = 10`

### Additional State Loss on Crash - Accurate

**Claim**: In-memory state is lost on crash.

**Status**: **ACCURATE** — Verified in `sync-proposer/src/group/group_actor.rs`:

1. **`active_proposal`** — Line 58: `active_proposal: Option<ActiveProposal>` — not persisted
2. **`attempt`** — Line 39: `attempt: universal_sync_core::Attempt` — initialized to `Attempt::default()` on line 244
3. **`quorum_tracker`** — Line 38: `quorum_tracker: QuorumTracker` — recreated on line 243
4. **`state_vector`** — Line 54: `state_vector: StateVector` — initialized to `StateVector::new()` on line 258
5. **`seen_messages`** — Line 55: `seen_messages: HashSet<(MemberFingerprint, u64)>` — initialized to empty on line 259
6. **`message_seq`** — Line 53: `message_seq: u64` — initialized to `0` on line 257

**Files**:
- `sync-proposer/src/group/group_actor.rs:223-268` — `GroupActor::new()` initialization
- `sync-proposer/src/group/group_actor.rs:31-70` — struct definition

### `message_seq` Reuse Risk - Accurate

**Claim**: "On crash, `message_seq` resets to 0."

**Status**: **ACCURATE** — Verified:
- Line 257: `message_seq: 0` in `GroupActor::new()`
- Line 584: `let seq = self.message_seq; self.message_seq += 1` — increments on send
- Line 1619: Same pattern for compaction messages

**Claim**: "acceptor key is `(group_id, sender_fingerprint, seq)`"

**Status**: **ACCURATE** — Verified in `sync-acceptor/src/state_store.rs:135-141`:
```rust
fn build_message_key(group_id: &GroupId, sender: &MemberFingerprint, seq: u64) -> [u8; 72] {
    let mut key = [0u8; 72];
    key[..32].copy_from_slice(group_id.as_bytes());
    key[32..64].copy_from_slice(sender.as_bytes());
    key[64..72].copy_from_slice(&seq.to_be_bytes());
    key
}
```

**Files**:
- `sync-proposer/src/group/group_actor.rs:257` — initialization
- `sync-proposer/src/group/group_actor.rs:584-585` — usage in `handle_send_message`
- `sync-acceptor/src/state_store.rs:135-141` — message key construction

### Proposed Solution - Additional Context

**Option A**: The acceptor already has `get_accepted_from_sync` (sync-acceptor/src/state_store.rs:329-360) and `subscribe_from` (sync-acceptor/src/state_store.rs:704-721). The issue is that:
- `subscribe_from` starts from epoch 0, not the proposer's current epoch
- There's no `ProposalRequest::Subscribe` variant — currently only `Prepare` and `Accept` exist (sync-proposer/src/connector.rs:196-201)
- The handshake doesn't include epoch information — `Handshake::JoinProposals(GroupId)` only sends group_id (sync-core/src/protocol.rs:59)

**Files**:
- `sync-acceptor/src/state_store.rs:329-360` — `get_accepted_from_sync`
- `sync-acceptor/src/state_store.rs:704-721` — `subscribe_from` implementation
- `sync-proposer/src/connector.rs:196-201` — `ProposalRequest` enum
- `sync-core/src/protocol.rs:56-69` — `Handshake` enum

### Additional Findings

1. **MLS Epoch Persistence**: The epoch IS persisted via `FjallGroupStateStorage` — `GroupLearner` wraps an MLS `Group` which uses `GroupStateStorage` to persist state. The epoch can be retrieved via `learner.mls_epoch()` which reads from the persisted MLS group context.

2. **Acceptor Subscription Mechanism**: The acceptor's subscription mechanism (`paxos/src/acceptor/runner.rs:47`) DOES forward commits, but:
   - Starts from epoch 0, causing wasteful resends
   - The proposer must receive commits from a quorum to learn them
   - No way for proposer to specify its current epoch

3. **Quorum Tracking**: The proposer uses `QuorumTracker` to detect when quorum is reached (sync-proposer/src/group/group_actor.rs:1036-1058). This means:
   - If only one acceptor is available, commits won't be learned (no quorum)
   - Commits are only applied when quorum is reached (line 1039)

4. **Message Stream Backfill**: The message stream backfill IS implemented correctly (sync-proposer/src/group/acceptor_actor.rs:115-118) — requests with empty `StateVector` to get all messages.

5. **Proposal Stream Handshake**: The proposal stream handshake only sends `GroupId`, not epoch information (sync-proposer/src/connection.rs:102, sync-core/src/protocol.rs:59).

## Implementation Plan

This plan implements Option A (commit backfill on proposal stream) combined with Option C (persist message_seq) to fix the commit catch-up gap.

### Overview

The implementation involves:
1. Extending the proposal stream handshake to include the proposer's current epoch
2. Modifying the acceptor to start subscription from the proposer's epoch instead of epoch 0
3. Persisting `message_seq` to `FjallGroupStateStorage` to prevent sequence number reuse on crash
4. Recovering `message_seq` on restart
5. Optionally persisting `state_vector` for efficiency (reduces wasteful re-backfill)

### Step 1: Extend Handshake to Include Epoch

**Files to modify:**
- `sync-core/src/protocol.rs`
- `sync-proposer/src/connection.rs`
- `sync-proposer/src/connector.rs`
- `sync-acceptor/src/server.rs`

**Changes:**

1. **Modify `Handshake` enum** (`sync-core/src/protocol.rs`):
   - Change `JoinProposals(GroupId)` to `JoinProposals { group_id: GroupId, current_epoch: Option<Epoch> }`
   - The `Option<Epoch>` allows backward compatibility: `None` means epoch 0 (old behavior)
   - For new connections, always provide `Some(epoch)` from `learner.mls_epoch()`

   ```rust
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub enum Handshake {
       /// Join an existing group's proposal stream.
       /// `current_epoch` is the proposer's current MLS epoch (None = epoch 0 for backward compat).
       JoinProposals { 
           group_id: GroupId,
           current_epoch: Option<Epoch>,
       },
       // ... other variants unchanged
   }
   ```

2. **Update handshake creation** (`sync-proposer/src/connection.rs`):
   - In `open_proposal_stream` (line 96-104), retrieve current epoch from `GroupLearner`
   - Pass epoch in handshake: `Handshake::JoinProposals { group_id, current_epoch: Some(epoch) }`
   - Note: `GroupActor` has access to `learner` via `self.learner.mls_epoch()`
   - However, `ConnectionManager` doesn't have access to learner. We need to pass epoch as parameter to `open_proposal_stream`

   **Signature change:**
   ```rust
   pub(crate) async fn open_proposal_stream(
       &self,
       acceptor_id: &AcceptorId,
       group_id: GroupId,
       current_epoch: Epoch,  // NEW parameter
   ) -> Result<(SendStream, RecvStream), Report<ConnectorError>>
   ```

3. **Update call sites** (`sync-proposer/src/group/acceptor_actor.rs`):
   - In `run_connection` (line 80-83), retrieve epoch before opening proposal stream
   - Pass epoch to `open_proposal_stream`
   - Problem: `AcceptorActor` doesn't have direct access to `GroupActor`'s learner
   - Solution: Pass epoch via `AcceptorOutbound` message or store it in `AcceptorActor`
   - Better: Store epoch in `AcceptorActor` struct, set it when actor is created

   **Add to `AcceptorActor` struct:**
   ```rust
   pub(super) struct AcceptorActor {
       // ... existing fields
       pub(super) current_epoch: Epoch,  // NEW field
   }
   ```

   **Update `spawn_acceptor_actor`** (`sync-proposer/src/group/group_actor.rs:340`):
   ```rust
   async fn spawn_acceptor_actor(&mut self, acceptor_id: AcceptorId, addr: EndpointAddr) {
       let current_epoch = self.learner.mls_epoch();
       self.spawn_acceptor_actor_inner(acceptor_id, addr, false, current_epoch).await;
   }
   ```

   **Update `spawn_acceptor_actor_inner`** to accept and store epoch:
   ```rust
   async fn spawn_acceptor_actor_inner(
       &mut self,
       acceptor_id: AcceptorId,
       addr: EndpointAddr,
       register: bool,
       current_epoch: Epoch,  // NEW parameter
   ) {
       // ... existing code ...
       let actor = AcceptorActor {
           // ... existing fields ...
           current_epoch,  // NEW field
       };
   }
   ```

   **Update `run_connection`** (`sync-proposer/src/group/acceptor_actor.rs:77`):
   ```rust
   let proposal_streams = self
       .connection_manager
       .open_proposal_stream(&self.acceptor_id, self.group_id, self.current_epoch)
       .await;
   ```

4. **Update acceptor handshake handling** (`sync-acceptor/src/server.rs`):
   - In `handle_proposal_stream` (line 141), extract epoch from handshake
   - Store epoch for use in subscription

   ```rust
   match handshake {
       Handshake::JoinProposals { group_id, current_epoch } => {
           let since_epoch = current_epoch.unwrap_or(Epoch(0));
           handle_proposal_stream(group_id, None, since_epoch, reader, writer, registry).await
       }
       // ... other variants
   }
   ```

   **Update `handle_proposal_stream` signature:**
   ```rust
   async fn handle_proposal_stream<C, CS>(
       mut group_id: GroupId,
       create_group_info: Option<Vec<u8>>,
       since_epoch: Epoch,  // NEW parameter
       reader: FramedRead<RecvStream, LengthDelimitedCodec>,
       mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
       registry: AcceptorRegistry<C, CS>,
   ) -> Result<(), Report<ConnectorError>>
   ```

5. **Update connector** (`sync-proposer/src/connector.rs`):
   - In `IrohConnector::connect` (line 79), update handshake creation
   - However, connector doesn't have epoch - this is handled at `ConnectionManager` level

**Migration concerns:**
- Old acceptors will receive `None` for `current_epoch` (backward compatible)
- Old proposers sending `JoinProposals(GroupId)` need deserialization support
- Add `#[serde(default)]` or custom deserializer to handle old format

**Custom deserializer approach:**
```rust
#[derive(Debug, Clone, Serialize)]
pub enum Handshake {
    #[serde(rename = "JoinProposals")]
    JoinProposals { 
        group_id: GroupId,
        current_epoch: Option<Epoch>,
    },
    // ...
}

// Custom deserializer to handle both old and new formats
impl<'de> Deserialize<'de> for Handshake {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Try new format first, fall back to old format
        // Implementation details...
    }
}
```

### Step 2: Modify Acceptor Subscription to Start from Proposer's Epoch

**Files to modify:**
- `paxos/src/acceptor/runner.rs`
- `sync-acceptor/src/server.rs`
- `sync-acceptor/src/state_store.rs`

**Changes:**

1. **Pass epoch to acceptor runner** (`sync-acceptor/src/server.rs`):
   - In `handle_proposal_stream`, after getting acceptor and state, pass `since_epoch` to runner
   - Modify `run_acceptor_with_epoch_waiter` call (line 208) to accept `since_epoch` parameter

   ```rust
   run_acceptor_with_epoch_waiter(
       handler, 
       connection, 
       proposer_id, 
       epoch_rx, 
       current_epoch_fn,
       since_epoch,  // NEW parameter
   ).await
   ```

2. **Update acceptor runner** (`paxos/src/acceptor/runner.rs`):
   - Modify `run_acceptor_with_epoch_waiter` signature to accept `since_epoch: Epoch`
   - Change initial subscription from `Default::default()` (epoch 0) to `since_epoch` (line 47)

   ```rust
   pub async fn run_acceptor_with_epoch_waiter<A, S, C>(
       mut handler: AcceptorHandler<A, S>,
       conn: C,
       proposer_id: <A::Proposal as Proposal>::NodeId,
       mut epoch_rx: watch::Receiver<<A::Proposal as Proposal>::RoundId>,
       mut current_epoch_fn: impl FnMut() -> <A::Proposal as Proposal>::RoundId,
       since_epoch: <A::Proposal as Proposal>::RoundId,  // NEW parameter
   ) -> Result<(), A::Error>
   {
       // Change line 47:
       let initial_subscription = handler.state().subscribe_from(since_epoch).await;
       // ... rest unchanged
   }
   ```

3. **Handle epoch validation** (`paxos/src/acceptor/runner.rs`):
   - If `since_epoch > current_epoch`, log warning but proceed (proposer may be ahead)
   - If `since_epoch` is in the future relative to acceptor's learned state, subscription will naturally start from earliest available

**Migration concerns:**
- Old proposers will send epoch 0, which maintains current behavior
- New proposers send their actual epoch, reducing wasteful resends

### Step 3: Persist message_seq to FjallGroupStateStorage

**Files to modify:**
- `sync-proposer/src/group_state.rs`
- `sync-proposer/src/group/group_actor.rs`

**Changes:**

1. **Extend `FjallGroupStateStorage`** (`sync-proposer/src/group_state.rs`):
   - Add a new keyspace for proposer state: `proposer_state`
   - Define key format: `group_id` (32 bytes) = key for proposer state per group
   - Define value format: serialized `ProposerState` struct

   ```rust
   #[derive(Debug, Clone, Serialize, Deserialize)]
   struct ProposerState {
       message_seq: u64,
       state_vector: StateVector,  // Optional, for Step 5
   }
   ```

   **Add to `FjallGroupStateStorageInner`:**
   ```rust
   struct FjallGroupStateStorageInner {
       db: Database,
       keyspace: Keyspace,
       proposer_state: Keyspace,  // NEW
   }
   ```

   **Update `open_sync`:**
   ```rust
   let proposer_state = db.keyspace("proposer_state", KeyspaceCreateOptions::default)?;
   ```

   **Add methods:**
   ```rust
   impl FjallGroupStateStorage {
       fn proposer_state_key(group_id: &[u8]) -> &[u8] {
           group_id
       }

       pub(crate) fn get_proposer_state(&self, group_id: &[u8]) -> Result<Option<ProposerState>, GroupStateError> {
           let key = Self::proposer_state_key(group_id);
           self.inner.proposer_state
               .get(key)
               .map_err(|_| GroupStateError)
               .and_then(|opt| {
                   opt.map(|bytes| {
                       postcard::from_bytes(&bytes)
                           .map_err(|_| GroupStateError)
                   })
                   .transpose()
               })
       }

       pub(crate) fn set_proposer_state(&mut self, group_id: &[u8], state: &ProposerState) -> Result<(), GroupStateError> {
           let key = Self::proposer_state_key(group_id);
           let value = postcard::to_allocvec(state).map_err(|_| GroupStateError)?;
           self.inner.proposer_state.insert(key, &value).map_err(|_| GroupStateError)?;
           // Note: fjall persistence is handled by MLS GroupStateStorage write() calls
           // We may need explicit persist here or rely on MLS writes
           Ok(())
       }
   }
   ```

   **Note:** `FjallGroupStateStorage` implements `GroupStateStorage` which is called by MLS library. We need to ensure persistence happens. Consider:
   - Calling `db.persist()` after each write, OR
   - Batching with MLS writes (modify `write()` method to also persist proposer state)

2. **Update `GroupActor` to persist message_seq** (`sync-proposer/src/group/group_actor.rs`):
   - Add `group_state_storage: Arc<FjallGroupStateStorage>` field to `GroupActor`
   - Store reference in `GroupActor::new` (need to pass it in)
   - After incrementing `message_seq` (lines 584-585, 1619-1620), persist state

   **Add field:**
   ```rust
   pub(crate) struct GroupActor<C, CS> {
       // ... existing fields
       group_state_storage: Arc<FjallGroupStateStorage>,  // NEW
   }
   ```

   **Update `new` method:**
   ```rust
   pub(crate) fn new(
       // ... existing parameters
       group_state_storage: Arc<FjallGroupStateStorage>,  // NEW parameter
   ) -> Self {
       // ... existing initialization
       Self {
           // ... existing fields
           group_state_storage,  // NEW field
       }
   }
   ```

   **Add helper method to persist:**
   ```rust
   fn persist_proposer_state(&self) {
       let state = ProposerState {
           message_seq: self.message_seq,
           state_vector: self.state_vector.clone(),
       };
       // Use blocking or async task
       let storage = self.group_state_storage.clone();
       let group_id = self.group_id;
       tokio::task::spawn_blocking(move || {
           // Need &mut, but we have Arc - need interior mutability or separate write handle
           // Option: Use RwLock<FjallGroupStateStorage> or separate write handle
       });
   }
   ```

   **Problem:** `FjallGroupStateStorage` methods require `&mut self`, but we have `Arc<FjallGroupStateStorage>`. 

   **Solution options:**
   - Use `Arc<RwLock<FjallGroupStateStorage>>` for interior mutability
   - Create separate `ProposerStateStore` trait with async methods
   - Use `tokio::sync::RwLock` for async access

   **Recommended:** Create async wrapper:
   ```rust
   pub(crate) struct ProposerStateStore {
       inner: Arc<RwLock<FjallGroupStateStorage>>,
       group_id: GroupId,
   }

   impl ProposerStateStore {
       pub async fn get(&self) -> Result<Option<ProposerState>, GroupStateError> {
           let inner = self.inner.read().await;
           inner.get_proposer_state(self.group_id.as_bytes())
       }

       pub async fn set(&self, state: &ProposerState) -> Result<(), GroupStateError> {
           let mut inner = self.inner.write().await;
           inner.set_proposer_state(self.group_id.as_bytes(), state)
       }
   }
   ```

   **Update `handle_send_message`** (line 579):
   ```rust
   fn handle_send_message(&mut self, ...) {
       let seq = self.message_seq;
       self.message_seq += 1;
       // ... existing code ...
       
       // Persist after increment
       self.persist_proposer_state().await;  // Or use try_send to background task
   }
   ```

   **Update compaction message sending** (line 1619):
   ```rust
   let seq = self.message_seq;
   self.message_seq += 1;
   // ... existing code ...
   self.persist_proposer_state().await;
   ```

   **Performance consideration:** Don't block on persistence. Use background task or batch writes.

### Step 4: Recover message_seq on Restart

**Files to modify:**
- `sync-proposer/src/group/group_actor.rs`

**Changes:**

1. **Load proposer state in `GroupActor::new`** (`sync-proposer/src/group/group_actor.rs:223`):
   - After creating `GroupActor`, load persisted state
   - If state exists, use `message_seq` and `state_vector` from storage
   - If not, use defaults (0, empty StateVector)

   ```rust
   pub(crate) fn new(
       // ... parameters
       group_state_storage: Arc<RwLock<FjallGroupStateStorage>>,
   ) -> Self {
       // ... existing initialization
       
       // Load persisted state
       let (message_seq, state_vector) = {
           let storage = group_state_storage.read().await;  // Blocking read
           if let Ok(Some(state)) = storage.get_proposer_state(group_id.as_bytes()) {
               (state.message_seq, state.state_vector)
           } else {
               (0, StateVector::new())
           }
       };

       Self {
           // ... existing fields
           message_seq,
           state_vector,
           group_state_storage: Arc::new(ProposerStateStore { inner: group_state_storage, group_id }),
       }
   }
   ```

   **Problem:** `GroupActor::new` is not async, but we need async to read from storage.

   **Solution:** Make `GroupActor::new` async, or load state before calling `new`:
   ```rust
   pub(crate) async fn new_async(
       // ... parameters
   ) -> Result<Self, Report<GroupError>> {
       // Load state first
       let (message_seq, state_vector) = load_proposer_state(...).await?;
       
       Ok(Self {
           // ... use loaded values
       })
   }
   ```

   **Or:** Load in `run` method before main loop:
   ```rust
   pub(crate) async fn run(mut self) {
       // Load persisted state
       if let Ok(Some(state)) = self.group_state_storage.get().await {
           self.message_seq = state.message_seq;
           self.state_vector = state.state_vector;
       }
       
       self.spawn_acceptor_actors().await;
       // ... rest of run method
   }
   ```

   **Recommended:** Load in `run` method (simpler, doesn't require making `new` async).

### Step 5: Optionally Persist state_vector

**Files to modify:**
- Same as Step 3

**Changes:**

1. **Include state_vector in ProposerState** (already done in Step 3)
2. **Update state_vector persistence** (`sync-proposer/src/group/group_actor.rs`):
   - When `state_vector` is updated (lines 586, 1464-1467), persist state
   - Batch updates: don't persist on every message receive, batch periodically

   **Update `handle_decrypted_message`** (line 1424):
   ```rust
   fn handle_decrypted_message(&mut self, received: ReceivedMessage) {
       // ... existing code ...
       let hw = self.state_vector.entry(sender_fp).or_insert(0);
       if seq > *hw {
           *hw = seq;
           // Persist periodically (e.g., every 10 updates or every second)
           self.maybe_persist_state_vector();
       }
   }
   ```

   **Add batching logic:**
   ```rust
   struct GroupActor<C, CS> {
       // ... existing fields
       state_vector_update_count: u32,  // Track updates since last persist
   }

   fn maybe_persist_state_vector(&mut self) {
       self.state_vector_update_count += 1;
       if self.state_vector_update_count >= 10 {
           self.persist_proposer_state();
           self.state_vector_update_count = 0;
       }
   }
   ```

   **Or use timer-based batching:**
   - Add `state_vector_persist_timer` to `GroupActor`
   - Persist every N seconds in main loop

### Step 6: Testing Strategy

**Test cases:**

1. **Handshake epoch transmission:**
   - Test old proposer → new acceptor (should work with epoch 0)
   - Test new proposer → old acceptor (should handle missing epoch gracefully)
   - Test new proposer → new acceptor (should use actual epoch)

2. **Subscription from epoch:**
   - Proposer at epoch 5 connects, acceptor has commits from epoch 3-10
   - Verify subscription starts from epoch 5, not epoch 0
   - Verify commits 3-4 are NOT sent (wasteful resend avoided)
   - Verify commits 5-10 ARE sent

3. **message_seq persistence:**
   - Send messages, crash, restart
   - Verify `message_seq` resumes from persisted value, not 0
   - Verify no sequence number reuse

4. **state_vector persistence:**
   - Receive messages, update state_vector, crash, restart
   - Verify state_vector is recovered
   - Verify message backfill uses recovered state_vector (fewer messages requested)

5. **Backward compatibility:**
   - Old proposer (sends `JoinProposals(GroupId)`) → new acceptor
   - New proposer → old acceptor (if possible, or graceful degradation)

6. **Integration test:**
   - Device A sends commits while device B is offline
   - Device B reconnects
   - Verify device B learns commits via proposal stream backfill
   - Verify device B's epoch advances correctly
   - Verify device B can decrypt new application messages

**Test files to create/modify:**
- `sync-proposer/src/group/group_actor.rs` - add unit tests for state persistence
- `sync-acceptor/src/server.rs` - add tests for epoch handling in handshake
- `paxos/src/acceptor/runner.rs` - add tests for subscription from epoch
- Integration tests in `tests/` directory

### Implementation Order

1. **Phase 1: Protocol changes (Steps 1-2)**
   - Extend handshake with epoch
   - Update acceptor subscription
   - Test backward compatibility

2. **Phase 2: State persistence (Steps 3-4)**
   - Add proposer state storage
   - Persist and recover message_seq
   - Test crash recovery

3. **Phase 3: Optimization (Step 5)**
   - Persist state_vector
   - Test reduced backfill

4. **Phase 4: Testing (Step 6)**
   - Comprehensive integration tests
   - Performance testing

### Migration Notes

- **Backward compatibility:** Old proposers sending `JoinProposals(GroupId)` must be handled gracefully
- **Database migration:** Existing databases won't have proposer state - handle missing state as "use defaults"
- **Rollout:** Can deploy acceptor changes first (they handle both old and new handshakes), then deploy proposer changes

### Open Questions

1. **Persistence frequency:** How often should we persist `message_seq`? Every message (safe but slow) or batched?
2. **state_vector persistence:** Is the performance gain worth the complexity?
3. **Error handling:** What happens if persistence fails? Should we continue operating or fail?
4. **Concurrent access:** Multiple `GroupActor` instances for same group? Need locking strategy.
