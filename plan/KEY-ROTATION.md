# Key Rotation

## Problem

There is no time-based key rotation floor. Key rotation occurs only when
triggered manually or as a side effect of compaction commits. In a
low-activity group, keys may never rotate.

### Current Behaviour

- `handle_update_keys()` in `group_actor.rs:559-577` builds an empty
  commit (no proposals) to trigger key rotation. This is a manual
  operation.
- Compaction commits advance epochs (via `CompactionComplete` custom
  proposals), providing key rotation as a side effect, but they do not
  include `UpdateKeys` proposals.
- Compaction is triggered by message count thresholds
  (`CompactionConfig`), not by time.

### Consequences

MLS provides post-compromise security (PCS) through epoch advancement.
If a device's keys are compromised, the attacker can decrypt messages
until the group advances to a new epoch. Without time-based rotation:

- A group that is rarely edited could go weeks/months without rotating.
- The PCS window is unbounded for quiet groups.
- An attacker who compromises a device key can silently read all future
  messages until activity resumes and triggers compaction.

### When This Matters

- Groups with infrequent edits (e.g., a shared document edited weekly).
- After a suspected device compromise where the group is not actively
  being used.

## Proposed Fix

Add a periodic key rotation timer to `GroupActor`. When no commit has
occurred for a configurable duration (e.g., 24 hours), automatically
propose an `UpdateKeys` commit.

Implementation:
1. Track `last_epoch_advance: Instant` in `GroupActor`.
2. Add a `tokio::time::interval` that checks if the duration since
   `last_epoch_advance` exceeds the configured threshold.
3. If so, call `handle_update_keys()`.
4. Reset `last_epoch_advance` on every epoch advance.

The threshold should be configurable per group, with a sensible default
(e.g., 24 hours). For high-security groups, this could be set lower
(e.g., 1 hour).

Contention risk is low: if multiple devices independently trigger
rotation, only one wins via Paxos and the others observe the epoch
advance, resetting their timer.

## Code Verification

### Verified Claims

1. **`handle_update_keys()` location and behavior** ✅
   - **Location**: `sync-proposer/src/group/group_actor.rs:559-577`
   - **Behavior**: Builds an empty commit (no proposals) via `commit_builder().build()`, which advances the MLS epoch. This is correct - MLS epoch advancement occurs on any commit, even empty ones.
   - **Public API**: Exposed via `GroupRequest::UpdateKeys` in `mod.rs:89-91`, called from `Group::update_keys()` at `mod.rs:518-528`.

2. **CompactionConfig is purely threshold-based** ✅
   - **Location**: `sync-core/src/crdt.rs:86-103`
   - **Structure**: `CompactionLevel` contains only `threshold: u32` (message count) and `replication: u8`. No time-based fields exist.
   - **Default config**: 3-level config with thresholds 0, 10, 5 (`crdt.rs:105-122`).

3. **No existing key rotation timer** ✅
   - **GroupActor struct**: `group_actor.rs:31-70` - No timer-related fields for key rotation.
   - **Existing timers**: The `run()` method (`group_actor.rs:271-316`) has two timers:
     - `timeout_check`: 1-second interval for proposal timeout checks (`group_actor.rs:274`)
     - `compaction_check`: 2-second interval (`COMPACTION_CHECK_INTERVAL_SECS`) for compaction checks (`group_actor.rs:277-280`)
   - **Event loop**: Uses `tokio::select!` at `group_actor.rs:282-308` with branches for cancellation, requests, acceptor messages, and the two timers. No key rotation timer branch exists.

### Corrected Claims

1. **Compaction commits and UpdateKeys** ❌ → ✅ CORRECTED
   - **Original claim**: "Compaction commits include `UpdateKeys` proposals"
   - **Actual behavior**: Compaction commits include `CompactionComplete` custom proposals (`SyncProposal::CompactionComplete`), not `UpdateKeys` proposals. However, compaction commits DO advance epochs (see `apply_proposal()` at `group_actor.rs:1206-1208` which emits `GroupEvent::EpochAdvanced`), so the security benefit (epoch advancement) is correct, but the mechanism description was inaccurate.
   - **Evidence**: `submit_compaction_complete()` at `group_actor.rs:1691-1729` builds a commit with `custom_proposal(custom_proposal)` where the proposal is `SyncProposal::compaction_complete()`, not an UpdateKeys operation.

### Additional Findings

1. **Operations that cause epoch advancement**:
   - `handle_update_keys()` - Empty commit (`group_actor.rs:559-577`)
   - `handle_add_member()` - Add proposal (`group_actor.rs:484-531`)
   - `handle_remove_member()` - Remove proposal (`group_actor.rs:533-557`)
   - `handle_add_acceptor()` - Custom proposal (`SyncProposal::AcceptorAdd`) (`group_actor.rs:649-675`)
   - `handle_remove_acceptor()` - Custom proposal (`SyncProposal::AcceptorRemove`) (`group_actor.rs:677-703`)
   - `submit_compaction_complete()` - Custom proposal (`SyncProposal::CompactionComplete`) (`group_actor.rs:1691-1729`)

2. **Epoch advancement tracking**:
   - Epoch advancement is detected in `apply_proposal()` when a `CommitEffect` is present (`group_actor.rs:1199-1217`)
   - An `EpochAdvanced` event is emitted at `group_actor.rs:1206-1208`
   - The current epoch is available via `self.learner.mls_epoch()` (`learner.rs:194-196`)

3. **Compaction trigger mechanism**:
   - Compaction is checked periodically via `compaction_check` timer (`group_actor.rs:305-307`)
   - `maybe_trigger_compaction()` (`group_actor.rs:1497-1541`) checks thresholds and triggers compaction
   - Compaction can also be forced (e.g., after adding a member) via `force_compaction` flag (`group_actor.rs:1255-1271`)

4. **Implementation notes for proposed fix**:
   - The event loop structure (`group_actor.rs:282-308`) already supports adding additional timer branches
   - Epoch advancement is already tracked via `GroupEvent::EpochAdvanced` events, which could be used to reset a `last_epoch_advance` timer
   - The `handle_update_keys()` function is already async and can be called from a timer handler
   - No TODO comments related to key rotation were found in the codebase

5. **GroupActor struct fields** (`group_actor.rs:31-70`):
   - No `last_epoch_advance` field exists (would need to be added)
   - No key rotation configuration field exists (would need to be added, possibly to `GroupContextExt` or as a separate config)
   - The struct already tracks `compaction_config` and `compaction_state`, suggesting a similar pattern could be used for key rotation config

6. **Configuration location**:
   - `GroupContextExt` (`sync-core/src/extension.rs:39-56`) currently stores `crdt_type_id` and `compaction_config`
   - Could potentially add key rotation threshold here, or create a separate extension/config mechanism

## Implementation Plan

### Step 1: Configuration Storage

**Decision**: Store the key rotation interval in `GroupContextExt` alongside `compaction_config`.

**Rationale**: 
- `GroupContextExt` already stores group-level configuration (`compaction_config`)
- It's serialized into the MLS group context, ensuring all members share the same configuration
- Follows the existing pattern used for compaction configuration
- No need for a separate extension or runtime configuration mechanism

**Files to modify**:
- `sync-core/src/extension.rs`

**Changes**:
1. Add field to `GroupContextExt` struct:
   ```rust
   pub struct GroupContextExt {
       pub crdt_type_id: String,
       pub compaction_config: CompactionConfig,
       pub key_rotation_interval_secs: Option<u64>,  // None = disabled, Some(duration) = enabled
   }
   ```

2. Update `GroupContextExt::new()` constructor:
   ```rust
   pub fn new(
       crdt_type_id: impl Into<String>,
       compaction_config: CompactionConfig,
       key_rotation_interval_secs: Option<u64>,
   ) -> Self
   ```

3. Update default handling in `sync-proposer/src/group/mod.rs`:
   - In `Group::create()` (line ~256): Pass `Some(DEFAULT_KEY_ROTATION_INTERVAL_SECS)` or `None`
   - In `Group::join()` (line ~362): Extract from `group_ctx` with fallback to `None` or default

**Default value**: `Some(86400)` (24 hours in seconds). Use `None` to disable time-based rotation.

**Constants to add**:
- `sync-proposer/src/group/group_actor.rs`: `const DEFAULT_KEY_ROTATION_INTERVAL_SECS: u64 = 86400;`
- `sync-proposer/src/group/group_actor.rs`: `const KEY_ROTATION_CHECK_INTERVAL_SECS: u64 = 3600;` (check every hour)

---

### Step 2: Add `last_epoch_advance` Field to GroupActor

**Files to modify**:
- `sync-proposer/src/group/group_actor.rs`

**Changes**:
1. Add field to `GroupActor` struct (after line 70):
   ```rust
   pub(crate) struct GroupActor<C, CS> {
       // ... existing fields ...
       last_epoch_advance: std::time::Instant,
       key_rotation_interval_secs: Option<u64>,
   }
   ```

2. Initialize in `GroupActor::new()` (around line 268):
   ```rust
   Self {
       // ... existing fields ...
       last_epoch_advance: std::time::Instant::now(),
       key_rotation_interval_secs: None,  // Will be set from GroupContextExt
   }
   ```

3. Update constructor signature to accept `key_rotation_interval_secs`:
   ```rust
   pub(crate) fn new(
       // ... existing parameters ...
       key_rotation_interval_secs: Option<u64>,
   ) -> Self
   ```

4. Update call site in `sync-proposer/src/group/mod.rs::spawn_actors()` (line ~421):
   - Extract `key_rotation_interval_secs` from `GroupContextExt` when creating the actor
   - Pass it to `GroupActor::new()`

**Note**: Initialize `last_epoch_advance` to `Instant::now()` so the timer starts from group creation/join time.

---

### Step 3: Add Key Rotation Timer to Event Loop

**Files to modify**:
- `sync-proposer/src/group/group_actor.rs`

**Changes**:
1. In `GroupActor::run()` method (around line 277), add timer initialization:
   ```rust
   let mut key_rotation_check = if self.key_rotation_interval_secs.is_some() {
       let mut interval = tokio::time::interval(std::time::Duration::from_secs(
           KEY_ROTATION_CHECK_INTERVAL_SECS,
       ));
       interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
       Some(interval)
   } else {
       None
   };
   ```

2. Add branch to `tokio::select!` in the event loop (after line 307):
   ```rust
   tokio::select! {
       biased;
       
       // ... existing branches ...
       
       _ = compaction_check.tick() => {
           self.maybe_trigger_compaction().await;
       }
       
       _ = key_rotation_check.as_mut().unwrap().tick(), if key_rotation_check.is_some() => {
           self.maybe_trigger_key_rotation().await;
       }
   }
   ```

**Note**: The `if key_rotation_check.is_some()` guard ensures this branch only runs when rotation is enabled.

---

### Step 4: Implement `maybe_trigger_key_rotation()` Method

**Files to modify**:
- `sync-proposer/src/group/group_actor.rs`

**Changes**:
Add new method after `maybe_trigger_compaction()` (around line 1541):

```rust
/// Check whether key rotation should be triggered based on time since last epoch advance.
/// If the configured interval has elapsed, call `handle_update_keys()` to propose rotation.
async fn maybe_trigger_key_rotation(&mut self) {
    // Don't trigger if rotation is disabled
    let Some(interval_secs) = self.key_rotation_interval_secs else {
        return;
    };
    
    // Don't trigger if we already have an active proposal
    if self.active_proposal.is_some() {
        return;
    }
    
    // Don't trigger if no acceptors (no point rotating if nowhere to store)
    if self.learner.acceptor_ids().len() == 0 {
        return;
    }
    
    let elapsed = self.last_epoch_advance.elapsed();
    let threshold = std::time::Duration::from_secs(interval_secs);
    
    if elapsed >= threshold {
        tracing::info!(
            elapsed_secs = elapsed.as_secs(),
            threshold_secs = interval_secs,
            "triggering time-based key rotation"
        );
        
        // Use a oneshot channel but don't block waiting for the result
        // The rotation will proceed asynchronously via Paxos
        let (reply_tx, reply_rx) = oneshot::channel();
        self.handle_update_keys(reply_tx).await;
        
        // Spawn a background task to log the result
        tokio::spawn(async move {
            match reply_rx.await {
                Ok(Ok(())) => tracing::info!("time-based key rotation commit accepted"),
                Ok(Err(e)) => tracing::warn!(?e, "time-based key rotation commit failed"),
                Err(_) => tracing::debug!("time-based key rotation reply dropped"),
            }
        });
    }
}
```

**Design decisions**:
- Non-blocking: Uses `oneshot::channel()` but doesn't await the result, similar to `submit_compaction_complete()`
- Logging: Spawns a background task to log success/failure
- Early returns: Checks for active proposals and acceptors before proceeding

---

### Step 5: Reset Timer on Epoch Advance

**Files to modify**:
- `sync-proposer/src/group/group_actor.rs`

**Changes**:
In `apply_proposal()` method, after emitting `EpochAdvanced` event (around line 1208):

```rust
let _ = self.event_tx.send(GroupEvent::EpochAdvanced {
    epoch: self.learner.mls_epoch().0,
});

// Reset key rotation timer on epoch advance
self.last_epoch_advance = std::time::Instant::now();
```

**Rationale**: 
- Any epoch advance (from any source) resets the timer
- This includes manual `UpdateKeys`, member add/remove, compaction, etc.
- Ensures the timer only tracks time since the last actual rotation

---

### Step 6: Handle Contention

**Files to modify**:
- `sync-proposer/src/group/group_actor.rs`

**Contention handling strategy**:

1. **Multiple devices trigger simultaneously**: 
   - Each device independently checks the timer and calls `handle_update_keys()`
   - Only one proposal wins via Paxos consensus (determined by attempt number and quorum)
   - When `apply_proposal()` processes the winning commit, it emits `EpochAdvanced` and resets `last_epoch_advance`
   - Other devices observe the epoch advance via `apply_proposal()` (even if their proposal lost), resetting their timers
   - No explicit coordination needed — Paxos handles it

2. **Our proposal loses**:
   - In `handle_proposal_response()` (line ~1025), when `is_ours` is false and we have an active proposal, we call `complete_proposal_error()` (line ~1054)
   - However, `apply_proposal()` is still called with the winning proposal (line ~1046)
   - The epoch advance from the winning proposal resets our timer
   - No additional handling needed

3. **Our proposal wins**:
   - Normal success path: `apply_proposal()` resets the timer
   - No additional handling needed

**No code changes required** — the existing Paxos mechanism handles contention automatically.

---

### Step 7: Edge Cases

#### Edge Case 1: Device is Offline

**Scenario**: Device goes offline, timer fires while offline, comes back online after interval has elapsed.

**Handling**:
- When the device comes back online, it will receive missed proposals via acceptor sync
- If another device already rotated keys, `apply_proposal()` will process the epoch advance and reset the timer
- If no rotation occurred, the next timer tick will trigger rotation
- No special handling needed — the timer continues running based on local `Instant`, and epoch advances reset it

**Code changes**: None required.

#### Edge Case 2: Proposal Already In Flight

**Scenario**: Timer fires while `active_proposal` is set (e.g., from a manual `UpdateKeys` call).

**Handling**:
- `maybe_trigger_key_rotation()` checks `if self.active_proposal.is_some()` and returns early
- The timer will fire again on the next interval
- If the in-flight proposal succeeds, the epoch advance resets the timer
- If it fails, the next timer tick will retry

**Code changes**: Already handled in Step 4.

#### Edge Case 3: Configuration Changes

**Scenario**: Group configuration changes (e.g., via a future admin API) to enable/disable or change interval.

**Handling**:
- `GroupContextExt` is immutable once set (stored in MLS group context)
- To change configuration, the group would need to be recreated or use a custom proposal mechanism
- For now, assume configuration is set at group creation and doesn't change
- Future enhancement: Add a `SyncProposal::ConfigUpdate` variant if needed

**Code changes**: None required for initial implementation.

#### Edge Case 4: Timer Fires During Compaction

**Scenario**: Timer fires while compaction is in progress (which also advances epochs).

**Handling**:
- Compaction doesn't set `active_proposal` — it submits proposals asynchronously (see `submit_compaction_complete()`)
- Timer check will proceed and may trigger `UpdateKeys`
- If compaction completes first, its epoch advance resets the timer
- If `UpdateKeys` completes first, its epoch advance resets the timer
- Both operations are safe — they're just empty commits or custom proposals
- Worst case: two epoch advances in quick succession (harmless, just extra rotation)

**Code changes**: None required — acceptable behavior.

#### Edge Case 5: Group Has No Acceptors

**Scenario**: Timer fires but group has no acceptors (edge case during group creation).

**Handling**:
- `maybe_trigger_key_rotation()` checks `if self.learner.acceptor_ids().len() == 0` and returns early
- Timer will continue firing, but rotation won't trigger until acceptors are added
- Once acceptors are added, the next timer tick will trigger rotation if needed

**Code changes**: Already handled in Step 4.

---

### Step 8: Update Group Creation and Join

**Files to modify**:
- `sync-proposer/src/group/mod.rs`

**Changes**:

1. In `Group::create()` (around line 256):
   ```rust
   let group_context_extensions = mls_rs::ExtensionList::default();
   group_context_extensions
       .set_from(GroupContextExt::new(
           &crdt_type_id,
           compaction_config.clone(),
           Some(DEFAULT_KEY_ROTATION_INTERVAL_SECS),  // Enable with default
       ))
       .change_context(GroupError)
       .attach(OperationContext::CREATING_GROUP)?;
   ```

2. In `Group::join()` (around line 362):
   ```rust
   let key_rotation_interval_secs = group_ctx
       .as_ref()
       .and_then(|e| e.key_rotation_interval_secs)
       .or(Some(DEFAULT_KEY_ROTATION_INTERVAL_SECS));  // Fallback to default
   ```

3. In `Group::spawn_actors()` (around line 407):
   - Extract `key_rotation_interval_secs` from `GroupContextExt` (for join) or use default (for create)
   - Pass to `GroupActor::new()`:
   ```rust
   let actor = group_actor::GroupActor::new(
       learner,
       group_id,
       endpoint,
       connection_manager,
       request_rx,
       app_message_tx,
       event_tx.clone(),
       cancel_token.clone(),
       compaction_config,
       crdt_factory,
       key_rotation_interval_secs,  // Add this parameter
   );
   ```

**Constants to add**:
- `sync-proposer/src/group/mod.rs`: `const DEFAULT_KEY_ROTATION_INTERVAL_SECS: u64 = 86400;` (24 hours)

---

### Step 9: Testing Strategy

#### Unit Tests

**Files to create/modify**:
- `sync-proposer/src/group/group_actor.rs` (add to existing `#[cfg(test)]` module)

**Test cases**:

1. **Timer triggers rotation after interval**:
   ```rust
   #[tokio::test]
   async fn test_key_rotation_timer_triggers() {
       // Create GroupActor with 1-second rotation interval
       // Advance time using tokio::time::pause()
       // Verify handle_update_keys() is called
   }
   ```

2. **Timer resets on epoch advance**:
   ```rust
   #[tokio::test]
   async fn test_key_rotation_timer_resets_on_epoch_advance() {
       // Create GroupActor, trigger epoch advance via apply_proposal()
       // Verify last_epoch_advance is updated
       // Verify timer doesn't fire immediately after reset
   }
   ```

3. **Timer doesn't trigger with active proposal**:
   ```rust
   #[tokio::test]
   async fn test_key_rotation_skips_with_active_proposal() {
       // Create GroupActor, set active_proposal
       // Advance time past interval
       // Verify handle_update_keys() is NOT called
   }
   ```

4. **Timer doesn't trigger with no acceptors**:
   ```rust
   #[tokio::test]
   async fn test_key_rotation_skips_with_no_acceptors() {
       // Create GroupActor with no acceptors
       // Advance time past interval
       // Verify handle_update_keys() is NOT called
   }
   ```

5. **Timer disabled when interval is None**:
   ```rust
   #[tokio::test]
   async fn test_key_rotation_disabled() {
       // Create GroupActor with key_rotation_interval_secs = None
       // Verify timer branch doesn't exist in select!
   }
   ```

#### Integration Tests

**Files to create**:
- `sync-proposer/tests/key_rotation_integration.rs` (or add to existing integration test file)

**Test cases**:

1. **Multiple devices trigger rotation simultaneously**:
   - Create 3 devices in a group
   - Advance time past rotation interval on all devices
   - Verify only one rotation commit succeeds (via epoch numbers)
   - Verify all devices observe the epoch advance and reset timers

2. **Rotation after manual UpdateKeys**:
   - Create group, manually call `update_keys()`
   - Advance time past rotation interval
   - Verify timer doesn't fire (already rotated recently)

3. **Rotation after compaction**:
   - Create group, trigger compaction
   - Advance time past rotation interval
   - Verify timer doesn't fire immediately (compaction advanced epoch)

4. **Rotation in low-activity group**:
   - Create group, no activity for 25 hours
   - Verify rotation triggers automatically
   - Verify epoch advances

#### Manual Testing Checklist

1. Create a group with default rotation interval (24h)
2. Wait 24+ hours (or use time manipulation), verify rotation occurs
3. Manually call `update_keys()`, verify timer resets
4. Create a group with `key_rotation_interval_secs = None`, verify no rotation
5. Create a group with custom interval (e.g., 1 hour), verify rotation at correct time
6. Test with multiple devices: verify contention handling works correctly

---

### Step 10: Documentation Updates

**Files to modify**:
- Update any API documentation for `Group::create()` and `Group::join()` if they exist
- Add comments to `GroupContextExt` explaining the new field
- Add doc comment to `maybe_trigger_key_rotation()` explaining the behavior

**Changes**:
1. Add doc comment to `GroupContextExt::key_rotation_interval_secs`:
   ```rust
   /// Time-based key rotation interval in seconds. If `None`, time-based rotation is disabled.
   /// When set, the group will automatically propose key rotation if no epoch advance
   /// has occurred within this duration. Default: 24 hours (86400 seconds).
   pub key_rotation_interval_secs: Option<u64>,
   ```

2. Add doc comment to `maybe_trigger_key_rotation()`:
   ```rust
   /// Check whether key rotation should be triggered based on time since last epoch advance.
   /// 
   /// This method is called periodically by the key rotation timer. It checks if the
   /// configured interval has elapsed since the last epoch advance, and if so, proposes
   /// an empty commit via `handle_update_keys()` to trigger key rotation.
   /// 
   /// Contention is handled automatically by Paxos: if multiple devices trigger rotation
   /// simultaneously, only one proposal wins, and all devices observe the epoch advance
   /// and reset their timers.
   ```

---

### Summary of File Changes

1. **sync-core/src/extension.rs**:
   - Add `key_rotation_interval_secs: Option<u64>` to `GroupContextExt`
   - Update `GroupContextExt::new()` signature

2. **sync-proposer/src/group/group_actor.rs**:
   - Add `last_epoch_advance: Instant` and `key_rotation_interval_secs: Option<u64>` fields
   - Update `GroupActor::new()` signature
   - Add timer initialization in `run()`
   - Add timer branch to `tokio::select!`
   - Implement `maybe_trigger_key_rotation()` method
   - Reset `last_epoch_advance` in `apply_proposal()` after epoch advance
   - Add constants: `DEFAULT_KEY_ROTATION_INTERVAL_SECS`, `KEY_ROTATION_CHECK_INTERVAL_SECS`
   - Add unit tests

3. **sync-proposer/src/group/mod.rs**:
   - Update `Group::create()` to pass rotation interval to `GroupContextExt::new()`
   - Update `Group::join()` to extract rotation interval from `GroupContextExt`
   - Update `Group::spawn_actors()` to pass rotation interval to `GroupActor::new()`
   - Add constant: `DEFAULT_KEY_ROTATION_INTERVAL_SECS`

4. **sync-proposer/tests/** (new or existing):
   - Add integration tests for key rotation

---

### Implementation Order

1. Step 1: Configuration storage (extension.rs)
2. Step 2: Add fields to GroupActor (group_actor.rs)
3. Step 8: Update Group creation/join (mod.rs)
4. Step 3: Add timer to event loop (group_actor.rs)
5. Step 4: Implement `maybe_trigger_key_rotation()` (group_actor.rs)
6. Step 5: Reset timer on epoch advance (group_actor.rs)
7. Step 9: Add tests (group_actor.rs, tests/)
8. Step 10: Documentation updates

Steps 6 (contention) and 7 (edge cases) require no code changes — they're handled by existing mechanisms.
