# Backoff and Contention

## Problem

The retry/backoff strategy for Paxos proposal contention is linear with no
jitter and a low retry limit.

### Current Behaviour

Location: `sync-proposer/src/group/group_actor.rs`

```
sleep(10ms * (retries + 1))
```

- Backoff: 10ms, 20ms, 30ms (linear)
- Max retries: 3 (`MAX_PROPOSAL_RETRIES`)
- No jitter

The code implements linear backoff with no jitter. (Note: The README does not
actually claim exponential backoff with jitter — this was an incorrect claim
in the original document.)

### Consequences

1. **Correlated retries**: Without jitter, two devices that collide on
   attempt 0 will retry at nearly the same time (10ms later), collide
   again, retry at 20ms, collide again, and exhaust retries.

2. **Low retry budget**: 3 retries may not be enough under contention.
   With N devices all trying to commit simultaneously (e.g., after a
   network partition heals), the probability of success within 3 attempts
   decreases with N.

3. **Fast failure**: After 3 retries (total ~60ms of backoff), the
   proposal fails with "max proposal retries exceeded". The caller must
   handle this. In practice, this surfaces as a user-visible error.

### When This Matters

- Rare in normal operation (commits are infrequent — add/remove members,
  key rotation, compaction).
- More likely after network partition recovery, where multiple devices
  may have queued commits.
- More likely with many group members who independently decide to rotate
  keys or trigger compaction simultaneously.

## Proposed Fix

Replace linear backoff with exponential backoff and jitter:

```rust
let base = 10u64;
let max_delay = 1000u64; // 1 second cap
let delay = base.saturating_mul(1u64 << retries.min(6));
let jitter = rand::random::<u64>() % (delay / 2 + 1);
sleep(Duration::from_millis(delay + jitter).min(Duration::from_millis(max_delay)))
```

Consider increasing `MAX_PROPOSAL_RETRIES` to 5-7 to handle higher
contention scenarios.

Update the attempt increment to use the superseding proposal's attempt
as the floor (this is already done — `next_attempt` uses the superseding
attempt).

## Code Verification

### Verified Claims

1. **MAX_PROPOSAL_RETRIES = 3** ✓
   - Location: `sync-proposer/src/group/group_actor.rs:208`
   - Value: `const MAX_PROPOSAL_RETRIES: u32 = 3;`

2. **Linear backoff formula** ✓ (mostly accurate)
   - Most retry paths use: `10 * (retries + 1)` milliseconds
   - Locations:
     - `retry_proposal_simple`: line 827-829
     - `retry_proposal_with_welcome`: line 882-884
     - `retry_active_proposal` (recursive): line 1133-1135
   - Backoff sequence: 10ms, 20ms, 30ms (for retries 0, 1, 2)

3. **No jitter** ✓
   - Confirmed: all backoff delays are deterministic with no random component

4. **Total backoff time ~60ms** ✓
   - Sum: 10ms + 20ms + 30ms = 60ms (for 3 retries)

5. **Error message** ✓
   - Location: `sync-proposer/src/group/group_actor.rs:796, 846, 1087, 1131`
   - Message: `"max proposal retries exceeded"`

6. **next_attempt implementation** ✓
   - Location: `sync-core/src/proposal.rs:135-137`
   - Implementation: `Attempt(attempt.0 + 1)` — correctly increments by 1
   - Usage: All rejection handlers correctly call `GroupProposal::next_attempt(superseded_by.attempt())`
   - Locations: lines 734, 778, 825, 880, 1080, 1127

### Inconsistencies Found

1. **Different backoff formula in one retry path** ⚠️
   - Location: `sync-proposer/src/group/group_actor.rs:1090-1092`
   - In `handle_proposal_response` → `retry_active_proposal` path:
     - Uses: `10 * retries` (not `10 * (retries + 1)`)
     - This means: retries=1 → 10ms, retries=2 → 20ms, retries=3 → 30ms
   - This is inconsistent with other retry paths and should be fixed to match

2. **Retry counting semantics** ⚠️
   - There are actually **4 total attempts** (initial + 3 retries), not 3 retries
   - The initial attempt starts with `retries=0` in `start_proposal` paths
   - The `retry_active_proposal` path starts with `retries=1` (after increment)
   - All paths allow retries up to `MAX_PROPOSAL_RETRIES` (3), meaning:
     - Initial attempt (retries=0)
     - Retry 1 (retries=1)
     - Retry 2 (retries=2)
     - Retry 3 (retries=3)
     - Then failure

### Additional Details

1. **Multiple retry paths**:
   - `retry_proposal_simple`: Used for simple proposals (remove member, update keys, add/remove acceptor)
   - `retry_proposal_with_welcome`: Used for add member proposals (needs welcome message handling)
   - `retry_active_proposal`: Used when an active proposal is rejected during the accept phase (after receiving acceptor responses)

2. **Retry trigger points**:
   - Immediate rejection in `propose()`: Lines 733-738, 777-784
   - Rejection during prepare phase: Lines 824-832, 879-893
   - Rejection during accept phase: Lines 1079-1096, 1126-1138

3. **Proposal timeout**:
   - Location: `sync-proposer/src/group/group_actor.rs:209`
   - Value: `PROPOSAL_TIMEOUT = 30 seconds`
   - Checked in `check_proposal_timeout()`: line 318-331
   - This is separate from retry backoff — proposals can timeout even if not rejected

4. **No higher-level retry logic**:
   - Callers of proposal functions do not implement additional retry logic
   - The retry mechanism is entirely contained within `GroupActor`

5. **Natural decorrelation**:
   - Network latency and proposal processing time provide some natural decorrelation
   - However, without jitter, synchronized devices will still retry at nearly identical times

### Recommendations

1. **Fix inconsistency**: Change line 1091 to use `10 * (retries + 1)` to match other paths
2. **Clarify retry semantics**: Document that MAX_PROPOSAL_RETRIES=3 means 4 total attempts
3. **Verify README claim**: The original document incorrectly claimed the README describes exponential backoff — this is not found in the README

## Implementation Plan

### Step 1: Create Exponential Backoff Utility Function

**File**: `sync-proposer/src/group/group_actor.rs`

**What to add**: Add a new module-level function `exponential_backoff_duration()` that calculates exponential backoff with jitter.

**Function signature**:
```rust
/// Calculate exponential backoff duration with jitter for proposal retries.
///
/// Formula: base * 2^min(retries, max_exponent) + jitter
/// - base: 10ms
/// - max_exponent: 6 (caps exponential growth at 2^6 = 64x base = 640ms)
/// - jitter: random value in [0, delay/2] to decorrelate retries
/// - max_delay: 1000ms (1 second cap)
///
/// Returns a Duration suitable for tokio::time::sleep().
fn exponential_backoff_duration(retries: u32) -> std::time::Duration {
    const BASE_MS: u64 = 10;
    const MAX_EXPONENT: u32 = 6;
    const MAX_DELAY_MS: u64 = 1000;
    
    let exponent = retries.min(MAX_EXPONENT);
    let delay_ms = BASE_MS.saturating_mul(1u64 << exponent);
    
    // Add jitter: random value in [0, delay_ms/2]
    // This decorrelates retries from competing devices
    let jitter_max = delay_ms / 2 + 1;
    let jitter = rand::random::<u64>() % jitter_max;
    
    let total_ms = (delay_ms + jitter).min(MAX_DELAY_MS);
    std::time::Duration::from_millis(total_ms)
}
```

**Placement**: Add this function before the `impl<C, CS> GroupActor<C, CS>` block (around line 217), or as a private method within the impl block. Module-level function is preferred for testability.

**Dependencies**: `rand` is already available in `sync-proposer/Cargo.toml` (line 30: `rand.workspace = true`), so no Cargo.toml changes needed.

### Step 2: Replace Linear Backoff in `retry_proposal_simple`

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Lines 827-829

**Current code**:
```rust
tokio::time::sleep(std::time::Duration::from_millis(
    10 * (u64::from(retries) + 1),
))
.await;
```

**Change to**:
```rust
tokio::time::sleep(exponential_backoff_duration(retries)).await;
```

**Context**: This is in the `ProposeResult::Rejected` branch of `retry_proposal_simple()`, called when a proposal is rejected during the prepare phase.

### Step 3: Replace Linear Backoff in `retry_proposal_with_welcome`

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Lines 882-884

**Current code**:
```rust
tokio::time::sleep(std::time::Duration::from_millis(
    10 * (u64::from(retries) + 1),
))
.await;
```

**Change to**:
```rust
tokio::time::sleep(exponential_backoff_duration(retries)).await;
```

**Context**: This is in the `ProposeResult::Rejected` branch of `retry_proposal_with_welcome()`, called when an add-member proposal is rejected during the prepare phase.

### Step 4: Fix Inconsistent Backoff in `handle_proposal_response` → `retry_active_proposal`

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Lines 1090-1092

**Current code** (inconsistent - uses `10 * retries` instead of `10 * (retries + 1)`):
```rust
tokio::time::sleep(std::time::Duration::from_millis(
    10 * u64::from(retries),
))
.await;
```

**Change to**:
```rust
tokio::time::sleep(exponential_backoff_duration(retries)).await;
```

**Context**: This is in the `ProposeResult::Rejected` branch of `handle_proposal_response()`, called when an active proposal is rejected during the accept phase. Note: `retries` here is `active.retries + 1` (line 1084), so it's already the correct retry count for the exponential backoff function.

### Step 5: Replace Linear Backoff in `retry_active_proposal` (recursive path)

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Lines 1133-1135

**Current code**:
```rust
tokio::time::sleep(std::time::Duration::from_millis(
    10 * u64::from(retries + 1),
))
.await;
```

**Change to**:
```rust
tokio::time::sleep(exponential_backoff_duration(retries)).await;
```

**Context**: This is in the recursive `ProposeResult::Rejected` branch of `retry_active_proposal()`. Note: The exponential backoff function expects the current retry count (not `retries + 1`), so we pass `retries` directly.

### Step 6: Increase MAX_PROPOSAL_RETRIES

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Line 208

**Current code**:
```rust
const MAX_PROPOSAL_RETRIES: u32 = 3;
```

**Change to**:
```rust
const MAX_PROPOSAL_RETRIES: u32 = 6;
```

**Rationale**: 
- With exponential backoff, retries are more spaced out (10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms+)
- Higher retry count improves success probability under contention
- Total backoff time with 6 retries: ~1.2 seconds (capped at 1s max delay), which is acceptable for rare contention scenarios
- This gives 7 total attempts (initial + 6 retries), which should handle contention from 5-10 simultaneous devices

**Note**: All retry count checks (`retries >= MAX_PROPOSAL_RETRIES` and `retries > MAX_PROPOSAL_RETRIES`) will automatically use the new value. The checks are at:
- Line 794: `retry_proposal_simple`
- Line 844: `retry_proposal_with_welcome`
- Line 1085: `handle_proposal_response` → `retry_active_proposal`
- Line 1130: `retry_active_proposal` (recursive)

### Step 7: Add Unit Test for Exponential Backoff Function

**File**: `sync-proposer/src/group/group_actor.rs`

**Location**: Add to the existing `#[cfg(test)]` module at the end of the file (after line 1965)

**Test to add**:
```rust
#[test]
fn test_exponential_backoff_duration() {
    // Test that backoff increases exponentially
    let d0 = exponential_backoff_duration(0);
    let d1 = exponential_backoff_duration(1);
    let d2 = exponential_backoff_duration(2);
    let d3 = exponential_backoff_duration(3);
    let d6 = exponential_backoff_duration(6);
    let d7 = exponential_backoff_duration(7); // Should cap at same as d6
    
    // Base delay: 10ms * 2^0 = 10ms (plus jitter)
    assert!(d0.as_millis() >= 10);
    assert!(d0.as_millis() <= 15); // 10ms + jitter (max 5ms)
    
    // Retry 1: 10ms * 2^1 = 20ms (plus jitter)
    assert!(d1.as_millis() >= 20);
    assert!(d1.as_millis() <= 30); // 20ms + jitter (max 10ms)
    
    // Retry 2: 10ms * 2^2 = 40ms (plus jitter)
    assert!(d2.as_millis() >= 40);
    assert!(d2.as_millis() <= 60); // 40ms + jitter (max 20ms)
    
    // Retry 3: 10ms * 2^3 = 80ms (plus jitter)
    assert!(d3.as_millis() >= 80);
    assert!(d3.as_millis() <= 120); // 80ms + jitter (max 40ms)
    
    // Retry 6: 10ms * 2^6 = 640ms (plus jitter, but capped at 1000ms)
    assert!(d6.as_millis() >= 640);
    assert!(d6.as_millis() <= 1000); // Capped at max_delay
    
    // Retry 7: Should be same as retry 6 (capped exponent)
    assert!(d7.as_millis() >= 640);
    assert!(d7.as_millis() <= 1000);
    
    // Verify exponential growth (allowing for jitter)
    assert!(d1.as_millis() > d0.as_millis());
    assert!(d2.as_millis() > d1.as_millis());
    assert!(d3.as_millis() > d2.as_millis());
}
```

**Note**: This test may be flaky due to randomness in jitter. Consider running it multiple times or using a seeded RNG for deterministic testing. Alternatively, test the deterministic parts separately and verify jitter is in the expected range.

### Step 8: Integration Test Under Contention

**File**: Create new test file `sync-proposer/src/group/group_actor_contention_test.rs` or add to existing test module

**Test strategy**: 
1. Create a test scenario with multiple `GroupActor` instances (simulating multiple devices)
2. Have all actors attempt to propose simultaneously (e.g., all call `handle_update_keys()` at the same time)
3. Verify that:
   - All proposals eventually succeed (within MAX_PROPOSAL_RETRIES)
   - Retries are decorrelated (timing should show jitter)
   - No "max proposal retries exceeded" errors occur under normal contention

**Test outline**:
```rust
#[tokio::test]
async fn test_proposal_contention_with_backoff() {
    // Setup: Create N GroupActor instances (e.g., 5 devices)
    // All attempt to propose simultaneously
    // Verify all succeed within retry budget
    // Check that retry timings show decorrelation (not all at same time)
}
```

**Alternative**: If integration test is complex, focus on unit test for the backoff function and verify manually that contention scenarios work in practice.

### Summary of Changes

**Files to modify**:
1. `sync-proposer/src/group/group_actor.rs`
   - Add `exponential_backoff_duration()` function
   - Replace 4 linear backoff callsites with exponential backoff
   - Increase `MAX_PROPOSAL_RETRIES` from 3 to 6
   - Add unit test for backoff function

**Dependencies**: No changes needed — `rand` is already available in `sync-proposer/Cargo.toml`

**Function signatures**:
- `fn exponential_backoff_duration(retries: u32) -> std::time::Duration`

**Expected backoff sequence** (with jitter):
- Retry 0: ~10-15ms (10ms base + 0-5ms jitter)
- Retry 1: ~20-30ms (20ms base + 0-10ms jitter)
- Retry 2: ~40-60ms (40ms base + 0-20ms jitter)
- Retry 3: ~80-120ms (80ms base + 0-40ms jitter)
- Retry 4: ~160-240ms (160ms base + 0-80ms jitter)
- Retry 5: ~320-480ms (320ms base + 0-160ms jitter)
- Retry 6+: ~640-1000ms (640ms base + 0-320ms jitter, capped at 1000ms)

**Total backoff time**: ~1.2 seconds for 6 retries (vs ~60ms with linear backoff)

**Testing**:
- Unit test for `exponential_backoff_duration()` function
- Integration test for contention scenarios (optional but recommended)
