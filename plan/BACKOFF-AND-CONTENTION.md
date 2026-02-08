# Backoff and Contention

**Status**: Implemented

## Problem

The retry/backoff strategy for Paxos proposal contention was linear with no
jitter and a low retry limit.

### Previous Behaviour

```
sleep(10ms * (retries + 1))
```

- Backoff: 10ms, 20ms, 30ms (linear)
- Max retries: 3 (`MAX_PROPOSAL_RETRIES`)
- No jitter

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

## Current Design

Location: `filament-weave/src/group/group_actor.rs`

### Backoff Function

```rust
fn exponential_backoff_duration(retries: u32) -> std::time::Duration {
    const BASE_MS: u64 = 10;
    const MAX_EXPONENT: u32 = 6;
    const MAX_DELAY_MS: u64 = 1000;
    let delay_ms = BASE_MS.saturating_mul(1u64 << retries.min(MAX_EXPONENT));
    let jitter = rand::random::<u64>() % (delay_ms / 2 + 1);
    std::time::Duration::from_millis((delay_ms + jitter).min(MAX_DELAY_MS))
}
```

### Parameters

- `MAX_PROPOSAL_RETRIES = 6` (7 total attempts including initial)
- Exponential growth: `10ms * 2^min(retries, 6)`
- Jitter: uniform random in `[0, delay/2]`
- Hard cap: 1000ms

### Backoff Sequence (with jitter ranges)

| Retry | Base delay | Jitter range | Total range |
|-------|-----------|-------------|-------------|
| 0     | 10ms      | 0–5ms       | 10–15ms     |
| 1     | 20ms      | 0–10ms      | 20–30ms     |
| 2     | 40ms      | 0–20ms      | 40–60ms     |
| 3     | 80ms      | 0–40ms      | 80–120ms    |
| 4     | 160ms     | 0–80ms      | 160–240ms   |
| 5     | 320ms     | 0–160ms     | 320–480ms   |
| 6+    | 640ms     | 0–320ms     | 640–1000ms  |

Total backoff time: ~1.2 seconds for 6 retries.

### Callsites

All four retry paths use `exponential_backoff_duration(retries)`:

1. `retry_proposal_simple` — simple proposals (remove member, update keys,
   add/remove acceptor)
2. `retry_proposal_with_welcome` — add member proposals
3. `handle_proposal_response` — rejection during accept phase
4. `retry_active_proposal` — recursive rejection path

### Consistency

The previous code had an inconsistency where `handle_proposal_response`
used `10 * retries` instead of `10 * (retries + 1)`. All paths now use
the same `exponential_backoff_duration` function.

### Contention Properties

- Jitter decorrelates competing devices so they don't retry at the same
  instant.
- 7 total attempts (vs previous 4) handle contention from 5–10
  simultaneous devices.
- Network latency and proposal processing provide additional natural
  decorrelation.
- No higher-level retry logic exists — the retry mechanism is entirely
  contained within `GroupActor`.

### Proposal Timeout

Separate from retry backoff: `PROPOSAL_TIMEOUT = 30 seconds`. Proposals
can timeout even if not rejected.
