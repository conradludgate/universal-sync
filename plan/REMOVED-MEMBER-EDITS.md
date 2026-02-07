# Removed Member Edits — Silent Data Loss

## Problem

When a member is removed from the group via Paxos commit, their CRDT updates
that were sent before removal are silently dropped by other devices.

### Current Behaviour

In `group_actor.rs:1440-1456`, `handle_decrypted_message()` looks up the
sender in the **current** MLS roster. If the sender is not found (because
they've been removed), the message is dropped with a warning log:

```
"sender not found in roster, dropping message"
```

### Scenario

1. Device A sends CRDT updates at epoch 4 (encrypted with epoch 4 keys).
2. Device B proposes removing Device A. This wins Paxos consensus.
3. The group advances to epoch 5. Device A is no longer in the roster.
4. Device C comes online, backfills from acceptors, receives Device A's
   epoch 4 messages.
5. Device C checks the roster (now epoch 5) — Device A is not found.
6. Device A's legitimate pre-removal edits are silently dropped.

### Impact

- Data loss: edits made by a member before removal are discarded.
- The loss is silent — only a `warn`-level log is emitted (not `debug`).
- This affects any device that processes the removed member's messages
  after the removal commit.

## Design Decision Required

This behaviour could be intentional or a bug. Two valid interpretations:

### Interpretation 1: Bug — Pre-removal edits should be honoured

Most collaborative editing systems honour edits made before a member was
removed. The member had legitimate access at the time they made the edit.

Fix: look up the sender in the roster **at the epoch the message was
encrypted**, not the current roster. If the sender was a valid member at
that epoch, process the message.

**Note**: `AuthData` does NOT contain epoch information — it only contains
`seq` (and optionally `level`/`watermark` for compaction). The message's
encryption epoch must be determined from the MLS message itself or tracked
separately.

This requires retaining historical roster information or querying MLS for
past-epoch membership.

### Interpretation 2: Intentional — Removal reverts pending edits

In some security models, removing a member should also revoke their
unprocessed contributions. This is a stronger security posture but causes
data loss.

If intentional, this should be documented explicitly and the log level
should be raised to `info` or `warn` to make it visible.

## Recommendation

Honour pre-removal edits. The member had valid group access when they made
the edit. The message is cryptographically valid (signed by the member,
encrypted with valid epoch keys). Dropping it is unexpected behaviour for
a collaborative editing system.

The fix is to check membership at the message's encryption epoch rather
than the current epoch. However, `AuthData` does not contain epoch
information — the epoch must be determined from the MLS message or tracked
separately.

## Code Verification

### Verified Claims

1. **Line numbers**: ✅ Accurate — `group_actor.rs:1440-1456` correctly
   identifies the roster lookup code.

2. **Roster lookup**: ✅ Accurate — The code at lines 1441-1448 does look up
   the sender in the **current** roster via `self.learner.group().roster()`.

3. **Message dropping**: ✅ Accurate — If sender is not found, the message
   is dropped with an early return at line 1455.

4. **Scenario description**: ✅ Accurate — The described scenario matches
   the code behavior.

### Inaccuracies Found

1. **Log level**: ❌ **INCORRECT** — The document claims "only a debug-level
   log is emitted" (line 31), but the actual code uses `tracing::warn!` at
   line 1451. The log level is `warn`, not `debug`.

2. **AuthData epoch information**: ❌ **INCORRECT** — The document claims
   that `AuthData` contains epoch information (lines 45, 68). However,
   examining `sync-core/src/protocol.rs:139-155`, `AuthData` only contains:
   - `seq: u64` (for both `Update` and `Compaction` variants)
   - `level: u8` (only for `Compaction`)
   - `watermark: StateVector` (only for `Compaction`)
   
   There is **no epoch field** in `AuthData`. The epoch must be determined
   from the MLS message itself or tracked separately.

### Additional Findings

1. **MLS decryption succeeds**: ✅ MLS successfully decrypts messages from
   removed members. The `process_incoming_message()` call at line 1155
   succeeds because MLS retains old epoch keys (as noted in
   `DESIGN-REVIEW.md:56-60`). The message is only dropped **after**
   decryption in `handle_decrypted_message()`.

2. **Historical roster data exists**: ✅ Acceptors store epoch rosters in
   the `epoch_rosters` keyspace (`sync-acceptor/src/state_store.rs:66,
   436-467`). The `get_epoch_roster_at_or_before_sync()` method can retrieve
   rosters for past epochs. However:
   - **Proposers don't have access**: `GroupActor` (proposers) don't have
     access to the state store — only acceptors do.
   - **Acceptors already use it**: Acceptors use historical rosters for
     validating proposals from past epochs (`acceptor.rs:100-120`).

3. **sender_index is MLS leaf index**: ✅ `sender_index` (line 1440) is the
   MLS leaf index (`MemberId`), as confirmed by `sync-core/src/proposal.rs:6-8`.
   This is a u32 that identifies the member's position in the MLS tree.

4. **Potential leaf index reuse**: ⚠️ **UNKNOWN** — The document doesn't
   address whether MLS leaf indices can be reused after a member is removed.
   If indices are reused, looking up by `sender_index` alone could match the
   wrong member. However, the code also extracts the signing key fingerprint
   (line 1448), which is stable across epochs and unique per member, so this
   may not be an issue in practice.

5. **Message processing before removal**: ✅ If a device receives and
   processes a removed member's messages **before** applying the removal
   commit, the messages would be processed correctly. The issue only occurs
   when messages are processed **after** the removal commit has been applied.

6. **Acceptor validation**: ✅ Acceptors validate proposals using historical
   rosters (`acceptor.rs:214-226`), but this is for **proposals** (Paxos
   messages), not application messages. Application messages bypass Paxos
   and are only validated by proposers after decryption.

### Implementation Notes

- The fix would require proposers to either:
  1. Access historical roster data (currently only available to acceptors), or
  2. Determine the message's encryption epoch from the MLS message structure, or
  3. Track epoch information alongside messages during storage/retrieval

- The epoch roster infrastructure already exists on acceptors and could
  potentially be exposed to proposers, or proposers could maintain their own
  historical roster cache.

## Implementation Plan

### Overview

The fix requires checking membership at the message's encryption epoch rather than the current epoch. This involves:
1. Determining the encryption epoch of a received message
2. Maintaining historical roster data on proposers
3. Updating `handle_decrypted_message` to use epoch-specific roster lookups
4. Handling edge cases (messages from before join, compaction messages, etc.)

### Step 1: Determine Message Encryption Epoch

**Challenge**: `AuthData` does not contain epoch information, and `ReceivedMessage::ApplicationMessage` may not directly expose the epoch.

**Solution**: Capture the epoch at decryption time. When MLS successfully decrypts a message, it uses epoch-specific keys. We can determine the epoch by:

**Option A (Recommended)**: Extract epoch from the MLS `PrivateMessage` structure before decryption. The MLS `PrivateMessage` wire format includes epoch information in its header. Parse the epoch from the raw ciphertext bytes before calling `process_incoming_message`.

**Option B**: Track epoch transitions. Before calling `process_incoming_message`, record the current epoch. After decryption, if the epoch advanced, the message was from the previous epoch. However, this is fragile if multiple epochs advance.

**Option C**: Use MLS group context. After decryption, check if the group context epoch changed. If it did, the message was from the previous epoch. This requires comparing epochs before/after decryption.

**Recommended Approach**: Use Option A — parse epoch from the MLS `PrivateMessage` wire format. The `mls_rs` library's `MlsMessage` enum contains `PrivateMessage` variants that include epoch information. We need to:
1. In `handle_encrypted_message`, parse the `MlsMessage` to extract epoch before decryption
2. Pass the epoch to `handle_decrypted_message` for roster lookup

**Code Changes**:
- Modify `handle_encrypted_message` to extract epoch from `MlsMessage` before calling `process_incoming_message`
- Add epoch parameter to `handle_decrypted_message`
- Handle cases where epoch extraction fails (fallback to current epoch with warning)

### Step 2: Maintain Historical Roster Data on Proposers

**Current State**: Acceptors store epoch rosters in `epoch_rosters` keyspace (`state_store.rs:436-467`), but proposers don't have access to state stores.

**Solution**: Maintain an in-memory `HashMap<Epoch, EpochRoster>` in `GroupActor`. Since proposers are ephemeral (devices), in-memory storage is sufficient. We only need rosters for epochs we've observed.

**Data Structure**:
```rust
// Add to GroupActor struct
epoch_rosters: HashMap<Epoch, EpochRoster>,
```

**Roster Storage**: Store rosters when epochs advance in `process_commit_effect` (after applying commits). This mirrors acceptor behavior (`acceptor.rs:367-391`).

**Code Changes**:
1. Add `epoch_rosters: HashMap<Epoch, EpochRoster>` field to `GroupActor` struct
2. **Share `EpochRoster` type**: Currently `EpochRoster` is defined in `sync-acceptor/src/state_store.rs` as `pub(crate)`. Options:
   - **Option A (Recommended)**: Move `EpochRoster` to `sync-core/src/protocol.rs` or new `sync-core/src/roster.rs` so both proposers and acceptors can use it
   - **Option B**: Duplicate the type in `sync-proposer` (not ideal, but simpler)
   - **Option C**: Make `EpochRoster` public in acceptor and import it (creates dependency, not ideal)
3. In `process_commit_effect`, after epoch advances, extract current roster and store it:
   ```rust
   let new_epoch = self.learner.mls_epoch();
   let roster = self.build_epoch_roster(new_epoch);
   self.epoch_rosters.insert(new_epoch, roster);
   ```
4. Add helper method `build_epoch_roster(&self, epoch: Epoch) -> EpochRoster` that extracts members and their signing keys from the current MLS roster

**Roster Lookup**: Add method `get_roster_at_epoch(&self, epoch: Epoch) -> Option<&EpochRoster>` that:
- Returns the roster for the exact epoch if stored
- Falls back to the most recent roster at or before the epoch (similar to `get_epoch_roster_at_or_before_sync`)
- Returns `None` if no roster exists (e.g., message from before our join epoch)

### Step 3: Update `handle_decrypted_message` Logic

**Current Logic** (lines 1440-1456):
1. Extract `sender_index` from `app_msg.sender_index`
2. Look up sender in **current** roster by leaf index
3. Extract `MemberFingerprint` from signing key
4. If not found, drop message with warning

**New Logic**:
1. Extract `sender_index` from `app_msg.sender_index`
2. Extract `MemberFingerprint` from the message's authenticated data or roster lookup
3. **Determine message encryption epoch** (from Step 1)
4. **Look up roster for that epoch** (from Step 2)
5. **Check if sender fingerprint exists in epoch roster**
6. If found, process message; if not found, check if it's a pre-join message (edge case)

**Key Change**: Use `MemberFingerprint` for identity, not just `sender_index`, because:
- Fingerprints are stable across epochs
- Leaf indices may be reused after member removal
- Fingerprints are unique per member

**Code Changes**:
```rust
fn handle_decrypted_message(&mut self, received: ReceivedMessage, encryption_epoch: Epoch) {
    // ... existing code to extract app_msg and auth_data ...
    
    let sender_member = MemberId(app_msg.sender_index);
    
    // Try to get fingerprint from current roster first (fast path)
    let sender_fp = self.learner.group()
        .roster()
        .members()
        .iter()
        .find(|m| m.index == sender_member.0)
        .map(|m| MemberFingerprint::from_signing_key(&m.signing_identity.signature_key));
    
    // If not in current roster, check historical roster at encryption epoch
    let sender_fp = match sender_fp {
        Some(fp) => fp,
        None => {
            // Look up in epoch-specific roster
            if let Some(roster) = self.get_roster_at_epoch(encryption_epoch) {
                // Find member by leaf index in that epoch's roster
                if let Some(key_bytes) = roster.get_member_key(sender_member) {
                    MemberFingerprint::from_signing_key(key_bytes)
                } else {
                    // Member not found in epoch roster - check if message is from before join
                    if encryption_epoch < self.join_epoch {
                        tracing::debug!(
                            ?sender_member,
                            encryption_epoch = encryption_epoch.0,
                            join_epoch = self.join_epoch.0,
                            "message from before join epoch, dropping"
                        );
                        return;
                    }
                    tracing::warn!(
                        ?sender_member,
                        encryption_epoch = encryption_epoch.0,
                        "sender not found in roster for encryption epoch, dropping message"
                    );
                    return;
                }
            } else {
                // No roster available for this epoch
                if encryption_epoch < self.join_epoch {
                    tracing::debug!(
                        encryption_epoch = encryption_epoch.0,
                        join_epoch = self.join_epoch.0,
                        "message from before join epoch (no roster), dropping"
                    );
                    return;
                }
                tracing::warn!(
                    ?sender_member,
                    encryption_epoch = encryption_epoch.0,
                    "no roster available for encryption epoch, dropping message"
                );
                return;
            }
        }
    };
    
    // Continue with existing deduplication and processing logic...
}
```

### Step 4: Handle Edge Cases

#### Edge Case 1: Messages from Before Join Epoch

**Scenario**: Device joins at epoch 10, receives messages from epoch 5.

**Handling**: Check if `encryption_epoch < self.join_epoch`. If so, drop the message with debug log (not warn) — this is expected behavior.

**Code**: Already handled in Step 3 logic above.

#### Edge Case 2: Compaction Messages from Removed Members

**Scenario**: Member sends compaction message at epoch 4, then is removed. Device processes compaction message after removal.

**Handling**: Same as regular messages — check membership at encryption epoch (epoch 4). Compaction messages use the same `handle_decrypted_message` path, so no special handling needed.

**Verification**: Compaction messages are application messages (`AuthData::Compaction`), so they follow the same code path.

#### Edge Case 3: Leaf Index Reuse

**Scenario**: Member A has leaf index 5, is removed, then Member B joins and gets leaf index 5.

**Handling**: Use `MemberFingerprint` for identity, not `sender_index`. Fingerprints are unique and stable, so even if leaf indices are reused, we can correctly identify the sender.

**Verification**: The code in Step 3 uses fingerprints for final identity check, so this is already handled.

#### Edge Case 4: Missing Historical Roster

**Scenario**: Device processes a message from epoch 5, but we only have rosters starting from epoch 8 (device joined later or rosters were pruned).

**Handling**: 
- If `encryption_epoch < self.join_epoch`: Drop with debug log (expected)
- Otherwise: Drop with warn log (unexpected but not fatal)

**Code**: Already handled in Step 3 lookup logic.

#### Edge Case 5: Epoch Extraction Failure

**Scenario**: Unable to parse epoch from MLS message (malformed message or API change).

**Handling**: Fallback to current epoch with warning, then use current roster lookup. This maintains backward compatibility but may incorrectly drop some messages. Log the failure for investigation.

**Code**: In `handle_encrypted_message`, if epoch extraction fails:
```rust
let encryption_epoch = extract_epoch_from_mls_message(&mls_message)
    .unwrap_or_else(|| {
        tracing::warn!("failed to extract epoch from MLS message, using current epoch");
        self.learner.mls_epoch()
    });
```

### Step 5: Testing Strategy

#### Unit Tests

1. **Epoch Extraction**: Test parsing epoch from various `MlsMessage` formats
2. **Roster Storage**: Test that rosters are stored when epochs advance
3. **Roster Lookup**: Test `get_roster_at_epoch` with various scenarios:
   - Exact epoch match
   - Fallback to earlier epoch
   - Missing roster (before join)
4. **Message Processing**: Test `handle_decrypted_message` with:
   - Message from current epoch (sender in current roster)
   - Message from past epoch (sender in historical roster)
   - Message from removed member (sender not in current roster but in historical)
   - Message from before join epoch

#### Integration Tests

1. **Removed Member Scenario**: 
   - Device A sends messages at epoch 4
   - Device B removes Device A (epoch advances to 5)
   - Device C joins and backfills
   - Verify Device C processes Device A's epoch 4 messages

2. **Compaction from Removed Member**:
   - Member sends compaction at epoch 4
   - Member is removed (epoch advances to 5)
   - Verify compaction message is processed correctly

3. **Multiple Removals**:
   - Remove Member A at epoch 5
   - Remove Member B at epoch 6
   - Verify messages from both are processed correctly

4. **Join After Removal**:
   - Device joins at epoch 10
   - Receives messages from epoch 4 (before join)
   - Verify messages are dropped with debug log (not warn)

#### Manual Testing

1. Create a group with 3 devices
2. Device A makes edits
3. Device B removes Device A
4. Device C joins and syncs
5. Verify Device C receives Device A's edits
6. Verify no data loss occurred

### Step 6: Implementation Order

1. **Add epoch roster storage** to `GroupActor` (Step 2)
   - Add `epoch_rosters` field
   - Add `build_epoch_roster` method
   - Store rosters in `process_commit_effect`

2. **Add epoch extraction** from MLS messages (Step 1)
   - Implement epoch parsing from `MlsMessage`
   - Update `handle_encrypted_message` to extract and pass epoch

3. **Update roster lookup logic** (Step 3)
   - Modify `handle_decrypted_message` to accept epoch parameter
   - Implement historical roster lookup
   - Use `MemberFingerprint` for identity

4. **Handle edge cases** (Step 4)
   - Add pre-join epoch checks
   - Add fallback for epoch extraction failure
   - Add appropriate logging

5. **Add tests** (Step 5)
   - Unit tests for each component
   - Integration tests for full scenarios

### Step 7: Migration Considerations

**Backward Compatibility**: The change is backward compatible:
- If epoch extraction fails, fallback to current epoch behavior
- If historical rosters are missing, fallback to current roster lookup
- Existing behavior is preserved as fallback

**Performance Impact**: 
- Minimal: In-memory HashMap lookup is O(1) average case
- Roster storage happens once per epoch advance (infrequent)
- No additional network calls or disk I/O

**Memory Impact**:
- Each roster stores: epoch (8 bytes) + members (variable, ~100 bytes per member)
- For a group with 10 members over 100 epochs: ~100KB (negligible)
- Can add pruning logic to remove rosters older than N epochs if needed

### Step 8: Code Locations

**Files to Modify**:

1. `sync-proposer/src/group/group_actor.rs`:
   - Add `epoch_rosters` field to `GroupActor` struct
   - Add `build_epoch_roster` method
   - Modify `process_commit_effect` to store rosters
   - Modify `handle_encrypted_message` to extract epoch
   - Modify `handle_decrypted_message` to use epoch-specific roster lookup
   - Add `get_roster_at_epoch` helper method

2. `sync-core/src/protocol.rs` or `sync-core/src/roster.rs` (new file):
   - Move `EpochRoster` from `sync-acceptor/src/state_store.rs` to shared location
   - Update acceptor code to import from shared location
   - Proposer can then import from shared location

3. `sync-acceptor/src/state_store.rs`:
   - Update imports to use shared `EpochRoster` type

**New Dependencies**: None (use existing `HashMap` from std)

### Step 9: Verification Checklist

- [ ] Epoch extraction works for all `MlsMessage` variants
- [ ] Rosters are stored when epochs advance
- [ ] Historical roster lookup finds correct roster
- [ ] Messages from removed members are processed correctly
- [ ] Messages from before join epoch are dropped appropriately
- [ ] Compaction messages from removed members work
- [ ] Leaf index reuse doesn't cause incorrect matches
- [ ] Fallback behavior works when epoch extraction fails
- [ ] Performance is acceptable (no regressions)
- [ ] All tests pass
- [ ] Manual testing confirms fix works
