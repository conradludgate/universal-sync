# Update Compression / Compaction — Implementation Plan

## Overview

Hierarchical LSM-style compaction for CRDT updates. Older updates are merged
into larger compressed entries and the originals are pruned from acceptor storage.

## Phase 1: Foundational Types (`sync-core`)

### 1a. `MemberFingerprint` and new `MessageId`

- [ ] Add `MemberFingerprint([u8; 32])` — SHA-256 of MLS signing public key
  - Derive, Hash, Eq, Ord, Serialize, Deserialize
  - `fn from_signing_key(key: &[u8]) -> Self`
- [ ] Replace `MessageId` with `{ group_id: GroupId, sender: MemberFingerprint, seq: u64 }`
- [ ] Add `StateVector = BTreeMap<MemberFingerprint, u64>` type alias
- [ ] Remove old `MessageId` (group_id, epoch, sender, index)

### 1b. Update `MessageRequest` / `MessageResponse`

- [ ] `MessageRequest::Send { id: MessageId, message: EncryptedAppMessage }`
- [ ] `MessageRequest::Subscribe { state_vector: StateVector }`
- [ ] `MessageRequest::Backfill { state_vector: StateVector, limit: u32 }`
- [ ] `MessageResponse::Stored` (no arrival_seq)
- [ ] `MessageResponse::Message { id: MessageId, message: EncryptedAppMessage }`
- [ ] `MessageResponse::BackfillComplete { has_more: bool }`
- [ ] Remove `MessageResponse::Stored { arrival_seq }` variant

### 1c. Custom proposal types for compaction

- [ ] `CompactionClaim { level: u8, watermark: StateVector, deadline: u64 }`
  - MLS custom proposal (unencrypted, signed)
  - Register extension type in private range
- [ ] `CompactionComplete { level: u8, watermark: StateVector }`
  - MLS custom proposal (unencrypted, signed)
  - Register extension type in private range
- [ ] Register both in MLS client extension type lists

### 1d. `CrdtFactory` and `CompactionConfig`

- [ ] Add `CompactionConfig` struct:
  - `levels: u8` — number of levels (minimum 2)
  - `compaction_thresholds: Vec<u32>` — entries at level N to trigger compaction to N+1
  - `replication: Vec<u8>` — per-level replication factor (0 = all acceptors)
- [ ] Add default `fn compact(&self, base: Option<&[u8]>, updates: &[&[u8]]) -> Result<Vec<u8>, CrdtError>` to `CrdtFactory`
- [ ] Add `fn compaction_config(&self) -> CompactionConfig` with sensible default

## Phase 2: Acceptor Storage (`sync-acceptor`)

### 2a. Rekey message storage

- [ ] Change message primary key from `(group_id, arrival_seq)` to `(group_id, sender_fingerprint, seq)`
- [ ] Remove `message_seq` keyspace and `next_message_seq()` counter
- [ ] Update `store_app_message()` to accept `MessageId`
- [ ] Update `get_messages_since()` → `get_messages_after(group_id, state_vector)` returning messages not covered by the state vector
- [ ] Update broadcast to include `MessageId`

### 2b. Watermark deletion

- [ ] Add `delete_before_watermark(group_id, watermark: StateVector)` to `FjallStateStore`
  - Iterate per sender, delete entries where seq <= watermark[sender]
- [ ] Wire into `GroupAcceptor::apply()` — when a `CompactionComplete` custom proposal is seen, call delete

### 2c. Update message stream handler

- [ ] Update `handle_message_request()` for new `MessageRequest` variants
- [ ] Backfill: scan per-sender ranges, return messages not in client's state vector
- [ ] Subscribe: filter broadcasts against client's state vector

## Phase 3: Proposer / Group (`sync-proposer`)

### 3a. Message ID generation

- [ ] Compute `MemberFingerprint` from MLS signing key on group create/join
- [ ] Track per-sender monotonic `seq: u64` (never resets across epochs)
- [ ] Update `handle_send_message()` to use new `MessageId`
- [ ] Remove `message_index` / `message_epoch` fields from `GroupActor`

### 3b. State vector tracking

- [ ] Track local `StateVector` in `GroupActor` — updated on send and receive
- [ ] Use state vector for backfill requests instead of `since_seq`
- [ ] Update deduplication: `seen_messages` keyed by `(MemberFingerprint, u64)` instead of `(MemberId, Epoch, u32)`

### 3c. Rendezvous hashing updates

- [ ] Update `compute_score()` to use new `MessageId` (fingerprint + seq)
- [ ] Parameterize `delivery_count()` by compaction level
- [ ] `fn delivery_count_for_level(num_acceptors: usize, level: u8, config: &CompactionConfig) -> usize`

### 3d. Compaction coordinator

- [ ] Add compaction state to `GroupActor`:
  - Current active claim (if any)
  - Pending compaction claims from other members
  - Per-level entry counts for trigger detection
- [ ] `handle_compaction_trigger()`:
  - Check if compaction needed (threshold exceeded at any level)
  - Check no active claim overlaps
  - Build commit with `CompactionClaim` + `UpdateKeys` proposals
  - Submit through Paxos
- [ ] `handle_compaction_claim_committed()`:
  - If we are the claimer: fetch covered messages, compact, store result, propose `CompactionComplete`
  - If someone else: record claim, back off overlapping work
- [ ] `handle_compaction_complete_committed()`:
  - Update compaction state (watermark advanced)
  - Clear expired claims
- [ ] Claim deadline enforcement:
  - Periodic check: if active claim past deadline, void it
- [ ] On member add: trigger L(max) compaction instead of embedding snapshot in Welcome

### 3e. Welcome changes

- [ ] Remove `CrdtSnapshotExt` from `welcome_group_info_extensions()`
- [ ] Make `CrdtSnapshotExt` optional in `Group::join()` (empty snapshot = backfill from acceptors)
- [ ] After `add_member()` succeeds, trigger L(max) compaction

## Phase 4: Testing (`sync-testing`)

### 4a. Unit tests

- [ ] `MemberFingerprint` derivation from signing key
- [ ] `StateVector` ordering and merge semantics
- [ ] `CompactionConfig` defaults and validation
- [ ] `CrdtFactory::compact()` default implementation
- [ ] `YrsCrdtFactory::compact()` with base snapshot + incremental updates
- [ ] Watermark deletion logic on state store

### 4b. Integration tests

- [ ] Two-peer sync with new `MessageId` and state vector backfill
- [ ] Compaction claim → complete cycle (single member)
- [ ] Compaction with concurrent edits (edits during compaction are preserved)
- [ ] New member join via compaction (no snapshot in Welcome)
- [ ] Claim expiry: claimer crashes, another member re-claims
- [ ] Multi-level compaction: L0 → L1 → L2

## Phase 5: AcceptorAdd/Remove bug fix (separate)

- [ ] Migrate `AcceptorAdd` / `AcceptorRemove` from group context extensions to MLS custom proposals
- [ ] Remove `persistent_context_extensions()` workaround
- [ ] Update acceptor `process_commit_acceptor_changes()` to read custom proposals
- [ ] Update proposer `handle_add_acceptor()` / `handle_remove_acceptor()` to use `add_custom()`
- [ ] Update proposer `process_commit_effect()` to read custom proposals
