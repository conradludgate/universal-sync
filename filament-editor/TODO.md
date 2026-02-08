# Filament Editor — Implementation Plan

## Architecture

```
Frontend (JS)
    │
    ▼
Tauri Commands (thin async fns)
    │
    │  AppState holds only: mpsc::Sender<CoordinatorRequest>
    ▼
CoordinatorActor  (tokio::spawn, single instance)
    │
    ├── Owns: GroupClient (MLS + iroh + CRDT factories)
    ├── Owns: HashMap<GroupId, mpsc::Sender<DocRequest>>
    │
    ├── Handles: create_document, get_key_package, recv_welcome, join_document_bytes
    │
    └── Routes: document-specific requests → correct DocumentActor
                                              │
        ┌─────────────────────────────────────┘
        ▼
DocumentActor[group_id]  (one per open document, tokio::spawn)
    │
    ├── Owns: Group<C, CS>  (which owns the CRDT + actor handle)
    ├── Owns: tauri::AppHandle  (for emitting events)
    │
    ├── select! loop:
    │     ├── DocRequest from channel → apply_delta, get_text, add_member, etc.
    │     └── group.wait_for_update() → emit "document-updated" Tauri event
    │
    └── Handles: apply_delta, get_document_text, add_member,
                 add_acceptor, list_acceptors, shutdown
```

No mutexes anywhere. All shared state is mediated by mpsc + oneshot channels.

## Tasks

### 1. Project scaffolding

- [x] Add `filament-editor` to workspace members in root `Cargo.toml`
- [x] Create `filament-editor/Cargo.toml` with dependencies
- [x] Create `filament-editor/build.rs` (tauri codegen build script)
- [x] Fix `capabilities/default.json` (removed unavailable `shell:allow-open`)

### 2. Request / response types (`filament-editor/src/types.rs`)

- [x] Define `Delta` enum (Insert, Delete, Replace)
- [x] Define `DocumentInfo` struct
- [x] Define `DocumentUpdatedPayload` struct
- [x] Define `AppState` struct (holds `mpsc::Sender<CoordinatorRequest>`)
- [x] Define `CoordinatorRequest` enum
- [x] Define `DocRequest` enum

### 3. CoordinatorActor (`filament-editor/src/actor.rs`)

- [x] Create `CoordinatorActor` struct with `welcome_rx` split out via `take_welcome_rx()`
- [x] Implement `run()` with `select!` over requests + welcome messages (non-blocking)
- [x] Implement `create_document()` — creates group, spawns DocumentActor
- [x] Implement `get_key_package()` — generates and base58-encodes
- [x] Implement `recv_welcome()` — stores pending reply, responds when welcome arrives
- [x] Implement `join_document_bytes()` — joins from base58 welcome
- [x] Implement `ForDoc` routing
- [x] Added `GroupClient::take_welcome_rx()` to `filament-weave/src/client.rs`

### 4. DocumentActor (`filament-editor/src/document.rs`)

- [x] Create `DocumentActor` struct
- [x] Implement `run()` select loop (requests + remote updates)
- [x] Implement `apply_delta()` with Insert, Delete, and Replace support
- [x] Implement `get_text()`
- [x] Implement `add_member()`
- [x] Implement `add_acceptor()`
- [x] Implement `list_acceptors()`
- [x] Implement `emit_text_update()` — emits `document-updated` Tauri event

### 5. Tauri commands (`filament-editor/src/commands.rs`)

Each command is a thin async fn that sends a request to the coordinator and awaits the oneshot reply.

- [x] `create_document(state) → DocumentInfo`
- [x] `get_key_package(state) → String`
- [x] `recv_welcome(state) → DocumentInfo`
- [x] `join_document_bytes(state, welcome_b58) → DocumentInfo`
- [x] `apply_delta(state, group_id, delta) → ()`
- [x] `get_document_text(state, group_id) → String`
- [x] `add_member(state, group_id, key_package_b58) → ()`
- [x] `add_acceptor(state, group_id, addr_b58) → ()`
- [x] `list_acceptors(state, group_id) → Vec<String>`

### 6. App entry point (`filament-editor/src/main.rs`)

- [x] Set up tracing
- [x] Generate ephemeral iroh secret key
- [x] Create MLS client with all extension types registered
- [x] Create iroh endpoint
- [x] Create `GroupClient`, register `NoCrdtFactory` + `YrsCrdtFactory`
- [x] Create `mpsc::channel` eagerly so `AppState` is available immediately
- [x] Spawn `CoordinatorActor` in `setup()` callback
- [x] Build and run Tauri app with all command handlers

### 7. Frontend adjustments (`filament-editor/ui/app.js`)

- [x] Fixed `computeDelta` to handle Replace (simultaneous delete + insert)
- [x] Updated `startWelcomeListener` — `recv_welcome` now returns `DocumentInfo` directly
- [x] Added echo suppression in `document-updated` handler (skip if text unchanged)
- [x] Verified `document-updated` event handler matches `group_id`

### 8. Testing

Tests are split into three layers: unit tests for pure logic, actor tests for
message-passing correctness, and end-to-end integration tests.

#### 8a. Unit tests (no Tauri, no network)

These test pure functions and type conversions in isolation.

- [x] `Delta` serde: round-trip `Insert`, `Delete`, and `Replace` variants through JSON
- [x] `Delta` application to a standalone `YrsCrdt`:
      - Insert at beginning / middle / end
      - Delete from beginning / middle / end
      - Insert then delete (replace semantics)
      - Empty insert (no-op)
      - Delete with length 0 (no-op)
- [x] `DocumentInfo` serialization matches what the frontend expects
- [x] Base58 parsing helpers: valid input, invalid input, empty input

#### 8b. Actor tests (no Tauri, real network via `filament-testing` helpers)

These spawn actors directly (without Tauri) using the same test infrastructure
as `filament-testing/tests/integration.rs`. They verify the actor message-passing
and routing logic. Uses a `MockEmitter` that captures events into an `mpsc`
channel instead of requiring a Tauri `AppHandle`.

- [x] **DocumentActor — local edit round-trip**:
      Send `ApplyDelta(Insert)` → `GetText` → verify text matches
- [x] **DocumentActor — multiple sequential edits**:
      Send several `ApplyDelta`s → `GetText` → verify accumulated text
- [x] **DocumentActor — delete after insert**:
      Insert text → delete part of it → `GetText` → verify
- [x] **DocumentActor — remote update triggers event**:
      Two `Group`s in the same MLS group (Alice & Bob).
      Alice's DocumentActor applies a delta and sends an update.
      Bob's DocumentActor receives the update via `wait_for_update()`
      and emits a `document-updated` event (captured via channel).
      Verify Bob's text matches Alice's.
- [x] **CoordinatorActor — create document**:
      Send `CreateDocument` → verify `DocumentInfo` returned with valid group_id
      Send `ForDoc { GetText }` → verify empty text for new document
- [x] **CoordinatorActor — create multiple documents**:
      Create two documents → verify different group_ids
      Apply deltas to each independently → verify texts are independent
- [x] **CoordinatorActor — route to correct DocumentActor**:
      Create two documents. Apply delta to doc A. GetText from doc A (has text)
      and doc B (still empty) — verify isolation.
- [x] **CoordinatorActor — route to unknown group_id**:
      Send `ForDoc` with a non-existent group_id → verify the oneshot reply
      is dropped (caller receives an error, no panic)
- [x] **CoordinatorActor — join via welcome**:
      Use two CoordinatorActors (Alice and Bob, each with their own GroupClient).
      Alice creates a document and adds Bob. Bob receives the welcome, joins,
      and verifies the document text matches.
- [x] **Full two-peer sync**:
      Alice creates doc, adds acceptor, adds Bob.
      Alice types "Hello" → Bob sees "Hello" (via event).
      Bob types " World" → Alice sees "Hello World" (via event).

#### 8c. Frontend / E2E tests (optional, manual for now)

These require the full Tauri app running. Document as manual test scripts
rather than automated tests for now.

- [ ] Launch app → "No document open" empty state shown
- [ ] Click "Create New Document" → editor appears, sync status shows "Synced"
- [ ] Type text → no errors, sync indicator stays green
- [ ] Click "Join" → invite code generated, modal shows "Waiting to be added..."
- [ ] Two-window test: Window A creates doc + adds acceptor.
      Window B clicks Join, copies invite code. Window A pastes invite code
      in Add Member. Window B auto-joins. Typing in A appears in B and vice versa.
- [ ] Acceptor modal: add/list acceptors works correctly
