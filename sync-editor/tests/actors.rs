//! Actor-level integration tests.
//!
//! These test the `DocumentActor` and `CoordinatorActor` without Tauri,
//! using a mock event emitter that captures events into an `mpsc` channel.

use std::time::Duration;

use sync_editor::actor::CoordinatorActor;
use sync_editor::document::DocumentActor;
use sync_editor::types::{
    CoordinatorRequest, Delta, DocRequest, DocumentInfo, DocumentUpdatedPayload, EventEmitter,
};
use tokio::sync::{mpsc, oneshot};
use universal_sync_core::GroupId;
use universal_sync_testing::{
    YrsCrdt, init_tracing, spawn_acceptor, test_endpoint, test_yrs_group_client,
};

// =============================================================================
// Mock event emitter
// =============================================================================

#[derive(Clone)]
struct MockEmitter {
    tx: mpsc::Sender<DocumentUpdatedPayload>,
}

impl EventEmitter for MockEmitter {
    fn emit_document_updated(&self, payload: &DocumentUpdatedPayload) {
        let _ = self.tx.try_send(payload.clone());
    }
    fn emit_group_state_changed(&self, _payload: &sync_editor::types::GroupStatePayload) {}
    fn emit_awareness_changed(&self, _payload: &sync_editor::types::AwarenessPayload) {}
}

fn mock_emitter() -> (MockEmitter, mpsc::Receiver<DocumentUpdatedPayload>) {
    let (tx, rx) = mpsc::channel(64);
    (MockEmitter { tx }, rx)
}

// =============================================================================
// Helper: send a doc request and get the reply
// =============================================================================

async fn send_doc<T>(
    tx: &mpsc::Sender<DocRequest>,
    make: impl FnOnce(oneshot::Sender<Result<T, String>>) -> DocRequest,
) -> Result<T, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(make(reply_tx)).await.unwrap();
    reply_rx.await.unwrap()
}

async fn send_coord<T>(
    tx: &mpsc::Sender<CoordinatorRequest>,
    make: impl FnOnce(oneshot::Sender<Result<T, String>>) -> CoordinatorRequest,
) -> Result<T, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(make(reply_tx)).await.unwrap();
    reply_rx.await.unwrap()
}

/// Send ForDoc request via coordinator
async fn send_coord_doc<T>(
    tx: &mpsc::Sender<CoordinatorRequest>,
    group_id: GroupId,
    make: impl FnOnce(oneshot::Sender<Result<T, String>>) -> DocRequest,
) -> Result<T, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(CoordinatorRequest::ForDoc {
        group_id,
        request: make(reply_tx),
    })
    .await
    .unwrap();
    reply_rx.await.unwrap()
}

// =============================================================================
// DocumentActor tests
// =============================================================================

/// Spawn a standalone DocumentActor for testing.
/// Returns the request sender, the event receiver, and the group_id.
async fn spawn_doc_actor() -> (
    mpsc::Sender<DocRequest>,
    mpsc::Receiver<DocumentUpdatedPayload>,
    GroupId,
) {
    let (acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = test_yrs_group_client("doc-actor-test", test_endpoint().await);
    let group = client
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");

    let group_id = group.group_id();
    let crdt = YrsCrdt::new();
    let (doc_tx, doc_rx) = mpsc::channel(64);
    let (emitter, event_rx) = mock_emitter();

    let actor = DocumentActor::new(group, crdt, group_id, doc_rx, emitter);
    tokio::spawn(async move {
        actor.run().await;
        acceptor_task.abort();
    });

    (doc_tx, event_rx, group_id)
}

#[tokio::test]
async fn doc_actor_local_edit_round_trip() {
    init_tracing();
    let (tx, _event_rx, _id) = spawn_doc_actor().await;

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "hello".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("apply delta");

    let text = send_doc(&tx, |reply| DocRequest::GetText { reply })
        .await
        .expect("get text");
    assert_eq!(text, "hello");
}

#[tokio::test]
async fn doc_actor_multiple_sequential_edits() {
    init_tracing();
    let (tx, _event_rx, _id) = spawn_doc_actor().await;

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 5,
            text: " World".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 11,
            text: "!".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    let text = send_doc(&tx, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();
    assert_eq!(text, "Hello World!");
}

#[tokio::test]
async fn doc_actor_delete_after_insert() {
    init_tracing();
    let (tx, _event_rx, _id) = spawn_doc_actor().await;

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello World".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    send_doc(&tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Delete {
            position: 5,
            length: 6,
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    let text = send_doc(&tx, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();
    assert_eq!(text, "Hello");
}

/// Test that a remote CRDT update triggers a document-updated event emission.
///
/// Two DocumentActors sharing the same MLS group (Alice & Bob).
/// Alice applies a delta -> Bob's doc actor receives via wait_for_update()
/// and emits a `document-updated` event captured by the MockEmitter.
#[tokio::test]
async fn doc_actor_remote_update_triggers_event() {
    init_tracing();

    let (_acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice_client = test_yrs_group_client("alice-doc-event", test_endpoint().await);
    let mut alice_group = alice_client
        .create_group(std::slice::from_ref(&acceptor_addr), "yrs")
        .await
        .expect("create group");
    let group_id = alice_group.group_id();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut bob_client = test_yrs_group_client("bob-doc-event", test_endpoint().await);
    let bob_kp = bob_client.generate_key_package().expect("bob kp");
    alice_group.add_member(bob_kp).await.expect("add bob");
    let welcome = bob_client.recv_welcome().await.expect("bob welcome");
    let join_info = bob_client.join_group(&welcome).await.expect("bob join");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let (bob_doc_tx, mut bob_event_rx) = {
        let bob_crdt = if let Some(snapshot) = join_info.snapshot {
            YrsCrdt::from_snapshot(&snapshot, 0).expect("from snapshot")
        } else {
            YrsCrdt::new()
        };
        let (tx, rx) = mpsc::channel(64);
        let (emitter, event_rx) = mock_emitter();
        let actor = DocumentActor::new(join_info.group, bob_crdt, group_id, rx, emitter);
        tokio::spawn(actor.run());
        (tx, event_rx)
    };

    let (alice_doc_tx, _alice_event_rx) = {
        let alice_crdt = YrsCrdt::new();
        let (tx, rx) = mpsc::channel(64);
        let (emitter, _event_rx) = mock_emitter();
        let actor = DocumentActor::new(alice_group, alice_crdt, group_id, rx, emitter);
        tokio::spawn(actor.run());
        (tx, _event_rx)
    };

    send_doc(&alice_doc_tx, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice apply delta");

    let event = tokio::time::timeout(Duration::from_secs(5), bob_event_rx.recv())
        .await
        .expect("bob event timeout")
        .expect("bob event channel closed");

    assert_eq!(event.text, "Hello");
    assert_eq!(event.deltas.len(), 1);
    assert!(
        matches!(&event.deltas[0], Delta::Insert { position: 0, text } if text == "Hello"),
        "expected Insert delta, got {:?}",
        event.deltas
    );

    let bob_text = send_doc(&bob_doc_tx, |reply| DocRequest::GetText { reply })
        .await
        .expect("bob get text");
    assert_eq!(bob_text, "Hello");
}

// =============================================================================
// CoordinatorActor tests
// =============================================================================

/// Spawn a CoordinatorActor for testing.
/// Returns the request sender and the event receiver.
fn spawn_coordinator(
    client: universal_sync_proposer::GroupClient<
        impl mls_rs::client_builder::MlsConfig + 'static,
        impl mls_rs::CipherSuiteProvider + Clone + 'static,
    >,
) -> (
    mpsc::Sender<CoordinatorRequest>,
    mpsc::Receiver<DocumentUpdatedPayload>,
) {
    let (coord_tx, coord_rx) = mpsc::channel(64);
    let (emitter, event_rx) = mock_emitter();

    let actor = CoordinatorActor::new(client, coord_rx, emitter);
    tokio::spawn(actor.run());

    (coord_tx, event_rx)
}

#[tokio::test]
async fn coordinator_create_document() {
    init_tracing();
    let (_acceptor_task, _acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = test_yrs_group_client("coord-create", test_endpoint().await);
    let (tx, _event_rx) = spawn_coordinator(client);

    let info: DocumentInfo = send_coord(&tx, |reply| CoordinatorRequest::CreateDocument { reply })
        .await
        .expect("create document");

    assert!(!info.group_id.is_empty());
    assert_eq!(info.text, "");

    let group_id = GroupId::from_slice(&bs58::decode(&info.group_id).into_vec().unwrap());
    let text = send_coord_doc(&tx, group_id, |reply| DocRequest::GetText { reply })
        .await
        .expect("get text");
    assert_eq!(text, "");

    drop(tx);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn coordinator_create_multiple_documents() {
    init_tracing();
    let (_acceptor_task, _addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = test_yrs_group_client("coord-multi", test_endpoint().await);
    let (tx, _event_rx) = spawn_coordinator(client);

    let info_a = send_coord(&tx, |reply| CoordinatorRequest::CreateDocument { reply })
        .await
        .expect("create doc A");

    let info_b = send_coord(&tx, |reply| CoordinatorRequest::CreateDocument { reply })
        .await
        .expect("create doc B");

    assert_ne!(info_a.group_id, info_b.group_id);

    let id_a = GroupId::from_slice(&bs58::decode(&info_a.group_id).into_vec().unwrap());
    let id_b = GroupId::from_slice(&bs58::decode(&info_b.group_id).into_vec().unwrap());

    send_coord_doc(&tx, id_a, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "doc A text".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .unwrap();

    let text_a = send_coord_doc(&tx, id_a, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();
    let text_b = send_coord_doc(&tx, id_b, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();

    assert_eq!(text_a, "doc A text");
    assert_eq!(text_b, "");
}

#[tokio::test]
async fn coordinator_route_to_unknown_group_id() {
    init_tracing();
    let client = test_yrs_group_client("coord-unknown", test_endpoint().await);
    let (tx, _event_rx) = spawn_coordinator(client);

    let fake_id = GroupId::new([42u8; 32]);

    let (reply_tx, reply_rx) = oneshot::channel::<Result<String, String>>();
    tx.send(CoordinatorRequest::ForDoc {
        group_id: fake_id,
        request: DocRequest::GetText { reply: reply_tx },
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = reply_rx.await;
    assert!(result.is_err(), "expected RecvError for unknown group_id");
}

// =============================================================================
// Two-peer sync via CoordinatorActor
// =============================================================================

#[tokio::test]
async fn coordinator_join_via_welcome() {
    init_tracing();
    let (_acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice_client = test_yrs_group_client("alice-coord", test_endpoint().await);
    let (alice_tx, _alice_events) = spawn_coordinator(alice_client);

    let doc_info = send_coord(&alice_tx, |reply| CoordinatorRequest::CreateDocument {
        reply,
    })
    .await
    .expect("alice create doc");
    let alice_group_id = GroupId::from_slice(&bs58::decode(&doc_info.group_id).into_vec().unwrap());

    let addr_b58 = bs58::encode(postcard::to_allocvec(&acceptor_addr).unwrap()).into_string();
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddAcceptor {
        addr_b58,
        reply,
    })
    .await
    .expect("add acceptor");
    tokio::time::sleep(Duration::from_millis(100)).await;

    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello from Alice".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice write");

    let bob_client = test_yrs_group_client("bob-coord", test_endpoint().await);
    let bob_kp = bob_client.generate_key_package().expect("bob kp");
    let bob_kp_b58 = bs58::encode(bob_kp.to_bytes().unwrap()).into_string();
    let (bob_tx, _bob_events) = spawn_coordinator(bob_client);

    let bob_tx2 = bob_tx.clone();
    let bob_welcome_handle = tokio::spawn(async move {
        send_coord(&bob_tx2, |reply| CoordinatorRequest::RecvWelcome { reply }).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddMember {
        key_package_b58: bob_kp_b58,
        reply,
    })
    .await
    .expect("alice add bob");

    let bob_doc_info = tokio::time::timeout(Duration::from_secs(10), bob_welcome_handle)
        .await
        .expect("bob welcome timeout")
        .expect("bob task panicked")
        .expect("bob join");

    let bob_group_id =
        GroupId::from_slice(&bs58::decode(&bob_doc_info.group_id).into_vec().unwrap());

    let mut synced = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let bob_text = send_coord_doc(&bob_tx, bob_group_id, |reply| DocRequest::GetText { reply })
            .await
            .expect("bob get text");
        if bob_text == "Hello from Alice" {
            synced = true;
            break;
        }
    }
    assert!(
        synced,
        "Bob should eventually sync Alice's text via backfill"
    );
}

#[tokio::test]
async fn full_two_peer_sync() {
    init_tracing();
    let (_acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let alice_client = test_yrs_group_client("alice-sync", test_endpoint().await);
    let (alice_tx, mut alice_events) = spawn_coordinator(alice_client);

    let doc_info = send_coord(&alice_tx, |reply| CoordinatorRequest::CreateDocument {
        reply,
    })
    .await
    .expect("alice create doc");
    let alice_group_id = GroupId::from_slice(&bs58::decode(&doc_info.group_id).into_vec().unwrap());

    let addr_b58 = bs58::encode(postcard::to_allocvec(&acceptor_addr).unwrap()).into_string();
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddAcceptor {
        addr_b58,
        reply,
    })
    .await
    .expect("add acceptor");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bob_client = test_yrs_group_client("bob-sync", test_endpoint().await);
    let bob_kp = bob_client.generate_key_package().expect("bob kp");
    let bob_kp_b58 = bs58::encode(bob_kp.to_bytes().unwrap()).into_string();
    let (bob_tx, mut bob_events) = spawn_coordinator(bob_client);

    let bob_tx2 = bob_tx.clone();
    let bob_welcome_handle = tokio::spawn(async move {
        send_coord(&bob_tx2, |reply| CoordinatorRequest::RecvWelcome { reply }).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddMember {
        key_package_b58: bob_kp_b58,
        reply,
    })
    .await
    .expect("alice add bob");

    let bob_doc_info = tokio::time::timeout(Duration::from_secs(10), bob_welcome_handle)
        .await
        .expect("bob welcome timeout")
        .expect("bob task panicked")
        .expect("bob join");

    let bob_group_id =
        GroupId::from_slice(&bs58::decode(&bob_doc_info.group_id).into_vec().unwrap());

    tokio::time::sleep(Duration::from_millis(200)).await;

    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice type hello");

    let bob_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = bob_events.recv().await.expect("bob event channel closed");
            if !event.deltas.is_empty() {
                return event;
            }
        }
    })
    .await
    .expect("bob event timeout");
    assert_eq!(bob_event.text, "Hello");

    send_coord_doc(&bob_tx, bob_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 5,
            text: " World".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("bob type world");

    let alice_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = alice_events
                .recv()
                .await
                .expect("alice event channel closed");
            if !event.deltas.is_empty() {
                return event;
            }
        }
    })
    .await
    .expect("alice event timeout");
    assert_eq!(alice_event.text, "Hello World");

    let alice_text = send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::GetText {
        reply,
    })
    .await
    .unwrap();
    let bob_text = send_coord_doc(&bob_tx, bob_group_id, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();

    assert_eq!(alice_text, "Hello World");
    assert_eq!(bob_text, "Hello World");
}

#[tokio::test]
async fn two_member_unidirectional_sync() {
    init_tracing();
    let (_acceptor_task, acceptor_addr, _dir) = spawn_acceptor().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Spawn Alice's coordinator
    let alice_client = test_yrs_group_client("alice-uni", test_endpoint().await);
    let (alice_tx, _alice_events) = spawn_coordinator(alice_client);

    // Alice creates a document
    let doc_info = send_coord(&alice_tx, |reply| CoordinatorRequest::CreateDocument {
        reply,
    })
    .await
    .expect("alice create doc");
    let alice_group_id = GroupId::from_slice(&bs58::decode(&doc_info.group_id).into_vec().unwrap());

    // Alice adds the acceptor
    let addr_b58 = bs58::encode(postcard::to_allocvec(&acceptor_addr).unwrap()).into_string();
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddAcceptor {
        addr_b58,
        reply,
    })
    .await
    .expect("add acceptor");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn Bob's coordinator
    let bob_client = test_yrs_group_client("bob-uni", test_endpoint().await);
    let bob_kp = bob_client.generate_key_package().expect("bob kp");
    let bob_kp_b58 = bs58::encode(bob_kp.to_bytes().unwrap()).into_string();
    let (bob_tx, mut bob_events) = spawn_coordinator(bob_client);

    // Bob starts waiting for a welcome
    let bob_tx2 = bob_tx.clone();
    let bob_welcome_handle = tokio::spawn(async move {
        send_coord(&bob_tx2, |reply| CoordinatorRequest::RecvWelcome { reply }).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice adds Bob as a member
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::AddMember {
        key_package_b58: bob_kp_b58,
        reply,
    })
    .await
    .expect("alice add bob");

    let bob_doc_info = tokio::time::timeout(Duration::from_secs(10), bob_welcome_handle)
        .await
        .expect("bob welcome timeout")
        .expect("bob task panicked")
        .expect("bob join");

    let bob_group_id =
        GroupId::from_slice(&bs58::decode(&bob_doc_info.group_id).into_vec().unwrap());
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice inserts "Hello"
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 0,
            text: "Hello".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice insert Hello");

    let bob_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = bob_events.recv().await.expect("bob event channel closed");
            if !event.deltas.is_empty() {
                return event;
            }
        }
    })
    .await
    .expect("bob event timeout after Hello");
    assert_eq!(bob_event.text, "Hello");

    // Alice appends " World"
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Insert {
            position: 5,
            text: " World".into(),
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice append World");

    let bob_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = bob_events.recv().await.expect("bob event channel closed");
            if !event.deltas.is_empty() {
                return event;
            }
        }
    })
    .await
    .expect("bob event timeout after World");
    assert_eq!(bob_event.text, "Hello World");

    // Alice deletes " World" (last 6 chars)
    send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::ApplyDelta {
        delta: Delta::Delete {
            position: 5,
            length: 6,
        },
        anchor: 0,
        head: 0,
        reply,
    })
    .await
    .expect("alice delete World");

    let bob_event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = bob_events.recv().await.expect("bob event channel closed");
            if !event.deltas.is_empty() {
                return event;
            }
        }
    })
    .await
    .expect("bob event timeout after delete");
    assert_eq!(bob_event.text, "Hello");

    // Verify both members have the same final text
    let alice_text = send_coord_doc(&alice_tx, alice_group_id, |reply| DocRequest::GetText {
        reply,
    })
    .await
    .unwrap();
    let bob_text = send_coord_doc(&bob_tx, bob_group_id, |reply| DocRequest::GetText { reply })
        .await
        .unwrap();

    assert_eq!(alice_text, "Hello");
    assert_eq!(bob_text, "Hello");
}
