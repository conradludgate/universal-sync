//! Request/response types for the editor actor system.
//!
//! These types are intentionally non-generic so they can be held in Tauri's
//! managed state without leaking MLS type parameters.

use serde::{Deserialize, Serialize};
use tauri::AppHandle;
use tokio::sync::{mpsc, oneshot};
use universal_sync_core::GroupId;

// =============================================================================
// Tauri-managed application state
// =============================================================================

/// Application state managed by Tauri.
///
/// Contains only an `mpsc::Sender` — no mutexes, no shared mutable state.
pub struct AppState {
    /// Channel to send requests to the [`CoordinatorActor`](crate::actor::CoordinatorActor).
    pub coordinator_tx: mpsc::Sender<CoordinatorRequest>,
}

// =============================================================================
// Data types exchanged with the frontend
// =============================================================================

/// A text editing operation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Delta {
    /// Insert text at a position.
    Insert { position: u32, text: String },
    /// Delete `length` characters starting at `position`.
    Delete { position: u32, length: u32 },
    /// Replace `length` characters at `position` with `text`.
    Replace {
        position: u32,
        length: u32,
        text: String,
    },
}

/// Information about an open document, returned to the frontend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentInfo {
    pub group_id: String,
    pub text: String,
    pub member_count: usize,
}

/// A member or acceptor listed in the peers dialog.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PeerEntry {
    Member {
        index: u32,
        identity: String,
        is_self: bool,
    },
    Acceptor {
        id: String,
    },
}

/// Payload emitted to the frontend when a document is updated by a remote peer.
#[derive(Debug, Clone, Serialize)]
pub struct DocumentUpdatedPayload {
    pub group_id: String,
    pub text: String,
}

// =============================================================================
// Event emission abstraction (for testability)
// =============================================================================

/// Trait abstracting event emission so actors can be tested without Tauri.
pub trait EventEmitter: Clone + Send + 'static {
    /// Emit a document-updated event.
    fn emit_document_updated(&self, payload: &DocumentUpdatedPayload);
}

/// Production implementation: emits Tauri events to all webviews.
impl EventEmitter for AppHandle {
    fn emit_document_updated(&self, payload: &DocumentUpdatedPayload) {
        use tauri::Emitter;
        let _ = self.emit("document-updated", payload);
    }
}

// =============================================================================
// Coordinator ↔ Tauri command messages
// =============================================================================

/// Request sent from Tauri commands to the [`CoordinatorActor`](crate::actor::CoordinatorActor).
pub enum CoordinatorRequest {
    /// Create a new document (group with Yrs CRDT).
    CreateDocument {
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    /// Generate a key package for joining a group.
    GetKeyPackage {
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Wait for an incoming welcome message, auto-join, and return the document.
    RecvWelcome {
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    /// Join a group from raw welcome bytes (base58-encoded).
    JoinDocumentBytes {
        welcome_b58: String,
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    /// Forward a request to a specific document actor.
    ForDoc {
        group_id: GroupId,
        request: DocRequest,
    },
}

// =============================================================================
// DocumentActor messages
// =============================================================================

/// Request sent from the coordinator (or commands) to a [`DocumentActor`](crate::document::DocumentActor).
pub enum DocRequest {
    /// Apply a text editing delta and broadcast to peers.
    ApplyDelta {
        delta: Delta,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Read the current document text.
    GetText {
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Add a member to the group.
    AddMember {
        key_package_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Add an acceptor to the group.
    AddAcceptor {
        addr_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// List the group's acceptors.
    ListAcceptors {
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    /// List all peers (members + acceptors).
    ListPeers {
        reply: oneshot::Sender<Result<Vec<PeerEntry>, String>>,
    },
    /// Add a peer: auto-detect KeyPackage (member) vs EndpointAddr (acceptor).
    AddPeer {
        input_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Remove a member by roster index.
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Remove an acceptor by its ID (base58).
    RemoveAcceptor {
        acceptor_id_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Shut down the document actor.
    #[allow(dead_code)]
    Shutdown,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    // =========================================================================
    // Delta serde round-trip
    // =========================================================================

    #[test]
    fn delta_insert_round_trip() {
        let delta = Delta::Insert {
            position: 5,
            text: "hello".into(),
        };
        let json = serde_json::to_string(&delta).unwrap();
        let parsed: Delta = serde_json::from_str(&json).unwrap();
        match parsed {
            Delta::Insert { position, text } => {
                assert_eq!(position, 5);
                assert_eq!(text, "hello");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn delta_delete_round_trip() {
        let delta = Delta::Delete {
            position: 3,
            length: 10,
        };
        let json = serde_json::to_string(&delta).unwrap();
        let parsed: Delta = serde_json::from_str(&json).unwrap();
        match parsed {
            Delta::Delete { position, length } => {
                assert_eq!(position, 3);
                assert_eq!(length, 10);
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn delta_replace_round_trip() {
        let delta = Delta::Replace {
            position: 1,
            length: 4,
            text: "world".into(),
        };
        let json = serde_json::to_string(&delta).unwrap();
        let parsed: Delta = serde_json::from_str(&json).unwrap();
        match parsed {
            Delta::Replace {
                position,
                length,
                text,
            } => {
                assert_eq!(position, 1);
                assert_eq!(length, 4);
                assert_eq!(text, "world");
            }
            other => panic!("expected Replace, got {:?}", other),
        }
    }

    #[test]
    fn delta_deserialize_from_frontend_json() {
        // Simulate the JSON the frontend sends
        let json = r#"{"type":"Insert","position":0,"text":"abc"}"#;
        let delta: Delta = serde_json::from_str(json).unwrap();
        match delta {
            Delta::Insert { position, text } => {
                assert_eq!(position, 0);
                assert_eq!(text, "abc");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    // =========================================================================
    // Delta applied to a standalone YrsCrdt
    // =========================================================================

    fn apply_delta_to_doc(doc: &yrs::Doc, delta: &Delta) {
        use yrs::{Text, Transact};
        let text_ref = doc.get_or_insert_text("doc");
        let mut txn = doc.transact_mut();
        match delta {
            Delta::Insert { position, text } => {
                text_ref.insert(&mut txn, *position, text);
            }
            Delta::Delete { position, length } => {
                text_ref.remove_range(&mut txn, *position, *length);
            }
            Delta::Replace {
                position,
                length,
                text,
            } => {
                text_ref.remove_range(&mut txn, *position, *length);
                text_ref.insert(&mut txn, *position, text);
            }
        }
    }

    fn read_doc_text(doc: &yrs::Doc) -> String {
        use yrs::{GetString, Transact};
        let text_ref = doc.get_or_insert_text("doc");
        let txn = doc.transact();
        text_ref.get_string(&txn)
    }

    #[test]
    fn delta_insert_at_beginning() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hello".into(),
            },
        );
        assert_eq!(read_doc_text(&doc), "hello");
    }

    #[test]
    fn delta_insert_at_middle() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hllo".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 1,
                text: "e".into(),
            },
        );
        assert_eq!(read_doc_text(&doc), "hello");
    }

    #[test]
    fn delta_insert_at_end() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hel".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 3,
                text: "lo".into(),
            },
        );
        assert_eq!(read_doc_text(&doc), "hello");
    }

    #[test]
    fn delta_delete_from_beginning() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hello".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Delete {
                position: 0,
                length: 2,
            },
        );
        assert_eq!(read_doc_text(&doc), "llo");
    }

    #[test]
    fn delta_delete_from_middle() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hello".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Delete {
                position: 1,
                length: 3,
            },
        );
        assert_eq!(read_doc_text(&doc), "ho");
    }

    #[test]
    fn delta_delete_from_end() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hello".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Delete {
                position: 3,
                length: 2,
            },
        );
        assert_eq!(read_doc_text(&doc), "hel");
    }

    #[test]
    fn delta_replace_semantics() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hello".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Replace {
                position: 1,
                length: 3,
                text: "a".into(),
            },
        );
        assert_eq!(read_doc_text(&doc), "hao");
    }

    #[test]
    fn delta_empty_insert_is_noop() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hi".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 1,
                text: "".into(),
            },
        );
        assert_eq!(read_doc_text(&doc), "hi");
    }

    #[test]
    fn delta_delete_zero_length_is_noop() {
        let doc = yrs::Doc::new();
        apply_delta_to_doc(
            &doc,
            &Delta::Insert {
                position: 0,
                text: "hi".into(),
            },
        );
        apply_delta_to_doc(
            &doc,
            &Delta::Delete {
                position: 0,
                length: 0,
            },
        );
        assert_eq!(read_doc_text(&doc), "hi");
    }

    // =========================================================================
    // DocumentInfo serialization
    // =========================================================================

    #[test]
    fn document_info_round_trip() {
        let info = DocumentInfo {
            group_id: "abc123".into(),
            text: "some text".into(),
            member_count: 3,
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: DocumentInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.group_id, "abc123");
        assert_eq!(parsed.text, "some text");
        assert_eq!(parsed.member_count, 3);
    }

    #[test]
    fn document_info_json_shape() {
        let info = DocumentInfo {
            group_id: "g1".into(),
            text: "".into(),
            member_count: 1,
        };
        let json = serde_json::to_string(&info).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v.get("group_id").unwrap().is_string());
        assert!(v.get("text").unwrap().is_string());
        assert!(v.get("member_count").unwrap().is_number());
    }

    // =========================================================================
    // Base58 parsing helpers
    // =========================================================================

    #[test]
    fn base58_valid_round_trip() {
        let data = [1u8, 2, 3, 4, 5];
        let encoded = bs58::encode(&data).into_string();
        let decoded = bs58::decode(&encoded).into_vec().unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn base58_invalid_input() {
        let result = bs58::decode("0OIl").into_vec(); // 0, O, I, l are invalid in base58
        assert!(result.is_err());
    }

    #[test]
    fn base58_empty_input() {
        let decoded = bs58::decode("").into_vec().unwrap();
        assert!(decoded.is_empty());
    }
}
