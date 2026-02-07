//! Request/response types for the editor actor system.

use serde::{Deserialize, Serialize};
use tauri::AppHandle;
use tokio::sync::{mpsc, oneshot};
use universal_sync_core::GroupId;

/// No mutexes — only an `mpsc::Sender` to the coordinator.
pub struct AppState {
    pub coordinator_tx: mpsc::Sender<CoordinatorRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Delta {
    Insert {
        position: u32,
        text: String,
    },
    Delete {
        position: u32,
        length: u32,
    },
    Replace {
        position: u32,
        length: u32,
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentInfo {
    pub group_id: String,
    pub text: String,
    pub member_count: usize,
}

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

#[derive(Debug, Clone, Serialize)]
pub struct DocumentUpdatedPayload {
    pub group_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupStatePayload {
    pub group_id: String,
    pub epoch: u64,
    pub transcript_hash: String,
    pub member_count: usize,
    pub acceptor_count: usize,
    pub connected_acceptor_count: usize,
}

/// Abstracts event emission so actors can be tested without Tauri.
pub trait EventEmitter: Clone + Send + 'static {
    fn emit_document_updated(&self, payload: &DocumentUpdatedPayload);
    fn emit_group_state_changed(&self, payload: &GroupStatePayload);
}

impl EventEmitter for AppHandle {
    fn emit_document_updated(&self, payload: &DocumentUpdatedPayload) {
        use tauri::Emitter;
        let _ = self.emit("document-updated", payload);
    }
    fn emit_group_state_changed(&self, payload: &GroupStatePayload) {
        use tauri::Emitter;
        let _ = self.emit("group-state-changed", payload);
    }
}

pub enum CoordinatorRequest {
    CreateDocument {
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    GetKeyPackage {
        reply: oneshot::Sender<Result<String, String>>,
    },
    RecvWelcome {
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    JoinDocumentBytes {
        welcome_b58: String,
        reply: oneshot::Sender<Result<DocumentInfo, String>>,
    },
    ForDoc {
        group_id: GroupId,
        request: DocRequest,
    },
}

pub enum DocRequest {
    ApplyDelta {
        delta: Delta,
        reply: oneshot::Sender<Result<(), String>>,
    },
    GetText {
        reply: oneshot::Sender<Result<String, String>>,
    },
    AddMember {
        key_package_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    AddAcceptor {
        addr_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    ListAcceptors {
        reply: oneshot::Sender<Result<Vec<String>, String>>,
    },
    ListPeers {
        reply: oneshot::Sender<Result<Vec<PeerEntry>, String>>,
    },
    /// Auto-detect KeyPackage (member) vs EndpointAddr (acceptor).
    AddPeer {
        input_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    RemoveMember {
        member_index: u32,
        reply: oneshot::Sender<Result<(), String>>,
    },
    RemoveAcceptor {
        acceptor_id_b58: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    GetGroupState {
        reply: oneshot::Sender<Result<GroupStatePayload, String>>,
    },
    UpdateKeys {
        reply: oneshot::Sender<Result<(), String>>,
    },
    #[allow(dead_code)]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn base58_valid_round_trip() {
        let data = [1u8, 2, 3, 4, 5];
        let encoded = bs58::encode(&data).into_string();
        let decoded = bs58::decode(&encoded).into_vec().unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn base58_invalid_input() {
        let result = bs58::decode("0OIl").into_vec();
        assert!(result.is_err());
    }

    #[test]
    fn base58_empty_input() {
        let decoded = bs58::decode("").into_vec().unwrap();
        assert!(decoded.is_empty());
    }
}
