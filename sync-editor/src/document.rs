//! Per-document actor.
//!
//! Each open document is managed by a [`DocumentActor`] that owns a [`Group`]
//! and runs a `select!` loop over:
//!
//! 1. Incoming [`DocRequest`]s from the coordinator / Tauri commands
//! 2. Remote CRDT updates from peers (via [`Group::wait_for_update`])
//!
//! No mutexes — the actor is the sole owner of the `Group`.

use mls_rs::client_builder::MlsConfig;
use mls_rs::{CipherSuiteProvider, MlsMessage};
use tokio::sync::mpsc;
use universal_sync_core::GroupId;
use universal_sync_proposer::Group;
use universal_sync_testing::YrsCrdt;
use yrs::{GetString, Text, Transact};

use crate::types::{Delta, DocRequest, DocumentUpdatedPayload, EventEmitter, PeerEntry};

/// Actor that manages a single open document / MLS group.
pub struct DocumentActor<C, CS, E>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
    E: EventEmitter,
{
    group: Group<C, CS>,
    group_id_b58: String,
    request_rx: mpsc::Receiver<DocRequest>,
    emitter: E,
}

impl<C, CS, E> DocumentActor<C, CS, E>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
    E: EventEmitter,
{
    /// Create a new document actor.
    pub fn new(
        group: Group<C, CS>,
        group_id: GroupId,
        request_rx: mpsc::Receiver<DocRequest>,
        emitter: E,
    ) -> Self {
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
        Self {
            group,
            group_id_b58,
            request_rx,
            emitter,
        }
    }

    /// Run the actor loop. Returns when the actor is shut down.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                req = self.request_rx.recv() => {
                    match req {
                        Some(req) => {
                            if self.handle_request(req).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                update = self.group.wait_for_update() => {
                    match update {
                        Some(()) => self.emit_text_update(),
                        None => break, // group shut down
                    }
                }
            }
        }

        self.group.shutdown().await;
    }

    /// Handle a single request. Returns `true` if shutdown was requested.
    async fn handle_request(&mut self, request: DocRequest) -> bool {
        match request {
            DocRequest::ApplyDelta { delta, reply } => {
                let result = self.apply_delta(delta).await;
                let _ = reply.send(result);
            }
            DocRequest::GetText { reply } => {
                let _ = reply.send(self.get_text());
            }
            DocRequest::AddMember {
                key_package_b58,
                reply,
            } => {
                let result = self.add_member(&key_package_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::AddAcceptor { addr_b58, reply } => {
                let result = self.add_acceptor(&addr_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::ListAcceptors { reply } => {
                let result = self.list_acceptors().await;
                let _ = reply.send(result);
            }
            DocRequest::ListPeers { reply } => {
                let result = self.list_peers().await;
                let _ = reply.send(result);
            }
            DocRequest::AddPeer { input_b58, reply } => {
                let result = self.add_peer(&input_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::RemoveMember {
                member_index,
                reply,
            } => {
                let result = self.remove_member(member_index).await;
                let _ = reply.send(result);
            }
            DocRequest::RemoveAcceptor {
                acceptor_id_b58,
                reply,
            } => {
                let result = self.remove_acceptor(&acceptor_id_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::Shutdown => return true,
        }
        false
    }

    // =========================================================================
    // CRDT helpers
    // =========================================================================

    fn yrs_crdt(&self) -> Result<&YrsCrdt, String> {
        self.group
            .crdt()
            .as_any()
            .downcast_ref::<YrsCrdt>()
            .ok_or_else(|| "CRDT is not YrsCrdt".to_string())
    }

    fn yrs_crdt_mut(&mut self) -> Result<&mut YrsCrdt, String> {
        self.group
            .crdt_mut()
            .as_any_mut()
            .downcast_mut::<YrsCrdt>()
            .ok_or_else(|| "CRDT is not YrsCrdt".to_string())
    }

    // =========================================================================
    // Document operations
    // =========================================================================

    fn get_text(&self) -> Result<String, String> {
        let yrs = self.yrs_crdt()?;
        let text_ref = yrs.doc().get_or_insert_text("doc");
        let txn = yrs.doc().transact();
        Ok(text_ref.get_string(&txn))
    }

    async fn apply_delta(&mut self, delta: Delta) -> Result<(), String> {
        {
            let yrs = self.yrs_crdt_mut()?;
            let text_ref = yrs.doc().get_or_insert_text("doc");
            let mut txn = yrs.doc().transact_mut();

            // The current length of the CRDT text.  We clamp incoming
            // positions / lengths so that stale or out-of-order deltas
            // (e.g. from frontend debouncing or concurrent remote
            // updates) never ask yrs to remove characters that don't
            // exist — which would otherwise panic.
            let doc_len = text_ref.len(&txn);

            match delta {
                Delta::Insert { position, text } => {
                    let pos = position.min(doc_len);
                    text_ref.insert(&mut txn, pos, &text);
                }
                Delta::Delete { position, length } => {
                    let pos = position.min(doc_len);
                    let len = length.min(doc_len - pos);
                    if len > 0 {
                        text_ref.remove_range(&mut txn, pos, len);
                    }
                }
                Delta::Replace {
                    position,
                    length,
                    text,
                } => {
                    let pos = position.min(doc_len);
                    let len = length.min(doc_len - pos);
                    if len > 0 {
                        text_ref.remove_range(&mut txn, pos, len);
                    }
                    text_ref.insert(&mut txn, pos, &text);
                }
            }
        }
        self.group
            .send_update()
            .await
            .map_err(|e| format!("failed to send update: {e:?}"))
    }

    async fn add_member(&mut self, key_package_b58: &str) -> Result<(), String> {
        let kp_bytes = bs58::decode(key_package_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let key_package = MlsMessage::from_bytes(&kp_bytes)
            .map_err(|e| format!("invalid key package: {e:?}"))?;
        self.group
            .add_member(key_package)
            .await
            .map_err(|e| format!("failed to add member: {e:?}"))
    }

    async fn add_acceptor(&mut self, addr_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(addr_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let addr: iroh::EndpointAddr =
            postcard::from_bytes(&bytes).map_err(|e| format!("invalid address: {e}"))?;
        self.group
            .add_acceptor(addr)
            .await
            .map_err(|e| format!("failed to add acceptor: {e:?}"))
    }

    async fn list_acceptors(&mut self) -> Result<Vec<String>, String> {
        let ctx = self
            .group
            .context()
            .await
            .map_err(|e| format!("failed to get context: {e:?}"))?;
        Ok(ctx
            .acceptors
            .iter()
            .map(|a| bs58::encode(a.as_bytes()).into_string())
            .collect())
    }

    async fn list_peers(&mut self) -> Result<Vec<PeerEntry>, String> {
        let ctx = self
            .group
            .context()
            .await
            .map_err(|e| format!("failed to get context: {e:?}"))?;

        let mut peers = Vec::new();
        for m in &ctx.members {
            let identity = String::from_utf8(m.identity.clone())
                .unwrap_or_else(|_| bs58::encode(&m.identity).into_string());
            peers.push(PeerEntry::Member {
                index: m.index,
                identity,
                is_self: m.is_self,
            });
        }
        for a in &ctx.acceptors {
            peers.push(PeerEntry::Acceptor {
                id: bs58::encode(a.as_bytes()).into_string(),
            });
        }
        Ok(peers)
    }

    async fn add_peer(&mut self, input_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(input_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;

        // Try parsing as a KeyPackage first (add member)
        if let Ok(msg) = MlsMessage::from_bytes(&bytes) {
            if msg.as_key_package().is_some() {
                return self.add_member(input_b58).await;
            }
        }

        // Otherwise try as an EndpointAddr (add acceptor)
        if postcard::from_bytes::<iroh::EndpointAddr>(&bytes).is_ok() {
            return self.add_acceptor(input_b58).await;
        }

        Err("input is neither a valid KeyPackage nor an EndpointAddr".to_string())
    }

    async fn remove_member(&mut self, member_index: u32) -> Result<(), String> {
        self.group
            .remove_member(member_index)
            .await
            .map_err(|e| format!("failed to remove member: {e:?}"))
    }

    async fn remove_acceptor(&mut self, acceptor_id_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(acceptor_id_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let acceptor_id =
            universal_sync_core::AcceptorId::from_bytes(bytes.try_into().map_err(|_| {
                "acceptor ID must be 32 bytes".to_string()
            })?);
        self.group
            .remove_acceptor(acceptor_id)
            .await
            .map_err(|e| format!("failed to remove acceptor: {e:?}"))
    }

    fn emit_text_update(&self) {
        if let Ok(text) = self.get_text() {
            let payload = DocumentUpdatedPayload {
                group_id: self.group_id_b58.clone(),
                text,
            };
            self.emitter.emit_document_updated(&payload);
        }
    }
}
