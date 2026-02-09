//! Per-document actor.
//!
//! Each open document is managed by a [`DocumentActor`] that owns a [`Weaver`]
//! and a [`YrsCrdt`] separately — no mutexes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use filament_core::GroupId;
use filament_weave::{Weaver, WeaverEvent};
use tokio::sync::{broadcast, mpsc};
use yrs::{Any, GetString, Observable, Out, Text, Transact};

use crate::types::{
    AwarenessPayload, AwarenessPeer, Delta, DocRequest, DocumentUpdatedPayload, EventEmitter,
    GroupStatePayload, PeerEntry,
};
use crate::yrs_crdt::YrsCrdt;

pub struct DocumentActor<E: EventEmitter> {
    group: Weaver,
    crdt: YrsCrdt,
    group_id_b58: String,
    request_rx: mpsc::Receiver<DocRequest>,
    event_rx: broadcast::Receiver<WeaverEvent>,
    emitter: E,
}

const AWARENESS_HEARTBEAT: Duration = Duration::from_secs(5);
const AWARENESS_TIMEOUT: Duration = Duration::from_secs(10);

impl<E: EventEmitter> DocumentActor<E> {
    pub fn new(
        group: Weaver,
        crdt: YrsCrdt,
        group_id: GroupId,
        request_rx: mpsc::Receiver<DocRequest>,
        emitter: E,
    ) -> Self {
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
        let event_rx = group.subscribe();
        Self {
            group,
            crdt,
            group_id_b58,
            request_rx,
            event_rx,
            emitter,
        }
    }

    pub async fn run(mut self) {
        self.emit_group_state().await;
        self.crdt.set_cursor(0, 0);

        let delta_buf: Arc<Mutex<Vec<Delta>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let text_ref = self.crdt.doc().get_or_insert_text("doc");
            let buf_clone = delta_buf.clone();
            text_ref.observe_with("delta-collector", move |txn, event| {
                let mut buf = buf_clone.lock().unwrap();
                let mut offset = 0u32;
                for d in event.delta(txn) {
                    match d {
                        yrs::types::Delta::Retain(n, _) => offset += n,
                        yrs::types::Delta::Inserted(value, _) => {
                            if let Out::Any(Any::String(s)) = value {
                                let text = s.to_string();
                                let len = text.len() as u32;
                                buf.push(Delta::Insert {
                                    position: offset,
                                    text,
                                });
                                offset += len;
                            }
                        }
                        yrs::types::Delta::Deleted(n) => {
                            buf.push(Delta::Delete {
                                position: offset,
                                length: *n,
                            });
                        }
                    }
                }
            });
        }

        let mut heartbeat = tokio::time::interval(AWARENESS_HEARTBEAT);
        heartbeat.tick().await; // consume immediate first tick

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
                    // Discard deltas produced by local edits
                    delta_buf.lock().unwrap().clear();
                }
                update = self.group.wait_for_update(&mut self.crdt) => {
                    match update {
                        Some(()) => {
                            self.emit_text_update(&delta_buf);
                            self.emit_awareness();
                        }
                        None => break,
                    }
                }
                event = self.event_rx.recv() => {
                    match event {
                        Ok(WeaverEvent::CompactionNeeded { level, force }) => {
                            let result = if force {
                                self.group.force_compact(&mut self.crdt, level).await
                            } else {
                                self.group.compact(&mut self.crdt, level).await
                            };
                            if let Err(e) = result {
                                tracing::warn!(?e, level, force, "compaction failed");
                            }
                        }
                        Ok(_group_event) => {
                            self.emit_group_state().await;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::debug!(skipped = n, "group event receiver lagged");
                            self.emit_group_state().await;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = heartbeat.tick() => {
                    self.crdt.set_cursor(0, 0);
                    if let Err(e) = self.group.send_update(&mut self.crdt).await {
                        tracing::debug!(?e, "heartbeat send failed");
                    }
                    if self.crdt.expire_stale_peers(AWARENESS_TIMEOUT) {
                        self.emit_awareness();
                    }
                }
            }
        }

        self.crdt.clear_local_state();
        let _ = self.group.send_update(&mut self.crdt).await;
        self.group.shutdown().await;
    }

    async fn handle_request(&mut self, request: DocRequest) -> bool {
        match request {
            DocRequest::ApplyDelta {
                delta,
                anchor,
                head,
                reply,
            } => {
                self.crdt.set_cursor(anchor, head);
                let result = self.apply_delta(delta).await;
                let _ = reply.send(result);
                self.emit_awareness();
            }
            DocRequest::GetText { reply } => {
                let _ = reply.send(Ok(self.get_text()));
            }
            DocRequest::AddMember {
                key_package_b58,
                reply,
            } => {
                let result = self.add_member(&key_package_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::AddSpool { addr_b58, reply } => {
                let result = self.add_spool(&addr_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::ListSpools { reply } => {
                let result = self.list_spools().await;
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
            DocRequest::RemoveSpool {
                spool_id_b58,
                reply,
            } => {
                let result = self.remove_spool(&spool_id_b58).await;
                let _ = reply.send(result);
            }
            DocRequest::GetGroupState { reply } => {
                let result = self.get_group_state().await;
                let _ = reply.send(result);
            }
            DocRequest::UpdateKeys { reply } => {
                let result = self.update_keys().await;
                let _ = reply.send(result);
            }
            DocRequest::UpdateCursor { anchor, head } => {
                self.crdt.set_cursor(anchor, head);
                if let Err(e) = self.group.send_update(&mut self.crdt).await {
                    tracing::debug!(?e, "cursor update send failed");
                }
            }
            DocRequest::Shutdown => return true,
        }
        false
    }

    fn get_text(&self) -> String {
        let text_ref = self.crdt.doc().get_or_insert_text("doc");
        let txn = self.crdt.doc().transact();
        text_ref.get_string(&txn)
    }

    async fn apply_delta(&mut self, delta: Delta) -> Result<(), String> {
        {
            let text_ref = self.crdt.doc().get_or_insert_text("doc");
            let mut txn = self.crdt.doc().transact_mut();

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
            .send_update(&mut self.crdt)
            .await
            .map_err(|e| format!("failed to send update: {e:?}"))
    }

    async fn add_member(&mut self, key_package_b58: &str) -> Result<(), String> {
        let kp_bytes = bs58::decode(key_package_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        self.group
            .add_member(&kp_bytes)
            .await
            .map_err(|e| format!("failed to add member: {e:?}"))
    }

    async fn add_spool(&mut self, addr_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(addr_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "spool address must be a 32-byte public key".to_string())?;
        let id = filament_core::AcceptorId::from_bytes(key_bytes);
        self.group
            .add_spool(id)
            .await
            .map_err(|e| format!("failed to add spool: {e:?}"))
    }

    async fn list_spools(&mut self) -> Result<Vec<String>, String> {
        let ctx = self
            .group
            .context()
            .await
            .map_err(|e| format!("failed to get context: {e:?}"))?;
        Ok(ctx
            .spools
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
                client_id: m.client_id.0,
            });
        }
        for a in &ctx.spools {
            peers.push(PeerEntry::Spool {
                id: bs58::encode(a.as_bytes()).into_string(),
            });
        }
        Ok(peers)
    }

    async fn add_peer(&mut self, input_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(input_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;

        if bytes.len() == 32 {
            return self.add_spool(input_b58).await;
        }

        if let Ok(msg) = mls_rs::MlsMessage::from_bytes(&bytes)
            && msg.as_key_package().is_some()
        {
            return self.add_member(input_b58).await;
        }

        Err("input is neither a 32-byte spool public key nor a valid KeyPackage".to_string())
    }

    async fn remove_member(&mut self, member_index: u32) -> Result<(), String> {
        self.group
            .remove_member(member_index)
            .await
            .map_err(|e| format!("failed to remove member: {e:?}"))
    }

    async fn remove_spool(&mut self, spool_id_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(spool_id_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let spool_id = filament_core::AcceptorId::from_bytes(
            bytes
                .try_into()
                .map_err(|_| "spool ID must be 32 bytes".to_string())?,
        );
        self.group
            .remove_spool(spool_id)
            .await
            .map_err(|e| format!("failed to remove spool: {e:?}"))
    }

    async fn get_group_state(&mut self) -> Result<GroupStatePayload, String> {
        let ctx = self
            .group
            .context()
            .await
            .map_err(|e| format!("failed to get context: {e:?}"))?;
        Ok(GroupStatePayload {
            group_id: self.group_id_b58.clone(),
            epoch: ctx.epoch.0,
            transcript_hash: hex::encode(&ctx.confirmed_transcript_hash),
            member_count: ctx.member_count,
            spool_count: ctx.spools.len(),
            connected_spool_count: ctx.connected_spools.len(),
        })
    }

    async fn update_keys(&mut self) -> Result<(), String> {
        self.group
            .update_keys()
            .await
            .map_err(|e| format!("failed to update keys: {e:?}"))
    }

    fn emit_text_update(&self, delta_buf: &Arc<Mutex<Vec<Delta>>>) {
        let deltas = std::mem::take(&mut *delta_buf.lock().unwrap());
        let payload = DocumentUpdatedPayload {
            group_id: self.group_id_b58.clone(),
            text: self.get_text(),
            deltas,
        };
        self.emitter.emit_document_updated(&payload);
    }

    async fn emit_group_state(&mut self) {
        match self.get_group_state().await {
            Ok(payload) => self.emitter.emit_group_state_changed(&payload),
            Err(e) => tracing::warn!(?e, "failed to emit group state"),
        }
    }

    fn emit_awareness(&self) {
        let peers: Vec<AwarenessPeer> = self
            .crdt
            .awareness_states()
            .values()
            .map(|pa| AwarenessPeer {
                client_id: pa.client_id,
                cursor: pa.cursor,
                selection_end: pa.selection_end,
            })
            .collect();
        let payload = AwarenessPayload {
            group_id: self.group_id_b58.clone(),
            peers,
        };
        self.emitter.emit_awareness_changed(&payload);
    }
}
