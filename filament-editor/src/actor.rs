//! Coordinator actor.
//!
//! Owns the [`WeaverClient`] and routes requests to per-document actors.
//! Spawns a [`DocumentActor`](crate::document::DocumentActor) when a
//! group is created or joined.

use std::collections::HashMap;

use filament_core::GroupId;
use filament_weave::{Weaver, WeaverClient};
use mls_rs::MlsMessage;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};
use yrs::Transact;

use crate::document::DocumentActor;
use crate::types::{CoordinatorRequest, DocRequest, DocumentInfo, EventEmitter};
use crate::yrs_crdt::YrsCrdt;

pub struct CoordinatorActor<E: EventEmitter> {
    group_client: WeaverClient,
    welcome_rx: mpsc::Receiver<Box<MlsMessage>>,
    doc_actors: HashMap<GroupId, mpsc::Sender<DocRequest>>,
    request_rx: mpsc::Receiver<CoordinatorRequest>,
    pending_welcome_reply: Option<oneshot::Sender<Result<DocumentInfo, String>>>,
    emitter: E,
}

impl<E: EventEmitter> CoordinatorActor<E> {
    pub fn new(
        mut group_client: WeaverClient,
        request_rx: mpsc::Receiver<CoordinatorRequest>,
        emitter: E,
    ) -> Self {
        let welcome_rx = group_client.take_welcome_rx();
        Self {
            group_client,
            welcome_rx,
            doc_actors: HashMap::new(),
            request_rx,
            pending_welcome_reply: None,
            emitter,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                req = self.request_rx.recv() => {
                    match req {
                        Some(req) => self.handle_request(req).await,
                        None => break,
                    }
                }
                Some(welcome) = self.welcome_rx.recv() => {
                    self.handle_welcome_received(&welcome).await;
                }
            }
        }

        info!("coordinator shutting down");
    }

    async fn handle_request(&mut self, request: CoordinatorRequest) {
        match request {
            CoordinatorRequest::CreateDocument { reply } => {
                let result = self.create_document().await;
                let _ = reply.send(result);
            }
            CoordinatorRequest::GetKeyPackage { reply } => {
                let _ = reply.send(self.get_key_package());
            }
            CoordinatorRequest::RecvWelcome { reply } => {
                self.pending_welcome_reply = Some(reply);
            }
            CoordinatorRequest::JoinDocumentBytes { welcome_b58, reply } => {
                let result = self.join_document_bytes(&welcome_b58).await;
                let _ = reply.send(result);
            }
            CoordinatorRequest::JoinExternal { invite_b58, reply } => {
                let result = self.send_key_package_from_invite(&invite_b58).await;
                let _ = reply.send(result);
            }
            CoordinatorRequest::ForDoc { group_id, request } => {
                self.route_to_doc(group_id, request).await;
            }
        }
    }

    async fn create_document(&mut self) -> Result<DocumentInfo, String> {
        let group = self
            .group_client
            .create(&[], "yrs")
            .await
            .map_err(|e| format!("failed to create group: {e:?}"))?;

        let crdt = YrsCrdt::with_client_id(group.client_id().0);
        self.register_document(group, crdt)
    }

    fn get_key_package(&self) -> Result<String, String> {
        let bytes = self
            .group_client
            .generate_key_package()
            .map_err(|e| format!("failed to generate key package: {e:?}"))?;
        Ok(bs58::encode(bytes).into_string())
    }

    async fn join_document_bytes(&mut self, welcome_b58: &str) -> Result<DocumentInfo, String> {
        let welcome_bytes = bs58::decode(welcome_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        let welcome = MlsMessage::from_bytes(&welcome_bytes)
            .map_err(|e| format!("invalid welcome message: {e}"))?;
        self.join_with_welcome(&welcome).await
    }

    async fn send_key_package_from_invite(&self, invite_b58: &str) -> Result<(), String> {
        let bytes = bs58::decode(invite_b58)
            .into_vec()
            .map_err(|e| format!("invalid base58: {e}"))?;
        if bytes.len() != 96 {
            return Err(format!("invite must be 96 bytes, got {}", bytes.len()));
        }
        let target: [u8; 32] = bytes[..32].try_into().unwrap();
        let group_id = GroupId::from_slice(&bytes[32..64]);
        let hmac_tag: [u8; 32] = bytes[64..96].try_into().unwrap();
        let target_b58 = bs58::encode(&target).into_string();
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();
        debug!(%target_b58, %group_id_b58, "sending key package to target");
        self.group_client
            .send_key_package(target, group_id, hmac_tag)
            .await
            .map_err(|e| format!("failed to send key package: {e:?}"))?;
        debug!(%target_b58, %group_id_b58, "key package sent, waiting for welcome");
        Ok(())
    }

    async fn handle_welcome_received(&mut self, welcome: &MlsMessage) {
        debug!("received welcome message");
        match self.join_with_welcome(welcome).await {
            Ok(doc_info) => {
                if let Some(reply) = self.pending_welcome_reply.take() {
                    let _ = reply.send(Ok(doc_info));
                }
            }
            Err(e) => {
                warn!(?e, "failed to join from welcome");
                if let Some(reply) = self.pending_welcome_reply.take() {
                    let _ = reply.send(Err(e));
                }
            }
        }
    }

    async fn join_with_welcome(&mut self, welcome: &MlsMessage) -> Result<DocumentInfo, String> {
        let join_info = self
            .group_client
            .join(welcome)
            .await
            .map_err(|e| format!("failed to join group: {e:?}"))?;

        let client_id = join_info.group.client_id().0;
        let crdt = YrsCrdt::with_client_id(client_id);

        self.register_document(join_info.group, crdt)
    }

    fn register_document(&mut self, group: Weaver, crdt: YrsCrdt) -> Result<DocumentInfo, String> {
        let group_id = group.group_id();
        let group_id_b58 = bs58::encode(group_id.as_bytes()).into_string();

        let text = {
            let text_ref = crdt.doc().get_or_insert_text("doc");
            let txn = crdt.doc().transact();
            yrs::GetString::get_string(&text_ref, &txn)
        };

        let (doc_tx, doc_rx) = mpsc::channel(64);
        let actor = DocumentActor::new(group, crdt, group_id, doc_rx, self.emitter.clone());
        tokio::spawn(actor.run());

        self.doc_actors.insert(group_id, doc_tx);

        info!(%group_id_b58, "document actor spawned");

        Ok(DocumentInfo {
            group_id: group_id_b58,
            text,
            member_count: 1,
        })
    }

    async fn route_to_doc(&self, group_id: GroupId, request: DocRequest) {
        if let Some(tx) = self.doc_actors.get(&group_id)
            && tx.send(request).await.is_err()
        {
            warn!("document actor closed for group");
        }
    }
}
