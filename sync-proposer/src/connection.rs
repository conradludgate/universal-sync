//! Connection cache with multiplexed streams per acceptor.
//!
//! ```text
//! ConnectionManager
//! └── Per-Acceptor Connection Cache
//!     └── Connection (to acceptor A)
//!         ├── Stream: Group X - Proposals (Paxos)
//!         ├── Stream: Group X - Messages
//!         ├── Stream: Group Y - Proposals (Paxos)
//!         └── Stream: Group Y - Messages
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use futures::{FutureExt, SinkExt, StreamExt};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio::sync::RwLock;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use universal_sync_core::{AcceptorId, GroupId, Handshake, HandshakeResponse, PAXOS_ALPN};

use crate::connector::ConnectorError;

/// Manages connections to acceptors with stream multiplexing.
/// Share across groups to maximize connection reuse.
#[derive(Clone)]
pub struct ConnectionManager {
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<AcceptorId, Connection>>>,
    address_hints: Arc<RwLock<HashMap<AcceptorId, EndpointAddr>>>,
}

impl ConnectionManager {
    #[must_use]
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            address_hints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[must_use]
    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Add an address hint for when iroh discovery is not available.
    pub(crate) async fn add_address_hint(&self, acceptor_id: AcceptorId, addr: EndpointAddr) {
        self.address_hints.write().await.insert(acceptor_id, addr);
    }

    async fn get_connection(&self, acceptor_id: &AcceptorId) -> Result<Connection, ConnectorError> {
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(acceptor_id) {
                if conn.closed().now_or_never().is_none() {
                    return Ok(conn.clone());
                }
            }
        }

        let addr = self
            .address_hints
            .read()
            .await
            .get(acceptor_id)
            .cloned()
            .unwrap_or_else(|| {
                let public_key = PublicKey::from_bytes(acceptor_id.as_bytes())
                    .expect("AcceptorId should be a valid public key");
                public_key.into()
            });

        let conn = self
            .endpoint
            .connect(addr, PAXOS_ALPN)
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))?;

        self.connections
            .write()
            .await
            .insert(*acceptor_id, conn.clone());

        Ok(conn)
    }

    pub(crate) async fn open_proposal_stream(
        &self,
        acceptor_id: &AcceptorId,
        group_id: GroupId,
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        let conn = self.get_connection(acceptor_id).await?;
        let (send, recv) = self
            .open_stream_with_handshake(&conn, Handshake::JoinProposals(group_id))
            .await?;
        Ok((send, recv))
    }

    /// Creates a new (uncached) connection because the server handles one stream per connection.
    pub(crate) async fn open_message_stream(
        &self,
        acceptor_id: &AcceptorId,
        group_id: GroupId,
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        let conn = self.new_connection(acceptor_id).await?;
        let (send, recv) = self
            .open_stream_with_handshake(&conn, Handshake::JoinMessages(group_id))
            .await?;
        Ok((send, recv))
    }

    async fn new_connection(&self, acceptor_id: &AcceptorId) -> Result<Connection, ConnectorError> {
        let addr = self
            .address_hints
            .read()
            .await
            .get(acceptor_id)
            .cloned()
            .unwrap_or_else(|| {
                let public_key = PublicKey::from_bytes(acceptor_id.as_bytes())
                    .expect("AcceptorId should be a valid public key");
                public_key.into()
            });

        self.endpoint
            .connect(addr, PAXOS_ALPN)
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))
    }

    async fn open_stream_with_handshake(
        &self,
        conn: &Connection,
        handshake: Handshake,
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))?;

        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(16 * 1024 * 1024)
            .new_codec();
        let mut reader = FramedRead::new(recv, codec.clone());
        let mut writer = FramedWrite::new(send, codec);

        let handshake_bytes =
            postcard::to_allocvec(&handshake).map_err(|e| ConnectorError::Codec(e.to_string()))?;
        writer
            .send(handshake_bytes.into())
            .await
            .map_err(ConnectorError::Io)?;

        let response_bytes = reader
            .next()
            .await
            .ok_or_else(|| {
                ConnectorError::Handshake("connection closed before response".to_string())
            })?
            .map_err(ConnectorError::Io)?;

        let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
            .map_err(|e| ConnectorError::Codec(format!("invalid response: {e}")))?;

        match response {
            HandshakeResponse::Ok => {}
            HandshakeResponse::GroupNotFound => {
                return Err(ConnectorError::Handshake("group not found".to_string()));
            }
            HandshakeResponse::InvalidGroupInfo(e) => {
                return Err(ConnectorError::Handshake(format!(
                    "invalid group info: {e}"
                )));
            }
            HandshakeResponse::Error(e) => {
                return Err(ConnectorError::Handshake(e));
            }
        }

        let recv = reader.into_inner();
        let send = writer.into_inner();

        Ok((send, recv))
    }
}
