//! Connection manager for multiplexed iroh connections
//!
//! This module provides [`ConnectionManager`] which caches iroh connections
//! to acceptors and opens multiplexed streams for different purposes.
//!
//! # Architecture
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
//!
//! Each stream is identified by a [`Handshake`] message sent at the start.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{FutureExt, SinkExt, StreamExt};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio::sync::RwLock;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use universal_sync_core::{AcceptorId, GroupId, Handshake, HandshakeResponse};

use crate::connector::{ConnectorError, PAXOS_ALPN};

/// Manages connections to acceptors with stream multiplexing.
///
/// A single `ConnectionManager` should be shared across all groups
/// to maximize connection reuse.
#[derive(Clone)]
pub struct ConnectionManager {
    endpoint: Endpoint,
    /// Cached connections by acceptor ID
    connections: Arc<RwLock<HashMap<AcceptorId, Connection>>>,
    /// Address hints for acceptors (useful when discovery is not available)
    address_hints: Arc<RwLock<HashMap<AcceptorId, EndpointAddr>>>,
}

impl ConnectionManager {
    /// Create a new connection manager.
    ///
    /// # Arguments
    /// * `endpoint` - The iroh endpoint to use for connections
    #[must_use]
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(RwLock::new(HashMap::new())),
            address_hints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the underlying iroh endpoint.
    #[must_use]
    pub(crate) fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Add an address hint for an acceptor.
    ///
    /// Use this when iroh discovery is not available and you have
    /// the full endpoint address for an acceptor.
    pub(crate) async fn add_address_hint(&self, acceptor_id: AcceptorId, addr: EndpointAddr) {
        self.address_hints.write().await.insert(acceptor_id, addr);
    }

    /// Remove an address hint for an acceptor.
    pub(crate) async fn remove_address_hint(&self, acceptor_id: &AcceptorId) {
        self.address_hints.write().await.remove(acceptor_id);
    }

    /// Get or create a connection to an acceptor.
    ///
    /// Returns an existing cached connection if available and still alive,
    /// otherwise creates a new connection.
    ///
    /// # Errors
    /// Returns an error if connection fails.
    async fn get_connection(&self, acceptor_id: &AcceptorId) -> Result<Connection, ConnectorError> {
        // Check for existing connection
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(acceptor_id) {
                // Check if connection is still alive
                if conn.closed().now_or_never().is_none() {
                    return Ok(conn.clone());
                }
            }
        }

        // Need to create a new connection
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

        // Cache the connection
        self.connections
            .write()
            .await
            .insert(*acceptor_id, conn.clone());

        Ok(conn)
    }

    /// Open a proposal stream for Paxos consensus.
    ///
    /// This opens a bidirectional stream and sends a `JoinProposals` handshake.
    ///
    /// # Errors
    /// Returns an error if connection or handshake fails.
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

    /// Open a message stream for application messages.
    ///
    /// This opens a bidirectional stream and sends a `JoinMessages` handshake.
    /// Note: This creates a new connection because the server handles only
    /// one stream per connection (due to `AcceptorStateStore` not being Send).
    ///
    /// # Errors
    /// Returns an error if connection or handshake fails.
    pub(crate) async fn open_message_stream(
        &self,
        acceptor_id: &AcceptorId,
        group_id: GroupId,
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        // Create a new connection for message stream
        // (server handles one stream per connection)
        let conn = self.new_connection(acceptor_id).await?;
        let (send, recv) = self
            .open_stream_with_handshake(&conn, Handshake::JoinMessages(group_id))
            .await?;
        Ok((send, recv))
    }

    /// Create a new connection to an acceptor (not cached).
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

    /// Register a new group with an acceptor.
    ///
    /// This opens a bidirectional stream and sends a `CreateGroup` handshake.
    ///
    /// # Errors
    /// Returns an error if connection or handshake fails.
    pub(crate) async fn register_group(
        &self,
        acceptor_id: &AcceptorId,
        group_info: &[u8],
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        let conn = self.get_connection(acceptor_id).await?;
        let (send, recv) = self
            .open_stream_with_handshake(&conn, Handshake::CreateGroup(group_info.to_vec()))
            .await?;
        Ok((send, recv))
    }

    /// Register a new group with an acceptor using a full endpoint address.
    ///
    /// Like [`Self::register_group`] but accepts an [`iroh::EndpointAddr`]
    /// for local testing where discovery may not be available.
    ///
    /// # Errors
    /// Returns an error if connection or handshake fails.
    pub(crate) async fn register_group_with_addr(
        &self,
        addr: impl Into<EndpointAddr>,
        group_info: &[u8],
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        let addr = addr.into();

        // Connect directly without caching (we don't have an AcceptorId yet)
        let conn = self
            .endpoint
            .connect(addr.clone(), PAXOS_ALPN)
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))?;

        let (send, recv) = self
            .open_stream_with_handshake(&conn, Handshake::CreateGroup(group_info.to_vec()))
            .await?;

        // Cache the connection by the remote's public key
        let acceptor_id = AcceptorId::from_bytes(*conn.remote_id().as_bytes());
        self.connections.write().await.insert(acceptor_id, conn);
        self.address_hints.write().await.insert(acceptor_id, addr);

        Ok((send, recv))
    }

    /// Open a stream with a handshake.
    ///
    /// Returns the raw streams after handshake completion.
    async fn open_stream_with_handshake(
        &self,
        conn: &Connection,
        handshake: Handshake,
    ) -> Result<(SendStream, RecvStream), ConnectorError> {
        // Open a bidirectional stream
        let (send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))?;

        // Create framed reader/writer for handshake
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(16 * 1024 * 1024)
            .new_codec();
        let mut reader = FramedRead::new(recv, codec.clone());
        let mut writer = FramedWrite::new(send, codec);

        // Send the handshake
        let handshake_bytes =
            postcard::to_allocvec(&handshake).map_err(|e| ConnectorError::Codec(e.to_string()))?;
        writer
            .send(handshake_bytes.into())
            .await
            .map_err(ConnectorError::Io)?;

        // Read the response
        let response_bytes = reader
            .next()
            .await
            .ok_or_else(|| {
                ConnectorError::Handshake("connection closed before response".to_string())
            })?
            .map_err(ConnectorError::Io)?;

        let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
            .map_err(|e| ConnectorError::Codec(format!("invalid response: {e}")))?;

        // Check response
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

        // Extract inner streams
        let recv = reader.into_inner();
        let send = writer.into_inner();

        Ok((send, recv))
    }

    /// Close and remove a cached connection to an acceptor.
    pub(crate) async fn close_connection(&self, acceptor_id: &AcceptorId) {
        if let Some(conn) = self.connections.write().await.remove(acceptor_id) {
            conn.close(0u8.into(), b"closed");
        }
    }

    /// Close all cached connections.
    pub(crate) async fn close_all(&self) {
        let connections: Vec<_> = self.connections.write().await.drain().collect();
        for (_, conn) in connections {
            conn.close(0u8.into(), b"closed");
        }
    }
}
