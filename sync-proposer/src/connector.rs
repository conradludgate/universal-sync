//! Iroh-based connector for Paxos acceptors
//!
//! This module provides a [`Connector`] implementation using iroh for
//! p2p QUIC connections to acceptors.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, Stream, StreamExt};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use pin_project_lite::pin_project;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use universal_sync_core::{
    AcceptorId, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse,
};
use universal_sync_paxos::{AcceptorMessage, AcceptorRequest, Connector, Learner};

/// ALPN protocol identifier for Paxos connections
pub const PAXOS_ALPN: &[u8] = b"universal-sync/paxos/1";

/// Error type for iroh connector operations
#[derive(Debug)]
pub enum ConnectorError {
    /// Connection failed
    Connect(String),
    /// Serialization/deserialization error
    Codec(String),
    /// IO error
    Io(std::io::Error),
    /// Handshake failed
    Handshake(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorError::Connect(e) => write!(f, "connection error: {e}"),
            ConnectorError::Codec(e) => write!(f, "codec error: {e}"),
            ConnectorError::Io(e) => write!(f, "io error: {e}"),
            ConnectorError::Handshake(e) => write!(f, "handshake error: {e}"),
        }
    }
}

impl std::error::Error for ConnectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectorError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConnectorError {
    fn from(e: std::io::Error) -> Self {
        ConnectorError::Io(e)
    }
}

/// Iroh-based connector for Paxos acceptors
///
/// Connects to acceptors using iroh's p2p QUIC connections.
/// Each acceptor is identified by its iroh public key ([`AcceptorId`]).
///
/// The connector performs a Join handshake when connecting to join an
/// existing group. For registering new groups with acceptors, use
/// [`register_group`] instead.
///
/// Generic over any [`Learner`] that uses:
/// - `Proposal = GroupProposal`
/// - `Message = GroupMessage`
/// - `AcceptorId = AcceptorId`
/// - `Error: From<ConnectorError>`
pub struct IrohConnector<L> {
    endpoint: Endpoint,
    group_id: GroupId,
    /// Optional address hints for acceptors (useful when discovery is not available)
    address_hints: Arc<HashMap<AcceptorId, EndpointAddr>>,
    _marker: PhantomData<fn() -> L>,
}

impl<L> Clone for IrohConnector<L> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            group_id: self.group_id,
            address_hints: self.address_hints.clone(),
            _marker: PhantomData,
        }
    }
}

impl<L> IrohConnector<L> {
    /// Create a new connector for joining an existing group
    ///
    /// # Arguments
    /// * `endpoint` - The iroh endpoint to use for connections
    /// * `group_id` - The group to join
    #[must_use]
    pub fn new(endpoint: Endpoint, group_id: GroupId) -> Self {
        Self {
            endpoint,
            group_id,
            address_hints: Arc::new(HashMap::new()),
            _marker: PhantomData,
        }
    }

    // /// Create a new connector with address hints
    // ///
    // /// Use this when iroh discovery is not available and you have
    // /// the full endpoint addresses for acceptors.
    // ///
    // /// # Arguments
    // /// * `endpoint` - The iroh endpoint to use for connections
    // /// * `group_id` - The group to join
    // /// * `address_hints` - Map of acceptor IDs to their endpoint addresses
    // pub fn with_address_hints(
    //     endpoint: Endpoint,
    //     group_id: GroupId,
    //     address_hints: impl IntoIterator<Item = (AcceptorId, EndpointAddr)>,
    // ) -> Self {
    //     Self {
    //         endpoint,
    //         group_id,
    //         address_hints: Arc::new(address_hints.into_iter().collect()),
    //         _marker: PhantomData,
    //     }
    // }

    /// Get the underlying iroh endpoint
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Get the group ID
    #[must_use]
    pub fn group_id(&self) -> &GroupId {
        &self.group_id
    }
}

impl<L> Connector<L> for IrohConnector<L>
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage, AcceptorId = AcceptorId>,
    L::Error: From<ConnectorError>,
{
    type Connection = IrohConnection<L>;
    type Error = ConnectorError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send>>;

    fn connect(&mut self, acceptor_id: &AcceptorId) -> Self::ConnectFuture {
        let endpoint = self.endpoint.clone();
        let group_id = self.group_id;

        // Use address hint if available, otherwise just the public key
        let addr: EndpointAddr =
            self.address_hints
                .get(acceptor_id)
                .cloned()
                .unwrap_or_else(|| {
                    let public_key = PublicKey::from_bytes(acceptor_id.as_bytes())
                        .expect("AcceptorId should be a valid public key");
                    public_key.into()
                });

        Box::pin(async move {
            // Connect to the acceptor
            let conn = endpoint
                .connect(addr, PAXOS_ALPN)
                .await
                .map_err(|e| ConnectorError::Connect(e.to_string()))?;

            // Open a bidirectional stream for the Paxos protocol
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

            // Send the Join handshake
            let handshake = Handshake::JoinProposals(group_id);
            let handshake_bytes = postcard::to_allocvec(&handshake)
                .map_err(|e| ConnectorError::Codec(e.to_string()))?;
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

            // Extract inner streams and create connection
            let recv = reader.into_inner();
            let send = writer.into_inner();

            Ok(IrohConnection::new(send, recv))
        })
    }
}

/// Register a new group with an acceptor
///
/// This sends a Create handshake to register the group with the acceptor.
/// Use this when first setting up a group before using [`IrohConnector`]
/// for ongoing Paxos connections.
///
/// # Arguments
/// * `endpoint` - The iroh endpoint to use
/// * `acceptor_id` - The acceptor to register with
/// * `group_info` - The MLS `GroupInfo` message bytes
///
/// # Returns
/// The [`GroupId`] assigned to the group on success.
///
/// # Errors
/// Returns an error if the connection fails or the handshake is rejected.
///
/// # Panics
/// Panics if the `AcceptorId` is not a valid public key.
pub async fn register_group(
    endpoint: &Endpoint,
    acceptor_id: &AcceptorId,
    group_info: &[u8],
) -> Result<GroupId, ConnectorError> {
    let public_key = PublicKey::from_bytes(acceptor_id.as_bytes())
        .expect("AcceptorId should be a valid public key");

    register_group_with_addr(endpoint, public_key, group_info).await
}

/// Register a new group with an acceptor using a full endpoint address
///
/// Like [`register_group`] but accepts an [`iroh::EndpointAddr`] for local testing
/// where discovery may not be available.
///
/// # Arguments
/// * `endpoint` - The iroh endpoint to use
/// * `addr` - The endpoint address (includes direct addresses for local connections)
/// * `group_info` - The MLS `GroupInfo` message bytes
///
/// # Returns
/// The [`GroupId`] assigned to the group on success.
///
/// # Errors
/// Returns an error if the connection fails or the handshake is rejected.
pub async fn register_group_with_addr(
    endpoint: &Endpoint,
    addr: impl Into<iroh::EndpointAddr>,
    group_info: &[u8],
) -> Result<GroupId, ConnectorError> {
    // Connect to the acceptor
    let conn = endpoint
        .connect(addr, PAXOS_ALPN)
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    // Open a bidirectional stream
    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    // Create framed reader/writer
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let mut writer = FramedWrite::new(send, codec);

    // Send the Create handshake
    let handshake = Handshake::CreateGroup(group_info.to_vec());
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
        .ok_or_else(|| ConnectorError::Handshake("connection closed before response".to_string()))?
        .map_err(ConnectorError::Io)?;

    let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
        .map_err(|e| ConnectorError::Codec(format!("invalid response: {e}")))?;

    // Check response
    match response {
        HandshakeResponse::Ok => {
            // Parse the group ID from the GroupInfo
            // The server creates the group and we trust it used the correct ID
            // For now, derive from the group_info bytes
            Ok(GroupId::from_slice(group_info))
        }
        HandshakeResponse::GroupNotFound => Err(ConnectorError::Handshake(
            "unexpected: group not found".to_string(),
        )),
        HandshakeResponse::InvalidGroupInfo(e) => Err(ConnectorError::Handshake(format!(
            "invalid group info: {e}"
        ))),
        HandshakeResponse::Error(e) => Err(ConnectorError::Handshake(e)),
    }
}

pin_project! {
    /// A bidirectional Paxos connection over iroh
    ///
    /// Implements both `Sink<AcceptorRequest>` and `Stream<Item = AcceptorMessage>`.
    pub struct IrohConnection<L> {
        #[pin]
        writer: FramedWrite<iroh::endpoint::SendStream, LengthDelimitedCodec>,
        #[pin]
        reader: FramedRead<iroh::endpoint::RecvStream, LengthDelimitedCodec>,
        _marker: PhantomData<fn() -> L>,
    }
}

impl<L> IrohConnection<L> {
    fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(16 * 1024 * 1024) // 16 MB max message size
            .new_codec();

        Self {
            writer: FramedWrite::new(send, codec.clone()),
            reader: FramedRead::new(recv, codec),
            _marker: PhantomData,
        }
    }
}

impl<L> Stream for IrohConnection<L>
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage>,
    L::Error: From<ConnectorError>,
{
    type Item = Result<AcceptorMessage<L>, L::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.reader.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => match postcard::from_bytes(&bytes) {
                Ok(msg) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(ConnectorError::Codec(e.to_string()).into()))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(ConnectorError::Io(e).into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<L> Sink<AcceptorRequest<L>> for IrohConnection<L>
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage>,
    L::Error: From<ConnectorError>,
{
    type Error = L::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_ready(cx)
            .map_err(|e| ConnectorError::Io(e).into())
    }

    fn start_send(self: Pin<&mut Self>, item: AcceptorRequest<L>) -> Result<(), Self::Error> {
        let bytes =
            postcard::to_allocvec(&item).map_err(|e| ConnectorError::Codec(e.to_string()))?;

        self.project()
            .writer
            .start_send(bytes.into())
            .map_err(|e| ConnectorError::Io(e).into())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_flush(cx)
            .map_err(|e| ConnectorError::Io(e).into())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_close(cx)
            .map_err(|e| ConnectorError::Io(e).into())
    }
}
