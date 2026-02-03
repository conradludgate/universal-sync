//! Iroh-based server for Paxos acceptors
//!
//! This module provides the server-side connection handling for acceptors,
//! accepting incoming iroh connections and running the Paxos acceptor protocol.
//!
//! # Connection Multiplexing
//!
//! A single iroh connection can host multiple streams. Each stream is identified
//! by a [`Handshake`] message at the start. Stream types:
//!
//! - **Proposal streams** (`JoinProposals`, `CreateGroup`): Run Paxos protocol
//! - **Message streams** (`JoinMessages`): Application message delivery
//!
//! # Example
//!
//! ```ignore
//! use universal_sync_acceptor::{accept_connection, GroupRegistry};
//!
//! let endpoint = iroh::Endpoint::builder()
//!     .alpns(vec![universal_sync_acceptor::PAXOS_ALPN.to_vec()])
//!     .bind()
//!     .await?;
//!
//! let registry = MyGroupRegistry::new();
//!
//! // Accept connections in a loop
//! while let Some(incoming) = endpoint.accept().await {
//!     let registry = registry.clone();
//!     tokio::spawn(async move {
//!         if let Err(e) = accept_connection(incoming, registry).await {
//!             eprintln!("connection error: {e}");
//!         }
//!     });
//! }
//! ```

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, Stream, StreamExt};
use iroh::endpoint::{Incoming, RecvStream, SendStream};
use pin_project_lite::pin_project;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, warn};
use universal_sync_core::{
    EncryptedAppMessage, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse,
    MemberId, MessageRequest, MessageResponse,
};
use universal_sync_paxos::acceptor::{AcceptorHandler, run_acceptor};
use universal_sync_paxos::{AcceptorMessage, AcceptorRequest, AcceptorStateStore, Learner};

use crate::connector::{ConnectorError, PAXOS_ALPN};

pin_project! {
    /// Server-side acceptor connection over iroh
    ///
    /// This is the inverse of `IrohConnection`:
    /// - Stream yields `AcceptorRequest` (from clients)
    /// - Sink accepts `AcceptorMessage` (to clients)
    pub struct IrohAcceptorConnection<A> {
        #[pin]
        writer: FramedWrite<SendStream, LengthDelimitedCodec>,
        #[pin]
        reader: FramedRead<RecvStream, LengthDelimitedCodec>,
        _marker: PhantomData<fn() -> A>,
    }
}

impl<A> IrohAcceptorConnection<A> {
    /// Create a new acceptor connection from iroh streams
    #[must_use]
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
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

impl<A> Stream for IrohAcceptorConnection<A>
where
    A: Learner<Proposal = GroupProposal, Message = GroupMessage>,
    A::Error: From<ConnectorError>,
{
    type Item = Result<AcceptorRequest<A>, A::Error>;

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

impl<A> Sink<AcceptorMessage<A>> for IrohAcceptorConnection<A>
where
    A: Learner<Proposal = GroupProposal, Message = GroupMessage>,
    A::Error: From<ConnectorError>,
{
    type Error = A::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_ready(cx)
            .map_err(|e| ConnectorError::Io(e).into())
    }

    fn start_send(self: Pin<&mut Self>, item: AcceptorMessage<A>) -> Result<(), Self::Error> {
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

pin_project! {
    /// Server-side message connection over iroh
    ///
    /// Used for application message streams (not Paxos):
    /// - Stream yields `MessageRequest` (from clients)
    /// - Sink accepts `MessageResponse` (to clients)
    pub struct IrohMessageConnection {
        #[pin]
        writer: FramedWrite<SendStream, LengthDelimitedCodec>,
        #[pin]
        reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    }
}

impl IrohMessageConnection {
    /// Create a new message connection from iroh streams
    #[must_use]
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(16 * 1024 * 1024) // 16 MB max message size
            .new_codec();

        Self {
            writer: FramedWrite::new(send, codec.clone()),
            reader: FramedRead::new(recv, codec),
        }
    }
}

impl Stream for IrohMessageConnection {
    type Item = Result<MessageRequest, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.reader.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => match postcard::from_bytes(&bytes) {
                Ok(msg) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(ConnectorError::Codec(e.to_string())))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(ConnectorError::Io(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<MessageResponse> for IrohMessageConnection {
    type Error = ConnectorError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_ready(cx)
            .map_err(ConnectorError::Io)
    }

    fn start_send(self: Pin<&mut Self>, item: MessageResponse) -> Result<(), Self::Error> {
        let bytes =
            postcard::to_allocvec(&item).map_err(|e| ConnectorError::Codec(e.to_string()))?;

        self.project()
            .writer
            .start_send(bytes.into())
            .map_err(ConnectorError::Io)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_flush(cx)
            .map_err(ConnectorError::Io)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .writer
            .poll_close(cx)
            .map_err(ConnectorError::Io)
    }
}

/// Registry for looking up and creating groups
///
/// Implementations should handle:
/// - Looking up existing groups by ID
/// - Creating new groups from `GroupInfo`
/// - Managing per-group state stores
pub trait GroupRegistry: Clone + Send + Sync + 'static {
    /// The acceptor type for groups
    type Acceptor: universal_sync_paxos::Acceptor<Proposal = GroupProposal, Message = GroupMessage>;

    /// The state store type for groups
    type StateStore: AcceptorStateStore<Self::Acceptor>;

    /// Look up an existing group by ID
    ///
    /// Returns the acceptor and state store if the group exists.
    fn get_group(&self, group_id: &GroupId) -> Option<(Self::Acceptor, Self::StateStore)>;

    /// Create a new group from `GroupInfo` bytes
    ///
    /// The bytes are a serialized MLS `MlsMessage` containing `GroupInfo`.
    /// Returns the group ID, acceptor, and state store if successful.
    ///
    /// # Errors
    /// Returns an error if parsing or joining the group fails.
    fn create_group(
        &self,
        group_info: &[u8],
    ) -> Result<(GroupId, Self::Acceptor, Self::StateStore), String>;

    /// Store an application message.
    ///
    /// # Errors
    /// Returns an error if the group is not found or storage fails.
    fn store_message(&self, group_id: &GroupId, msg: &EncryptedAppMessage) -> Result<u64, String>;

    /// Get messages since a sequence number.
    ///
    /// # Errors
    /// Returns an error if the group is not found.
    fn get_messages_since(
        &self,
        group_id: &GroupId,
        since_seq: u64,
    ) -> Result<Vec<(u64, EncryptedAppMessage)>, String>;

    /// Subscribe to new messages for a group.
    fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> tokio::sync::broadcast::Receiver<EncryptedAppMessage>;
}

/// Accept incoming iroh connection and handle multiplexed streams.
///
/// This function:
/// 1. Accepts the incoming iroh connection
/// 2. Validates the ALPN protocol
/// 3. Loops accepting streams until the connection closes
/// 4. For each stream, reads handshake and spawns appropriate handler
///
/// Stream types:
/// - `JoinProposals` / `CreateGroup`: Paxos proposal stream
/// - `JoinMessages`: Application message stream
///
/// # Arguments
/// * `incoming` - The incoming iroh connection
/// * `registry` - The group registry for looking up/creating groups
///
/// # Returns
/// Returns `Ok(())` when the connection closes normally.
///
/// # Errors
/// Returns an error if the connection fails or ALPN is wrong.
pub async fn accept_connection<R>(incoming: Incoming, registry: R) -> Result<(), ConnectorError>
where
    R: GroupRegistry,
    <R::Acceptor as Learner>::Error: From<ConnectorError>,
{
    // Accept the connection
    let conn = incoming
        .accept()
        .map_err(|e| ConnectorError::Connect(e.to_string()))?
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    // Check ALPN
    let alpn = conn.alpn();
    if alpn != PAXOS_ALPN {
        warn!(?alpn, "unexpected ALPN, closing connection");
        return Err(ConnectorError::Connect("unexpected ALPN".to_string()));
    }

    let remote_id = conn.remote_id();
    debug!(?remote_id, "accepted connection");

    // Accept and handle one stream (the first one)
    // TODO: Support multiple concurrent streams per connection once
    // AcceptorStateStore methods have Send bounds on their futures
    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    handle_stream(send, recv, registry).await
}

/// Handle a single multiplexed stream.
async fn handle_stream<R>(
    send: SendStream,
    recv: RecvStream,
    registry: R,
) -> Result<(), ConnectorError>
where
    R: GroupRegistry,
    <R::Acceptor as Learner>::Error: From<ConnectorError>,
{
    // Create framed reader/writer for handshake
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let writer = FramedWrite::new(send, codec);

    // Read the handshake message
    let handshake_bytes = reader
        .next()
        .await
        .ok_or_else(|| ConnectorError::Connect("stream closed before handshake".to_string()))?
        .map_err(ConnectorError::Io)?;

    let handshake: Handshake = postcard::from_bytes(&handshake_bytes)
        .map_err(|e| ConnectorError::Codec(format!("invalid handshake: {e}")))?;

    match handshake {
        Handshake::JoinProposals(group_id) => {
            handle_proposal_stream(group_id, None, reader, writer, registry).await
        }
        Handshake::CreateGroup(group_info) => {
            handle_proposal_stream(
                GroupId::from_slice(&[0; 32]),
                Some(group_info),
                reader,
                writer,
                registry,
            )
            .await
        }
        Handshake::JoinMessages(group_id) => {
            handle_message_stream(group_id, reader, writer, registry).await
        }
    }
}

/// Handle a proposal (Paxos) stream.
async fn handle_proposal_stream<R>(
    mut group_id: GroupId,
    create_group_info: Option<Vec<u8>>,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: R,
) -> Result<(), ConnectorError>
where
    R: GroupRegistry,
    <R::Acceptor as Learner>::Error: From<ConnectorError>,
{
    // Get or create the group
    let (acceptor, state) = if let Some(group_info) = create_group_info {
        match registry.create_group(&group_info) {
            Ok((id, acceptor, state)) => {
                group_id = id;
                (acceptor, state)
            }
            Err(e) => {
                let response = HandshakeResponse::InvalidGroupInfo(e.clone());
                let response_bytes = postcard::to_allocvec(&response)
                    .map_err(|e| ConnectorError::Codec(e.to_string()))?;
                writer
                    .send(response_bytes.into())
                    .await
                    .map_err(ConnectorError::Io)?;
                return Err(ConnectorError::Connect(format!(
                    "failed to create group: {e}"
                )));
            }
        }
    } else if let Some((acceptor, state)) = registry.get_group(&group_id) {
        (acceptor, state)
    } else {
        let response = HandshakeResponse::GroupNotFound;
        let response_bytes =
            postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
        writer
            .send(response_bytes.into())
            .await
            .map_err(ConnectorError::Io)?;
        return Err(ConnectorError::Connect("group not found".to_string()));
    };

    debug!(?group_id, "proposal stream handshake complete");

    // Send success response
    let response = HandshakeResponse::Ok;
    let response_bytes =
        postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
    writer
        .send(response_bytes.into())
        .await
        .map_err(ConnectorError::Io)?;

    // Extract inner streams from framed wrappers
    let recv = reader.into_inner();
    let send = writer.into_inner();

    // Create the connection wrapper for Paxos protocol
    let connection = IrohAcceptorConnection::<R::Acceptor>::new(send, recv);

    // Create the handler with shared state
    let handler = AcceptorHandler::new(acceptor, state);

    // Run the acceptor protocol
    let proposer_id = MemberId(0);

    run_acceptor(handler, connection, proposer_id)
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    debug!(?group_id, "proposal stream closed");
    Ok(())
}

/// Handle a message stream.
async fn handle_message_stream<R>(
    group_id: GroupId,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: R,
) -> Result<(), ConnectorError>
where
    R: GroupRegistry,
{
    // Check if group exists
    if registry.get_group(&group_id).is_none() {
        let response = HandshakeResponse::GroupNotFound;
        let response_bytes =
            postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
        writer
            .send(response_bytes.into())
            .await
            .map_err(ConnectorError::Io)?;
        return Err(ConnectorError::Connect("group not found".to_string()));
    }

    debug!(?group_id, "message stream handshake complete");

    // Send success response
    let response = HandshakeResponse::Ok;
    let response_bytes =
        postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
    writer
        .send(response_bytes.into())
        .await
        .map_err(ConnectorError::Io)?;

    // Extract inner streams
    let recv = reader.into_inner();
    let send = writer.into_inner();

    // Create the message connection
    let mut connection = IrohMessageConnection::new(send, recv);

    // Subscribe to live messages
    let mut subscription = registry.subscribe_messages(&group_id);

    // Handle requests and forward live messages
    loop {
        tokio::select! {
            // Handle incoming requests
            request = connection.next() => {
                let Some(request) = request else {
                    debug!(?group_id, "message stream closed by client");
                    break;
                };

                let request = request?;
                handle_message_request(&group_id, request, &mut connection, &registry).await?;
            }

            // Forward live messages from subscription
            msg = subscription.recv() => {
                match msg {
                    Ok(msg) => {
                        let response = MessageResponse::Message {
                            arrival_seq: 0, // We don't have the seq from broadcast
                            message: msg,
                        };
                        connection.send(response).await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Client fell behind, continue
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break;
                    }
                }
            }
        }
    }

    debug!(?group_id, "message stream closed");
    Ok(())
}

/// Handle a single message request.
async fn handle_message_request<R>(
    group_id: &GroupId,
    request: MessageRequest,
    connection: &mut IrohMessageConnection,
    registry: &R,
) -> Result<(), ConnectorError>
where
    R: GroupRegistry,
{
    match request {
        MessageRequest::Send(msg) => match registry.store_message(group_id, &msg) {
            Ok(arrival_seq) => {
                let response = MessageResponse::Stored { arrival_seq };
                connection.send(response).await?;
            }
            Err(e) => {
                let response = MessageResponse::Error(e);
                connection.send(response).await?;
            }
        },

        MessageRequest::Subscribe { since_seq: _ } => {
            // Subscription is handled via the broadcast channel in the main loop
            // The since_seq is informational - we don't backfill here
        }

        MessageRequest::Backfill { since_seq, limit } => {
            match registry.get_messages_since(group_id, since_seq) {
                Ok(messages) => {
                    let has_more = messages.len() > limit as usize;
                    let messages: Vec<_> = messages.into_iter().take(limit as usize).collect();
                    let last_seq = messages.last().map_or(since_seq, |(seq, _)| *seq);

                    for (arrival_seq, msg) in messages {
                        let response = MessageResponse::Message {
                            arrival_seq,
                            message: msg,
                        };
                        connection.send(response).await?;
                    }

                    let response = MessageResponse::BackfillComplete { last_seq, has_more };
                    connection.send(response).await?;
                }
                Err(e) => {
                    let response = MessageResponse::Error(e);
                    connection.send(response).await?;
                }
            }
        }
    }

    Ok(())
}
