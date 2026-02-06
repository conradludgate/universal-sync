//! Server-side connection handling for acceptors.

use futures::{SinkExt, StreamExt};
use iroh::endpoint::{Incoming, RecvStream, SendStream};
use mls_rs::CipherSuiteProvider;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, instrument, warn};
use universal_sync_core::codec::PostcardCodec;
use universal_sync_core::sink_stream::{Mapped, SinkStream};
use universal_sync_core::{
    ConnectorError, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse, MemberId,
    MessageRequest, MessageResponse, PAXOS_ALPN,
};
use universal_sync_paxos::acceptor::{AcceptorHandler, run_acceptor_with_epoch_waiter};
use universal_sync_paxos::{AcceptorMessage, AcceptorRequest, Learner};

use crate::acceptor::GroupAcceptor;
use crate::registry::AcceptorRegistry;

pub type IrohAcceptorConnection<A, E> = Mapped<
    SinkStream<
        FramedWrite<SendStream, PostcardCodec<AcceptorMessage<A>>>,
        FramedRead<RecvStream, PostcardCodec<AcceptorRequest<A>>>,
    >,
    E,
>;

#[must_use]
pub(crate) fn new_acceptor_connection<A, E>(
    send: SendStream,
    recv: RecvStream,
) -> IrohAcceptorConnection<A, E>
where
    A: Learner<Proposal = GroupProposal, Message = GroupMessage>,
{
    Mapped::new(SinkStream::new(
        FramedWrite::new(send, PostcardCodec::new()),
        FramedRead::new(recv, PostcardCodec::new()),
    ))
}

pub(crate) type IrohMessageConnection = SinkStream<
    FramedWrite<SendStream, PostcardCodec<MessageResponse>>,
    FramedRead<RecvStream, PostcardCodec<MessageRequest>>,
>;

#[must_use]
pub(crate) fn new_message_connection(send: SendStream, recv: RecvStream) -> IrohMessageConnection {
    SinkStream::new(
        FramedWrite::new(send, PostcardCodec::new()),
        FramedRead::new(recv, PostcardCodec::new()),
    )
}

#[instrument(skip_all, name = "accept_connection")]
pub async fn accept_connection<C, CS>(
    incoming: Incoming,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), ConnectorError>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let conn = incoming
        .accept()
        .map_err(|e| ConnectorError::Connect(e.to_string()))?
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    let alpn = conn.alpn();
    if alpn != PAXOS_ALPN {
        warn!(?alpn, "unexpected ALPN, closing connection");
        return Err(ConnectorError::Connect("unexpected ALPN".to_string()));
    }

    let remote_id = conn.remote_id();
    debug!(?remote_id, "accepted connection");

    loop {
        let (send, recv) = conn
            .accept_bi()
            .await
            .map_err(|e| ConnectorError::Connect(e.to_string()))?;

        tokio::spawn(handle_stream(send, recv, registry.clone()));
    }
}

#[instrument(skip_all, name = "stream")]
async fn handle_stream<C, CS>(
    send: SendStream,
    recv: RecvStream,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), ConnectorError>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let writer = FramedWrite::new(send, codec);

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
        Handshake::SendWelcome(_) => {
            Err(ConnectorError::Handshake(
                "acceptors do not handle welcome messages".to_string(),
            ))
        }
    }
}

#[instrument(skip_all, name = "proposal_stream", fields(?group_id))]
async fn handle_proposal_stream<C, CS>(
    mut group_id: GroupId,
    create_group_info: Option<Vec<u8>>,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), ConnectorError>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let (acceptor, state) = if let Some(group_info) = create_group_info {
        match registry.create_group(&group_info) {
            Ok((id, acceptor, state)) => {
                group_id = id;
                (acceptor, state)
            }
            Err(e) => {
                warn!(%e, "failed to create group from GroupInfo");
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
        warn!(?group_id, "group not found for proposal stream");
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

   
    let response = HandshakeResponse::Ok;
    let response_bytes =
        postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
    writer
        .send(response_bytes.into())
        .await
        .map_err(ConnectorError::Io)?;

    let recv = reader.into_inner();
    let send = writer.into_inner();

    let connection = new_acceptor_connection::<
        GroupAcceptor<C, CS>,
        error_stack::Report<crate::acceptor::AcceptorError>,
    >(send, recv);

    let handler = AcceptorHandler::new(acceptor, state);

    let proposer_id = MemberId(0);
    let (epoch_rx, current_epoch_fn) = registry
        .get_epoch_watcher(&group_id)
        .expect("epoch watcher should exist after get_group/create_group");

    run_acceptor_with_epoch_waiter(handler, connection, proposer_id, epoch_rx, current_epoch_fn)
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    debug!(?group_id, "proposal stream closed");
    Ok(())
}

#[instrument(skip_all, name = "message_stream", fields(?group_id))]
async fn handle_message_stream<C, CS>(
    group_id: GroupId,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), ConnectorError>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    if registry.get_group(&group_id).is_none() {
        warn!("group not found for message stream");
        let response = HandshakeResponse::GroupNotFound;
        let response_bytes =
            postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
        writer
            .send(response_bytes.into())
            .await
            .map_err(ConnectorError::Io)?;
        return Err(ConnectorError::Connect("group not found".to_string()));
    }

    debug!("message stream handshake complete");

   
    let response = HandshakeResponse::Ok;
    let response_bytes =
        postcard::to_allocvec(&response).map_err(|e| ConnectorError::Codec(e.to_string()))?;
    writer
        .send(response_bytes.into())
        .await
        .map_err(ConnectorError::Io)?;

    let recv = reader.into_inner();
    let send = writer.into_inner();
    let mut connection = new_message_connection(send, recv);
    let mut subscription = registry.subscribe_messages(&group_id);

    loop {
        tokio::select! {
            request = connection.next() => {
                let Some(request) = request else {
                    debug!(?group_id, "message stream closed by client");
                    break;
                };

                let request = request?;
                handle_message_request(&group_id, request, &mut connection, &registry).await?;
            }

            msg = subscription.recv() => {
                match msg {
                    Ok(msg) => {
                        let response = MessageResponse::Message {
                            arrival_seq: 0, // We don't have the seq from broadcast
                            message: msg,
                        };
                        connection.send(response).await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    debug!(?group_id, "message stream closed");
    Ok(())
}

async fn handle_message_request<C, CS>(
    group_id: &GroupId,
    request: MessageRequest,
    connection: &mut IrohMessageConnection,
    registry: &AcceptorRegistry<C, CS>,
) -> Result<(), ConnectorError>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
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

        MessageRequest::Subscribe { since_seq: _ } => {}

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
