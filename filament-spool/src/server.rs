//! Server-side connection handling for acceptors.

use error_stack::{Report, ResultExt};
use filament_core::codec::{PostcardCodec, VersionedCodec};
use filament_core::sink_stream::{Mapped, SinkStream};
use filament_core::{
    ConnectorError, Epoch, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse,
    MemberFingerprint, MemberId, MessageRequest, MessageResponse, PAXOS_ALPN, StateVector,
};
use filament_warp::acceptor::{AcceptorHandler, run_acceptor_with_epoch_waiter};
use filament_warp::{AcceptorMessage, AcceptorRequest, Learner};
use futures::{SinkExt, StreamExt};
use iroh::endpoint::{Incoming, RecvStream, SendStream};
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::{CipherSuiteProvider, MlsMessage};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, instrument, warn};

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
    FramedWrite<SendStream, VersionedCodec<MessageResponse>>,
    FramedRead<RecvStream, VersionedCodec<MessageRequest>>,
>;

#[must_use]
pub(crate) fn new_message_connection(
    send: SendStream,
    recv: RecvStream,
    protocol_version: u32,
) -> IrohMessageConnection {
    SinkStream::new(
        FramedWrite::new(send, VersionedCodec::new(protocol_version)),
        FramedRead::new(recv, VersionedCodec::new(protocol_version)),
    )
}

/// # Errors
///
/// Returns [`ConnectorError`] if the connection or protocol exchange fails.
#[instrument(skip_all, name = "accept_connection")]
pub async fn accept_connection<C, CS>(
    incoming: Incoming,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), Report<ConnectorError>>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let conn = incoming
        .accept()
        .change_context(ConnectorError)?
        .await
        .change_context(ConnectorError)?;

    let alpn = conn.alpn();
    if alpn != PAXOS_ALPN {
        warn!(?alpn, "unexpected ALPN, closing connection");
        return Err(Report::new(ConnectorError).attach("unexpected ALPN"));
    }

    let remote_id = conn.remote_id();
    debug!(?remote_id, "accepted connection");

    let metrics = registry.metrics().clone();
    metrics.metrics.connections_opened_total.inc();

    let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_tx: u64 = 0;
    let mut last_rx: u64 = 0;

    let mut flush_stats = || {
        let stats = conn.stats();
        let tx = stats.udp_tx.bytes;
        let rx = stats.udp_rx.bytes;
        metrics.metrics.data_sent_bytes_total.inc_by(tx - last_tx);
        metrics
            .metrics
            .data_received_bytes_total
            .inc_by(rx - last_rx);
        last_tx = tx;
        last_rx = rx;
    };

    let result = loop {
        tokio::select! {
            incoming = conn.accept_bi() => {
                match incoming.change_context(ConnectorError) {
                    Ok((send, recv)) => {
                        tokio::spawn(handle_stream(send, recv, registry.clone()));
                    }
                    Err(e) => break Err(e),
                }
            }
            _ = stats_interval.tick() => {
                flush_stats();
            }
        }
    };

    flush_stats();
    metrics.metrics.connections_closed_total.inc();

    result
}

#[instrument(skip_all, name = "stream")]
async fn handle_stream<C, CS>(
    send: SendStream,
    recv: RecvStream,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), Report<ConnectorError>>
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
        .ok_or_else(|| Report::new(ConnectorError).attach("stream closed before handshake"))?
        .change_context(ConnectorError)?;

    let handshake: Handshake = postcard::from_bytes(&handshake_bytes)
        .change_context(ConnectorError)
        .attach("invalid handshake")?;

    match handshake {
        Handshake::JoinProposals {
            group_id,
            since_epoch,
        } => handle_proposal_stream(group_id, None, since_epoch, reader, writer, registry).await,
        Handshake::CreateGroup { group_info } => {
            handle_proposal_stream(
                GroupId::from_slice(&[0; 32]),
                Some(group_info),
                Epoch(0),
                reader,
                writer,
                registry,
            )
            .await
        }
        Handshake::JoinMessages(group_id, subscriber) => {
            handle_message_stream(group_id, subscriber, reader, writer, registry).await
        }
        Handshake::SendWelcome { .. } | Handshake::SendKeyPackage { .. } => {
            Err(Report::new(ConnectorError)
                .attach("acceptors do not handle welcome or key package messages"))
        }
    }
}

#[instrument(skip_all, name = "proposal_stream", fields(?group_id))]
async fn handle_proposal_stream<C, CS>(
    mut group_id: GroupId,
    create_group_info: Option<MlsMessage>,
    since_epoch: Epoch,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), Report<ConnectorError>>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let (acceptor, state) = if let Some(group_info) = create_group_info {
        match registry.create_group(group_info) {
            Ok((id, acceptor, state)) => {
                group_id = id;
                (acceptor, state)
            }
            Err(e) => {
                warn!(?e, "failed to create group from GroupInfo");
                let msg = format!("{e:?}");
                let response = HandshakeResponse::InvalidGroupInfo(msg);
                let response_bytes =
                    postcard::to_allocvec(&response).change_context(ConnectorError)?;
                writer
                    .send(response_bytes.into())
                    .await
                    .change_context(ConnectorError)?;
                return Err(e.change_context(ConnectorError));
            }
        }
    } else if let Some((acceptor, state)) = registry.get_group(&group_id) {
        (acceptor, state)
    } else {
        warn!(?group_id, "group not found for proposal stream");
        let response = HandshakeResponse::GroupNotFound;
        let response_bytes = postcard::to_allocvec(&response).change_context(ConnectorError)?;
        writer
            .send(response_bytes.into())
            .await
            .change_context(ConnectorError)?;
        return Err(Report::new(ConnectorError).attach("group not found"));
    };

    debug!(?group_id, "proposal stream handshake complete");

    let response = HandshakeResponse::Ok;
    let response_bytes = postcard::to_allocvec(&response).change_context(ConnectorError)?;
    writer
        .send(response_bytes.into())
        .await
        .change_context(ConnectorError)?;

    let recv = reader.into_inner();
    let send = writer.into_inner();

    let connection = new_acceptor_connection::<
        GroupAcceptor<C, CS>,
        Report<crate::acceptor::AcceptorError>,
    >(send, recv);

    let handler = AcceptorHandler::new(acceptor, state);

    let proposer_id = MemberId(0);
    let (epoch_rx, current_epoch_fn) = registry
        .get_epoch_watcher(&group_id)
        .expect("epoch watcher should exist after get_group/create_group");

    run_acceptor_with_epoch_waiter(
        handler,
        connection,
        proposer_id,
        since_epoch,
        epoch_rx,
        current_epoch_fn,
    )
    .await
    .change_context(ConnectorError)?;

    debug!(?group_id, "proposal stream closed");
    Ok(())
}

#[instrument(skip_all, name = "message_stream", fields(?group_id))]
async fn handle_message_stream<C, CS>(
    group_id: GroupId,
    subscriber: MemberFingerprint,
    reader: FramedRead<RecvStream, LengthDelimitedCodec>,
    mut writer: FramedWrite<SendStream, LengthDelimitedCodec>,
    registry: AcceptorRegistry<C, CS>,
) -> Result<(), Report<ConnectorError>>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    let Some((acceptor, _state)) = registry.get_group(&group_id) else {
        warn!("group not found for message stream");
        let response = HandshakeResponse::GroupNotFound;
        let response_bytes = postcard::to_allocvec(&response).change_context(ConnectorError)?;
        writer
            .send(response_bytes.into())
            .await
            .change_context(ConnectorError)?;
        return Err(Report::new(ConnectorError).attach("group not found"));
    };
    let protocol_version = acceptor.protocol_version().change_context(ConnectorError)?;

    debug!("message stream handshake complete");

    let response = HandshakeResponse::Ok;
    let response_bytes = postcard::to_allocvec(&response).change_context(ConnectorError)?;
    writer
        .send(response_bytes.into())
        .await
        .change_context(ConnectorError)?;

    let recv = reader.into_inner();
    let send = writer.into_inner();
    let mut connection = new_message_connection(send, recv, protocol_version);
    let mut subscription = registry.subscribe_messages(&group_id);
    let mut local_sv = StateVector::default();

    loop {
        tokio::select! {
            request = connection.next() => {
                let Some(request) = request else {
                    debug!(?group_id, "message stream closed by client");
                    break;
                };

                let request = request.change_context(ConnectorError)?;
                handle_message_request(&group_id, &subscriber, request, &mut connection, &registry, &acceptor).await?;
            }

            result = subscription.changed() => {
                if result.is_err() {
                    break;
                }

                let messages = registry.get_messages_after(&group_id, &local_sv);
                for (id, message) in messages {
                    let hw = local_sv.entry(id.sender).or_insert(0);
                    *hw = (*hw).max(id.seq);

                    if id.sender == subscriber {
                        continue;
                    }
                    let response = MessageResponse::Message { id, message };
                    connection.send(response).await.change_context(ConnectorError)?;
                }
            }
        }
    }

    debug!(?group_id, "message stream closed");
    Ok(())
}

async fn handle_message_request<C, CS>(
    group_id: &GroupId,
    subscriber: &MemberFingerprint,
    request: MessageRequest,
    connection: &mut IrohMessageConnection,
    registry: &AcceptorRegistry<C, CS>,
    acceptor: &GroupAcceptor<C, CS>,
) -> Result<(), Report<ConnectorError>>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    match request {
        MessageRequest::Send { id, message } => {
            if !acceptor.is_fingerprint_in_roster(group_id, id.sender) {
                debug!(sender = ?id.sender, "rejecting message from sender not in roster");
                connection
                    .send(MessageResponse::Error("sender not in roster".into()))
                    .await
                    .change_context(ConnectorError)?;
                return Ok(());
            }

            match registry.store_message(group_id, &id, &message) {
                Ok(()) => {
                    connection
                        .send(MessageResponse::Stored)
                        .await
                        .change_context(ConnectorError)?;
                }
                Err(e) => {
                    connection
                        .send(MessageResponse::Error(format!("{e:?}")))
                        .await
                        .change_context(ConnectorError)?;
                }
            }
        }

        MessageRequest::Subscribe { state_vector: _ } => {}

        MessageRequest::Backfill {
            state_vector,
            limit,
        } => {
            let messages = registry.get_messages_after(group_id, &state_vector);
            let has_more = messages.len() > limit as usize;
            // Filter out the subscriber's own messages.
            let messages: Vec<_> = messages
                .into_iter()
                .filter(|(id, _)| id.sender != *subscriber)
                .take(limit as usize)
                .collect();

            for (id, message) in messages {
                connection
                    .send(MessageResponse::Message { id, message })
                    .await
                    .change_context(ConnectorError)?;
            }

            connection
                .send(MessageResponse::BackfillComplete { has_more })
                .await
                .change_context(ConnectorError)?;
        }
    }

    Ok(())
}
