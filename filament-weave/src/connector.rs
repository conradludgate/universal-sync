//! [`Connector`] implementation using iroh for p2p QUIC connections to acceptors.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use error_stack::{Report, ResultExt};
pub use filament_core::ConnectorError;
use filament_core::codec::PostcardCodec;
use filament_core::sink_stream::{Mapped, SinkStream};
use filament_core::{
    AcceptorId, Epoch, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse,
    PAXOS_ALPN,
};
use filament_warp::{AcceptorMessage, AcceptorRequest, Connector, Learner};
use futures::{SinkExt, StreamExt};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Connects to Paxos acceptors using iroh p2p QUIC. Performs a Join handshake
/// on connect. For registering new groups, use [`register_group_with_addr`].
pub struct IrohConnector<L> {
    endpoint: Endpoint,
    group_id: GroupId,
    since_epoch: Arc<std::sync::atomic::AtomicU64>,
    address_hints: Arc<HashMap<AcceptorId, EndpointAddr>>,
    _marker: PhantomData<fn() -> L>,
}

impl<L> Clone for IrohConnector<L> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            group_id: self.group_id,
            since_epoch: self.since_epoch.clone(),
            address_hints: self.address_hints.clone(),
            _marker: PhantomData,
        }
    }
}

impl<L> IrohConnector<L> {
    pub fn set_since_epoch(&self, epoch: Epoch) {
        self.since_epoch
            .store(epoch.0, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<L> Connector<L> for IrohConnector<L>
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage, AcceptorId = AcceptorId>,
    L::Error: From<ConnectorError> + filament_core::FromIoError,
{
    type Connection = IrohConnection<L, L::Error>;
    type Error = Report<ConnectorError>;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Connection, Self::Error>> + Send>>;

    fn connect(&mut self, acceptor_id: &AcceptorId) -> Self::ConnectFuture {
        let endpoint = self.endpoint.clone();
        let group_id = self.group_id;
        let since_epoch = self.since_epoch.load(std::sync::atomic::Ordering::Relaxed);

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
            let conn = endpoint
                .connect(addr, PAXOS_ALPN)
                .await
                .change_context(ConnectorError)?;

            let (send, recv) = conn.open_bi().await.change_context(ConnectorError)?;

            let codec = LengthDelimitedCodec::builder()
                .max_frame_length(16 * 1024 * 1024)
                .new_codec();
            let mut reader = FramedRead::new(recv, codec.clone());
            let mut writer = FramedWrite::new(send, codec);

            let handshake = Handshake::JoinProposals {
                group_id,
                since_epoch: Epoch(since_epoch),
            };
            let handshake_bytes =
                postcard::to_allocvec(&handshake).change_context(ConnectorError)?;
            writer
                .send(handshake_bytes.into())
                .await
                .change_context(ConnectorError)?;

            let response_bytes = reader
                .next()
                .await
                .ok_or_else(|| {
                    Report::new(ConnectorError).attach("connection closed before response")
                })?
                .change_context(ConnectorError)?;

            let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
                .change_context(ConnectorError)
                .attach("invalid handshake response")?;

            match response {
                HandshakeResponse::Ok => {}
                HandshakeResponse::GroupNotFound => {
                    return Err(Report::new(ConnectorError).attach("group not found"));
                }
                HandshakeResponse::InvalidGroupInfo(e) => {
                    return Err(
                        Report::new(ConnectorError).attach(format!("invalid group info: {e}"))
                    );
                }
                HandshakeResponse::Error(e) => {
                    return Err(Report::new(ConnectorError).attach(e));
                }
            }

            Ok(new_iroh_connection::<L, L::Error>(writer, reader))
        })
    }
}

/// Register a new group with an acceptor. Returns the [`GroupId`] on success.
pub(crate) async fn register_group_with_addr(
    endpoint: &Endpoint,
    addr: impl Into<iroh::EndpointAddr>,
    group_info: &[u8],
) -> Result<GroupId, Report<ConnectorError>> {
    let conn = endpoint
        .connect(addr, PAXOS_ALPN)
        .await
        .change_context(ConnectorError)?;

    let (send, recv) = conn.open_bi().await.change_context(ConnectorError)?;

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let mut writer = FramedWrite::new(send, codec);

    let handshake = Handshake::CreateGroup(group_info.to_vec());
    let handshake_bytes = postcard::to_allocvec(&handshake).change_context(ConnectorError)?;
    writer
        .send(handshake_bytes.into())
        .await
        .change_context(ConnectorError)?;

    let response_bytes = reader
        .next()
        .await
        .ok_or_else(|| Report::new(ConnectorError).attach("connection closed before response"))?
        .change_context(ConnectorError)?;

    let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
        .change_context(ConnectorError)
        .attach("invalid handshake response")?;

    match response {
        HandshakeResponse::Ok => Ok(GroupId::from_slice(group_info)),
        HandshakeResponse::GroupNotFound => {
            Err(Report::new(ConnectorError).attach("unexpected: group not found"))
        }
        HandshakeResponse::InvalidGroupInfo(e) => {
            Err(Report::new(ConnectorError).attach(format!("invalid group info: {e}")))
        }
        HandshakeResponse::Error(e) => Err(Report::new(ConnectorError).attach(e)),
    }
}

/// Bidirectional Paxos connection over iroh (`Sink<AcceptorRequest>` + `Stream<AcceptorMessage>`).
pub type IrohConnection<L, E> = Mapped<
    SinkStream<
        FramedWrite<iroh::endpoint::SendStream, PostcardCodec<AcceptorRequest<L>>>,
        FramedRead<iroh::endpoint::RecvStream, PostcardCodec<AcceptorMessage<L>>>,
    >,
    E,
>;

#[must_use]
pub(crate) fn new_iroh_connection<L, E>(
    writer: crate::connection::HandshakeWriter,
    reader: crate::connection::HandshakeReader,
) -> IrohConnection<L, E>
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage>,
{
    Mapped::new(SinkStream::new(
        writer.map_encoder(PostcardCodec::wrap),
        reader.map_decoder(PostcardCodec::wrap),
    ))
}

/// Wire format for proposal requests (independent of Learner type).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ProposalRequest {
    /// Phase 1: Prepare
    Prepare(GroupProposal),
    /// Phase 2: Accept
    Accept(GroupProposal, GroupMessage),
}

/// Wire format for proposal responses (independent of Learner type).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProposalResponse {
    pub(crate) promised: GroupProposal,
    pub(crate) accepted: Option<(GroupProposal, GroupMessage)>,
}

pub(crate) type ProposalWriter =
    FramedWrite<iroh::endpoint::SendStream, PostcardCodec<ProposalRequest>>;

pub(crate) type ProposalReader =
    FramedRead<iroh::endpoint::RecvStream, PostcardCodec<ProposalResponse>>;

#[must_use]
pub(crate) fn make_proposal_streams(
    writer: crate::connection::HandshakeWriter,
    reader: crate::connection::HandshakeReader,
) -> (ProposalWriter, ProposalReader) {
    (
        writer.map_encoder(PostcardCodec::wrap),
        reader.map_decoder(PostcardCodec::wrap),
    )
}
