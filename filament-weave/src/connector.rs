//! [`Connector`] implementation using iroh for p2p QUIC connections to acceptors.

use error_stack::{Report, ResultExt};
pub use filament_core::ConnectorError;
use filament_core::codec::PostcardCodec;
use filament_core::{
    AcceptorId, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse, PAXOS_ALPN,
};
use futures::{SinkExt, StreamExt};
use iroh::{Endpoint, PublicKey};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Register a new group with an acceptor via discovery.
pub(crate) async fn register_group(
    endpoint: &Endpoint,
    acceptor_id: &AcceptorId,
    group_info: &[u8],
) -> Result<GroupId, Report<ConnectorError>> {
    let public_key = PublicKey::from_bytes(acceptor_id.as_bytes())
        .expect("AcceptorId should be a valid public key");
    let conn = endpoint
        .connect(public_key, PAXOS_ALPN)
        .await
        .change_context(ConnectorError)?;

    let (send, recv) = conn.open_bi().await.change_context(ConnectorError)?;

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let mut writer = FramedWrite::new(send, codec);

    let handshake = Handshake::CreateGroup(bytes::Bytes::copy_from_slice(group_info));
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
        HandshakeResponse::Data(_) => {
            Err(Report::new(ConnectorError).attach("unexpected Data response"))
        }
    }
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
