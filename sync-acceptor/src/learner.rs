//! Acceptor-side learning for multi-acceptor quorum tracking.

use std::collections::BTreeMap;

use futures::{SinkExt, StreamExt};
use iroh::{Endpoint, EndpointAddr};
use tokio::sync::{mpsc, watch};
use universal_sync_core::{
    AcceptorId, ConnectorError, Epoch, GroupId, GroupMessage, GroupProposal, PAXOS_ALPN,
};
use universal_sync_paxos::AcceptorStateStore;
use universal_sync_paxos::proposer::QuorumTracker;

use crate::acceptor::GroupAcceptor;
use crate::state_store::GroupStateStore;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ProposalRequest {
    Prepare(GroupProposal),
    Accept(GroupProposal, GroupMessage),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProposalResponse {
    pub(crate) promised: GroupProposal,
    pub(crate) accepted: Option<(GroupProposal, GroupMessage)>,
}

#[derive(Debug)]
enum PeerEvent {
    Accepted {
        acceptor_id: AcceptorId,
        proposal: GroupProposal,
        message: Box<GroupMessage>,
    },
    Disconnected {
        acceptor_id: AcceptorId,
        error: String,
    },
}

pub(crate) struct GroupLearningActor<C, CS>
where
    C: mls_rs::external_client::builder::MlsConfig + Clone + 'static,
    CS: mls_rs::CipherSuiteProvider + 'static,
{
    own_id: AcceptorId,
    group_id: GroupId,
    endpoint: Endpoint,
    peer_acceptors: BTreeMap<AcceptorId, EndpointAddr>,
    quorum_tracker: QuorumTracker<GroupAcceptor<C, CS>>,
    peer_rx: mpsc::Receiver<PeerEvent>,
    peer_tx: mpsc::Sender<PeerEvent>,
    epoch_tx: watch::Sender<Epoch>,
    peer_handles: BTreeMap<AcceptorId, tokio::task::JoinHandle<()>>,
}

impl<C, CS> GroupLearningActor<C, CS>
where
    C: mls_rs::external_client::builder::MlsConfig + Clone + Send + Sync + 'static,
    CS: mls_rs::CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    #[must_use]
    pub(crate) fn new(
        own_id: AcceptorId,
        group_id: GroupId,
        endpoint: Endpoint,
        initial_acceptors: impl IntoIterator<Item = (AcceptorId, EndpointAddr)>,
        epoch_tx: watch::Sender<Epoch>,
    ) -> Self {
        let (peer_tx, peer_rx) = mpsc::channel(256);

        let peer_acceptors: BTreeMap<_, _> = initial_acceptors
            .into_iter()
            .filter(|(id, _)| *id != own_id)
            .collect();

        let total_acceptors = peer_acceptors.len() + 1;

        Self {
            own_id,
            group_id,
            endpoint,
            peer_acceptors,
            quorum_tracker: QuorumTracker::new(total_acceptors),
            peer_rx,
            peer_tx,
            epoch_tx,
            peer_handles: BTreeMap::new(),
        }
    }

    pub(crate) async fn run(mut self, state: GroupStateStore, initial_round: Epoch) {
        let mut local_subscription =
            AcceptorStateStore::<GroupAcceptor<C, CS>>::subscribe_from(&state, initial_round).await;

        let peers: Vec<_> = self
            .peer_acceptors
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        for (id, addr) in peers {
            self.spawn_peer_actor(id, addr, initial_round);
        }

        loop {
            tokio::select! {
                Some((proposal, message)) = local_subscription.next() => {
                    self.handle_accepted(self.own_id, proposal, message);
                }

                Some(event) = self.peer_rx.recv() => {
                    match event {
                        PeerEvent::Accepted { acceptor_id, proposal, message } => {
                            self.handle_accepted(acceptor_id, proposal, *message);
                        }
                        PeerEvent::Disconnected { acceptor_id, error } => {
                            tracing::debug!(?acceptor_id, %error, "peer disconnected (will reconnect)");
                        }
                    }
                }

                else => break,
            }
        }

        for handle in self.peer_handles.values() {
            handle.abort();
        }
        self.peer_handles.clear();
    }

    fn handle_accepted(
        &mut self,
        _acceptor_id: AcceptorId,
        proposal: GroupProposal,
        message: GroupMessage,
    ) {
        let current_epoch = *self.epoch_tx.borrow();

        if proposal.epoch < current_epoch {
            return;
        }

        if let Some((p, _m)) = self.quorum_tracker.track(proposal, message) {
            let new_epoch = Epoch(p.epoch.0 + 1);
            tracing::info!(epoch = ?p.epoch, "quorum reached");
            let _ = self.epoch_tx.send(new_epoch);
        }
    }

    fn spawn_peer_actor(&mut self, id: AcceptorId, addr: EndpointAddr, current_round: Epoch) {
        let endpoint = self.endpoint.clone();
        let group_id = self.group_id;
        let tx = self.peer_tx.clone();

        let handle = tokio::spawn(async move {
            run_peer_actor(endpoint, addr, group_id, id, current_round, tx).await;
        });

        self.peer_handles.insert(id, handle);
    }
}

const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

async fn run_peer_actor(
    endpoint: Endpoint,
    addr: EndpointAddr,
    group_id: GroupId,
    acceptor_id: AcceptorId,
    initial_round: Epoch,
    tx: mpsc::Sender<PeerEvent>,
) {
    let mut reconnect_attempts = 0u32;
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    let mut current_round = initial_round;

    loop {
        match run_peer_connection(&endpoint, &addr, group_id, acceptor_id, current_round, &tx).await
        {
            Ok(last_epoch) => {
                current_round = last_epoch;
                reconnect_attempts = 0;
                reconnect_delay = INITIAL_RECONNECT_DELAY;
            }
            Err(e) => {
                reconnect_attempts += 1;

                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    tracing::warn!(
                        ?acceptor_id,
                        %e,
                        "max reconnect attempts reached, giving up on peer"
                    );
                    let _ = tx
                        .send(PeerEvent::Disconnected {
                            acceptor_id,
                            error: format!("max reconnects exceeded: {e}"),
                        })
                        .await;
                    return;
                }

                tracing::debug!(
                    ?acceptor_id,
                    attempt = reconnect_attempts,
                    delay_ms = reconnect_delay.as_millis(),
                    %e,
                    "reconnecting to peer acceptor"
                );
            }
        }

        let _ = tx
            .send(PeerEvent::Disconnected {
                acceptor_id,
                error: "connection closed, reconnecting".to_string(),
            })
            .await;

        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn run_peer_connection(
    endpoint: &Endpoint,
    addr: &EndpointAddr,
    group_id: GroupId,
    acceptor_id: AcceptorId,
    current_round: Epoch,
    tx: &mpsc::Sender<PeerEvent>,
) -> Result<Epoch, ConnectorError> {
    use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
    use universal_sync_core::codec::PostcardCodec;
    use universal_sync_core::{Handshake, HandshakeResponse};

    tracing::debug!(?acceptor_id, "connecting to peer acceptor");

    // Connect to the peer acceptor
    let conn = endpoint
        .connect(addr.clone(), PAXOS_ALPN)
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    // Open a bidirectional stream
    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| ConnectorError::Connect(e.to_string()))?;

    // Handshake
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(16 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(recv, codec.clone());
    let mut writer = FramedWrite::new(send, codec);

    // Send Join handshake
    let handshake = Handshake::JoinProposals(group_id);
    let handshake_bytes =
        postcard::to_allocvec(&handshake).map_err(|e| ConnectorError::Codec(e.to_string()))?;
    writer
        .send(handshake_bytes.into())
        .await
        .map_err(ConnectorError::Io)?;

    // Read response
    let response_bytes = reader
        .next()
        .await
        .ok_or_else(|| ConnectorError::Handshake("connection closed".to_string()))?
        .map_err(ConnectorError::Io)?;

    let response: HandshakeResponse = postcard::from_bytes(&response_bytes)
        .map_err(|e| ConnectorError::Codec(format!("invalid response: {e}")))?;

    match response {
        HandshakeResponse::Ok => {}
        other => {
            return Err(ConnectorError::Handshake(format!("{other:?}")));
        }
    }

    // Extract streams and create protocol readers/writers
    let recv = reader.into_inner();
    let send = writer.into_inner();

    let mut reader: FramedRead<iroh::endpoint::RecvStream, PostcardCodec<ProposalResponse>> =
        FramedRead::new(recv, PostcardCodec::new());
    let mut writer: FramedWrite<iroh::endpoint::SendStream, PostcardCodec<ProposalRequest>> =
        FramedWrite::new(send, PostcardCodec::new());

    // Send a sync Prepare to initiate the learning stream
    // Use attempt=0 to indicate this is a learning request
    let sync_proposal = GroupProposal {
        member_id: universal_sync_core::MemberId(u32::MAX),
        epoch: current_round,
        attempt: universal_sync_core::Attempt::default(),
        message_hash: [0u8; 32], // Will be filled by actual proposer
        signature: vec![],
    };

    writer
        .send(ProposalRequest::Prepare(sync_proposal))
        .await
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e)))?;

    tracing::debug!(?acceptor_id, "learning stream established");

    // Track the last epoch we saw for resumption
    let mut last_epoch = current_round;

    // Read accepted values and forward to learning actor
    while let Some(result) = reader.next().await {
        let response = result.map_err(|e| ConnectorError::Io(std::io::Error::other(e)))?;

        if let Some((proposal, message)) = response.accepted {
            // Update last epoch for resumption
            if proposal.epoch > last_epoch {
                last_epoch = proposal.epoch;
            }

            tx.send(PeerEvent::Accepted {
                acceptor_id,
                proposal,
                message: Box::new(message),
            })
            .await
            .map_err(|_| ConnectorError::Io(std::io::Error::other("channel closed")))?;
        }
    }

    Ok(last_epoch)
}
