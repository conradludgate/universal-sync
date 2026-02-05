//! Acceptor learning infrastructure
//!
//! This module provides the learning capability for acceptors in a multi-acceptor
//! deployment. Each acceptor acts as a learner, connecting to peer acceptors to:
//!
//! 1. Receive accepted values from peers
//! 2. Track quorum across all acceptors (including itself)
//! 3. Apply values when quorum is reached
//!
//! # Architecture
//!
//! ```text
//!                    ┌─────────────────────────────────────┐
//!                    │         GroupLearningActor          │
//!                    │                                     │
//!  Local accepts ───►│  ┌─────────────────────────────┐   │
//!                    │  │      QuorumTracker          │   │
//!  Peer streams ────►│  │  (tracks quorum per round)  │   │──► apply() on quorum
//!                    │  └─────────────────────────────┘   │
//!                    │                                     │
//!                    │  ┌─────────────────────────────┐   │
//!                    │  │  PeerAcceptorActor (each)   │   │
//!                    │  │  - Connects to peer         │   │
//!                    │  │  - Sends sync proposals     │   │
//!                    │  │  - Receives accepts         │   │
//!                    │  └─────────────────────────────┘   │
//!                    └─────────────────────────────────────┘
//! ```
//!
//! # Learning Protocol
//!
//! Learners act as "pseudo-proposers" by sending a sync proposal (Prepare)
//! to initiate the learning stream from each acceptor. The acceptor responds
//! with all accepted values from that round onwards.
//!
//! This reuses the existing Paxos protocol - the Prepare message initiates
//! the stream, and the acceptor sends back accepted values as they occur.

use std::collections::BTreeMap;

use futures::SinkExt;
use iroh::{Endpoint, EndpointAddr};
use tokio::sync::mpsc;
use universal_sync_core::{AcceptorId, Epoch, GroupId, GroupMessage, GroupProposal, PAXOS_ALPN};
use universal_sync_paxos::Learner;
use universal_sync_paxos::proposer::QuorumTracker;

use crate::acceptor::{AcceptorChangeEvent, AcceptorError, GroupAcceptor};
use crate::connector::ConnectorError;

/// Events sent from peer acceptor actors to the learning coordinator
#[derive(Debug)]
pub(crate) enum PeerEvent {
    /// Peer accepted a value
    Accepted {
        /// The acceptor ID that sent this
        acceptor_id: AcceptorId,
        /// The accepted proposal
        proposal: GroupProposal,
        /// The accepted message (boxed to reduce enum size)
        message: Box<GroupMessage>,
    },
    /// Peer connection failed
    Disconnected {
        /// The acceptor ID that disconnected
        acceptor_id: AcceptorId,
        /// Error message
        error: String,
    },
}

/// Commands to the learning actor
#[derive(Debug)]
pub(crate) enum LearningCommand {
    /// Local acceptor accepted a value
    LocalAccepted {
        /// The accepted proposal
        proposal: GroupProposal,
        /// The accepted message (boxed to reduce enum size)
        message: Box<GroupMessage>,
    },
    /// Acceptor set changed
    AcceptorChanged(AcceptorChangeEvent),
    /// Shutdown
    Shutdown,
}

/// Events emitted by the learning actor
#[derive(Debug)]
pub(crate) enum LearningEvent {
    /// A value reached quorum and was applied
    Learned {
        /// The new epoch after applying
        epoch: Epoch,
    },
    /// An acceptor change was processed
    AcceptorChange(AcceptorChangeEvent),
}

/// Learning actor that coordinates quorum tracking across acceptors
///
/// This actor:
/// - Manages connections to peer acceptors
/// - Tracks accepted values from all sources (local + peers)
/// - Applies values when quorum is reached
/// - Updates peer connections when the acceptor set changes
pub(crate) struct GroupLearningActor<C, CS>
where
    C: mls_rs::external_client::builder::MlsConfig + Clone + 'static,
    CS: mls_rs::CipherSuiteProvider + 'static,
{
    /// This acceptor's ID
    own_id: AcceptorId,
    /// The group ID
    group_id: GroupId,
    /// Iroh endpoint for peer connections
    endpoint: Endpoint,
    /// Current acceptor addresses (excluding self)
    peer_acceptors: BTreeMap<AcceptorId, EndpointAddr>,
    /// Quorum tracker
    quorum_tracker: QuorumTracker<GroupAcceptor<C, CS>>,
    /// Channel for receiving peer events
    peer_rx: mpsc::Receiver<PeerEvent>,
    /// Channel sender for peer actors to send events
    peer_tx: mpsc::Sender<PeerEvent>,
    /// Channel for receiving commands
    command_rx: mpsc::Receiver<LearningCommand>,
    /// Channel for sending events
    event_tx: mpsc::Sender<LearningEvent>,
    /// Current peer actor handles (for cleanup)
    peer_handles: BTreeMap<AcceptorId, tokio::task::JoinHandle<()>>,
}

impl<C, CS> GroupLearningActor<C, CS>
where
    C: mls_rs::external_client::builder::MlsConfig + Clone + Send + Sync + 'static,
    CS: mls_rs::CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new learning actor
    ///
    /// # Arguments
    /// * `own_id` - This acceptor's ID
    /// * `group_id` - The group being learned
    /// * `endpoint` - Iroh endpoint for peer connections
    /// * `initial_acceptors` - Initial set of acceptor addresses
    /// * `command_rx` - Channel to receive commands
    /// * `event_tx` - Channel to send events
    #[must_use]
    pub(crate) fn new(
        own_id: AcceptorId,
        group_id: GroupId,
        endpoint: Endpoint,
        initial_acceptors: impl IntoIterator<Item = (AcceptorId, EndpointAddr)>,
        command_rx: mpsc::Receiver<LearningCommand>,
        event_tx: mpsc::Sender<LearningEvent>,
    ) -> Self {
        let (peer_tx, peer_rx) = mpsc::channel(256);

        // Filter out self from peer acceptors
        let peer_acceptors: BTreeMap<_, _> = initial_acceptors
            .into_iter()
            .filter(|(id, _)| *id != own_id)
            .collect();

        // Total acceptors includes self
        let total_acceptors = peer_acceptors.len() + 1;

        Self {
            own_id,
            group_id,
            endpoint,
            peer_acceptors,
            quorum_tracker: QuorumTracker::new(total_acceptors),
            peer_rx,
            peer_tx,
            command_rx,
            event_tx,
            peer_handles: BTreeMap::new(),
        }
    }

    /// Run the learning actor
    ///
    /// This spawns peer actors for each peer acceptor and coordinates
    /// quorum tracking and application.
    ///
    /// # Errors
    ///
    /// Returns an error if applying a learned value to the acceptor fails.
    pub(crate) async fn run<A>(mut self, acceptor: &mut A) -> Result<(), AcceptorError>
    where
        A: Learner<
                Proposal = GroupProposal,
                Message = GroupMessage,
                AcceptorId = AcceptorId,
                Error = AcceptorError,
            >,
    {
        // Spawn peer actors for initial acceptors
        let current_round = acceptor.current_round();
        let peers: Vec<_> = self
            .peer_acceptors
            .iter()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();
        for (id, addr) in peers {
            self.spawn_peer_actor(id, addr, current_round);
        }

        loop {
            tokio::select! {
                // Handle peer events
                Some(event) = self.peer_rx.recv() => {
                    match event {
                        PeerEvent::Accepted { acceptor_id, proposal, message } => {
                            self.handle_peer_accepted(
                                acceptor,
                                acceptor_id,
                                proposal,
                                *message,
                            ).await?;
                        }
                        PeerEvent::Disconnected { acceptor_id, error } => {
                            tracing::warn!(?acceptor_id, %error, "peer disconnected");
                            // Could implement reconnection logic here
                        }
                    }
                }

                // Handle commands
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        LearningCommand::LocalAccepted { proposal, message } => {
                            // Track local accept
                            self.handle_peer_accepted(
                                acceptor,
                                self.own_id,
                                proposal,
                                *message,
                            ).await?;
                        }
                        LearningCommand::AcceptorChanged(change) => {
                            self.handle_acceptor_change(change, acceptor.current_round());
                        }
                        LearningCommand::Shutdown => {
                            break;
                        }
                    }
                }

                else => break,
            }
        }

        // Cleanup: abort peer actors
        for handle in self.peer_handles.values() {
            handle.abort();
        }
        self.peer_handles.clear();

        Ok(())
    }

    /// Handle an accepted value (from local or peer)
    async fn handle_peer_accepted<A>(
        &mut self,
        acceptor: &mut A,
        _acceptor_id: AcceptorId,
        proposal: GroupProposal,
        message: GroupMessage,
    ) -> Result<(), AcceptorError>
    where
        A: Learner<
                Proposal = GroupProposal,
                Message = GroupMessage,
                AcceptorId = AcceptorId,
                Error = AcceptorError,
            >,
    {
        let current_round = acceptor.current_round();

        // Skip old rounds
        if proposal.epoch < current_round {
            return Ok(());
        }

        // Track and check for quorum
        if let Some((p, m)) = self.quorum_tracker.track(proposal, message) {
            // Quorum reached - apply
            tracing::info!(epoch = ?p.epoch, "quorum reached, applying");
            acceptor.apply(p.clone(), m.clone()).await?;

            // Notify
            let _ = self
                .event_tx
                .send(LearningEvent::Learned { epoch: p.epoch })
                .await;
        }

        Ok(())
    }

    /// Handle acceptor set change
    fn handle_acceptor_change(&mut self, change: AcceptorChangeEvent, current_round: Epoch) {
        match &change {
            AcceptorChangeEvent::Added { id, addr } => {
                if *id != self.own_id {
                    self.peer_acceptors.insert(*id, addr.clone());
                    self.spawn_peer_actor(*id, addr.clone(), current_round);
                }
            }
            AcceptorChangeEvent::Removed { id } => {
                self.peer_acceptors.remove(id);
                if let Some(handle) = self.peer_handles.remove(id) {
                    handle.abort();
                }
            }
        }

        // Update quorum tracker with new count
        let total = self.peer_acceptors.len() + 1;
        self.quorum_tracker = QuorumTracker::new(total);

        // Notify
        let _ = self
            .event_tx
            .try_send(LearningEvent::AcceptorChange(change));
    }

    /// Spawn a peer actor for connecting to a peer acceptor
    fn spawn_peer_actor(&mut self, id: AcceptorId, addr: EndpointAddr, current_round: Epoch) {
        let endpoint = self.endpoint.clone();
        let group_id = self.group_id;
        let tx = self.peer_tx.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_peer_actor(endpoint, addr, group_id, id, current_round, tx).await {
                tracing::warn!(?id, %e, "peer actor failed");
            }
        });

        self.peer_handles.insert(id, handle);
    }
}

/// Run a peer acceptor actor
///
/// Connects to a peer acceptor, sends a sync proposal to initiate the
/// learning stream, and forwards received accepts to the learning actor.
async fn run_peer_actor(
    endpoint: Endpoint,
    addr: EndpointAddr,
    group_id: GroupId,
    acceptor_id: AcceptorId,
    current_round: Epoch,
    tx: mpsc::Sender<PeerEvent>,
) -> Result<(), ConnectorError> {
    use futures::StreamExt;
    use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
    use universal_sync_core::codec::PostcardCodec;
    use universal_sync_core::{Handshake, HandshakeResponse};

    tracing::debug!(?acceptor_id, "connecting to peer acceptor");

    // Connect to the peer acceptor
    let conn = endpoint
        .connect(addr, PAXOS_ALPN)
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

    let mut reader: FramedRead<
        iroh::endpoint::RecvStream,
        PostcardCodec<crate::connector::ProposalResponse>,
    > = FramedRead::new(recv, PostcardCodec::new());
    let mut writer: FramedWrite<
        iroh::endpoint::SendStream,
        PostcardCodec<crate::connector::ProposalRequest>,
    > = FramedWrite::new(send, PostcardCodec::new());

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
        .send(crate::connector::ProposalRequest::Prepare(sync_proposal))
        .await
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e)))?;

    tracing::debug!(?acceptor_id, "learning stream established");

    // Read accepted values and forward to learning actor
    while let Some(result) = reader.next().await {
        let response = result.map_err(|e| ConnectorError::Io(std::io::Error::other(e)))?;

        if let Some((proposal, message)) = response.accepted {
            tx.send(PeerEvent::Accepted {
                acceptor_id,
                proposal,
                message: Box::new(message),
            })
            .await
            .map_err(|_| ConnectorError::Io(std::io::Error::other("channel closed")))?;
        }
    }

    // Connection closed
    let _ = tx
        .send(PeerEvent::Disconnected {
            acceptor_id,
            error: "connection closed".to_string(),
        })
        .await;

    Ok(())
}

/// Create channels for a learning actor
#[must_use]
pub(crate) fn learning_channels() -> (
    mpsc::Sender<LearningCommand>,
    mpsc::Receiver<LearningCommand>,
    mpsc::Sender<LearningEvent>,
    mpsc::Receiver<LearningEvent>,
) {
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (event_tx, event_rx) = mpsc::channel(64);
    (cmd_tx, cmd_rx, event_tx, event_rx)
}
