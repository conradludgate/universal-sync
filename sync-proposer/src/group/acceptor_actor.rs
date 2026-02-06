use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use universal_sync_core::{AcceptorId, GroupId, MessageRequest, MessageResponse};

use super::{AcceptorInbound, AcceptorOutbound};
use crate::connection::ConnectionManager;

pub(super) struct AcceptorActor {
    pub(super) acceptor_id: AcceptorId,
    pub(super) group_id: GroupId,
    pub(super) connection_manager: ConnectionManager,
    pub(super) outbound_rx: mpsc::Receiver<AcceptorOutbound>,
    pub(super) inbound_tx: mpsc::Sender<AcceptorInbound>,
    pub(super) cancel_token: tokio_util::sync::CancellationToken,
}

const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(10);

enum ConnectionResult {
    Cancelled,
    Disconnected,
}

impl AcceptorActor {
    pub(super) async fn run(mut self) {
        let mut reconnect_attempts = 0u32;
        let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

            match self.run_connection().await {
                ConnectionResult::Cancelled => break,
                ConnectionResult::Disconnected => {
                    reconnect_attempts += 1;
                    if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                        tracing::warn!(
                            acceptor_id = ?self.acceptor_id,
                            "max reconnect attempts reached, giving up"
                        );
                        break;
                    }

                    tracing::debug!(
                        acceptor_id = ?self.acceptor_id,
                        attempt = reconnect_attempts,
                        delay_ms = reconnect_delay.as_millis(),
                        "reconnecting to acceptor"
                    );

                    tokio::select! {
                        () = tokio::time::sleep(reconnect_delay) => {}
                        () = self.cancel_token.cancelled() => break,
                    }

                    reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
                }
            }
        }

        let _ = self
            .inbound_tx
            .send(AcceptorInbound::Disconnected {
                acceptor_id: self.acceptor_id,
            })
            .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn run_connection(&mut self) -> ConnectionResult {
        use futures::{SinkExt, StreamExt};

        let proposal_streams = self
            .connection_manager
            .open_proposal_stream(&self.acceptor_id, self.group_id)
            .await;

        let (proposal_send, proposal_recv) = match proposal_streams {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to open proposal stream");
                return ConnectionResult::Disconnected;
            }
        };

        let (mut proposal_writer, mut proposal_reader) =
            crate::connector::make_proposal_streams(proposal_send, proposal_recv);

        let message_streams = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            self.connection_manager
                .open_message_stream(&self.acceptor_id, self.group_id),
        )
        .await;

        let message_io: Option<(
            FramedWrite<iroh::endpoint::SendStream, LengthDelimitedCodec>,
            FramedRead<iroh::endpoint::RecvStream, LengthDelimitedCodec>,
        )> = match message_streams {
            Ok(Ok((message_send, message_recv))) => {
                let mut message_writer =
                    FramedWrite::new(message_send, LengthDelimitedCodec::new());
                let message_reader = FramedRead::new(message_recv, LengthDelimitedCodec::new());

                // Request a backfill of all historical messages (empty state vector
                // means "give me everything"). New messages will arrive via the
                // broadcast subscription which is always active on the server side.
                let backfill_request = MessageRequest::Backfill {
                    state_vector: Default::default(),
                    limit: u32::MAX,
                };
                if let Ok(request_bytes) = postcard::to_allocvec(&backfill_request) {
                    if message_writer.send(request_bytes.into()).await.is_ok() {
                        Some((message_writer, message_reader))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Ok(Err(_)) | Err(_) => None,
        };

        let (mut message_writer_opt, mut message_reader_opt) = match message_io {
            Some((w, r)) => (Some(w), Some(r)),
            None => (None, None),
        };

        loop {
            tokio::select! {
                biased;

                () = self.cancel_token.cancelled() => {
                    return ConnectionResult::Cancelled;
                }

                Some(outbound) = self.outbound_rx.recv() => {
                    match outbound {
                        AcceptorOutbound::ProposalRequest { request } => {
                            if let Err(e) = proposal_writer.send(request).await {
                                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to send proposal");
                                return ConnectionResult::Disconnected;
                            }
                        }
                        AcceptorOutbound::AppMessage { id, msg } => {
                            if let Some(ref mut writer) = message_writer_opt {
                                let request = MessageRequest::Send { id, message: msg };
                                if let Ok(request_bytes) = postcard::to_allocvec(&request) {
                                    let _ = writer.send(request_bytes.into()).await;
                                }
                            }
                        }
                    }
                }

                Some(result) = proposal_reader.next() => {
                    match result {
                        Ok(response) => {
                            let _ = self.inbound_tx.send(AcceptorInbound::ProposalResponse {
                                acceptor_id: self.acceptor_id,
                                response,
                            }).await;
                        }
                        Err(e) => {
                            tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "proposal stream error");
                            return ConnectionResult::Disconnected;
                        }
                    }
                }

                Some(result) = async {
                    if let Some(ref mut reader) = message_reader_opt {
                        reader.next().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Ok(bytes) = result {
                        match postcard::from_bytes::<MessageResponse>(&bytes) {
                            Ok(MessageResponse::Message { message, .. }) => {
                                let _ = self.inbound_tx.send(AcceptorInbound::EncryptedMessage {
                                    msg: message,
                                }).await;
                            }
                            Ok(MessageResponse::BackfillComplete { .. }) => {
                                // Backfill complete — new messages arrive via broadcast
                            }
                            Ok(MessageResponse::Stored) => {
                                // Acknowledgment for a stored message
                            }
                            Ok(MessageResponse::Error(e)) => {
                                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "message stream error from acceptor");
                            }
                            Err(e) => {
                                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to decode message response");
                            }
                        }
                    } else {
                        message_reader_opt = None;
                        message_writer_opt = None;
                    }
                }
            }
        }
    }
}
