use filament_core::codec::VersionedCodec;
use filament_core::{
    AcceptorId, Epoch, GroupId, MemberFingerprint, MessageRequest, MessageResponse, StateVector,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

type MessageWriter = FramedWrite<iroh::endpoint::SendStream, VersionedCodec<MessageRequest>>;
type MessageReader = FramedRead<iroh::endpoint::RecvStream, VersionedCodec<MessageResponse>>;

use super::{AcceptorInbound, AcceptorOutbound};
use crate::connection::ConnectionManager;

pub(super) struct AcceptorActor {
    pub(super) acceptor_id: AcceptorId,
    pub(super) group_id: GroupId,
    pub(super) current_epoch: Epoch,
    pub(super) own_fingerprint: MemberFingerprint,
    pub(super) connection_manager: ConnectionManager,
    pub(super) outbound_rx: mpsc::Receiver<AcceptorOutbound>,
    pub(super) inbound_tx: mpsc::Sender<AcceptorInbound>,
    pub(super) cancel_token: tokio_util::sync::CancellationToken,
    pub(super) protocol_version: u32,
}

const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(10);

enum ConnectionResult {
    Cancelled,
    Disconnected { was_connected: bool },
}

impl AcceptorActor {
    pub(super) async fn run(mut self) {
        let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

            match self.run_connection().await {
                ConnectionResult::Cancelled => break,
                ConnectionResult::Disconnected { was_connected } => {
                    if was_connected {
                        reconnect_delay = INITIAL_RECONNECT_DELAY;
                    }

                    tracing::debug!(
                        acceptor_id = ?self.acceptor_id,
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
    }

    #[allow(clippy::too_many_lines)]
    async fn run_connection(&mut self) -> ConnectionResult {
        use futures::{SinkExt, StreamExt};

        let proposal_streams = self
            .connection_manager
            .open_proposal_stream(&self.acceptor_id, self.group_id, self.current_epoch)
            .await;

        let (proposal_writer, proposal_reader) = match proposal_streams {
            Ok(streams) => streams,
            Err(e) => {
                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to open proposal stream");
                return ConnectionResult::Disconnected {
                    was_connected: false,
                };
            }
        };

        let (mut proposal_writer, mut proposal_reader) =
            crate::connector::make_proposal_streams(proposal_writer, proposal_reader);

        let message_streams = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            self.connection_manager.open_message_stream(
                &self.acceptor_id,
                self.group_id,
                self.own_fingerprint,
            ),
        )
        .await;

        let message_io: Option<(MessageWriter, MessageReader)> = match message_streams {
            Ok(Ok((handshake_writer, handshake_reader))) => {
                let pv = self.protocol_version;
                let mut message_writer =
                    handshake_writer.map_encoder(|ldc| VersionedCodec::wrap(ldc, pv));
                let message_reader =
                    handshake_reader.map_decoder(|ldc| VersionedCodec::wrap(ldc, pv));

                let backfill_request = MessageRequest::Backfill {
                    state_vector: StateVector::default(),
                    limit: u32::MAX,
                };
                if message_writer.send(backfill_request).await.is_ok() {
                    Some((message_writer, message_reader))
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

        let _ = self
            .inbound_tx
            .send(AcceptorInbound::Connected {
                acceptor_id: self.acceptor_id,
            })
            .await;

        let result = loop {
            tokio::select! {
                biased;

                () = self.cancel_token.cancelled() => {
                    break ConnectionResult::Cancelled;
                }

                Some(outbound) = self.outbound_rx.recv() => {
                    match outbound {
                        AcceptorOutbound::ProposalRequest { request } => {
                            if let Err(e) = proposal_writer.send(request).await {
                                tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to send proposal");
                                break ConnectionResult::Disconnected { was_connected: true };
                            }
                        }
                        AcceptorOutbound::AppMessage { id, level, msg } => {
                            if let Some(ref mut writer) = message_writer_opt {
                                let request = MessageRequest::Send { id, level, message: msg };
                                let _ = writer.send(request).await;
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
                            break ConnectionResult::Disconnected { was_connected: true };
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
                    match result {
                        Ok(MessageResponse::Message { message, .. }) => {
                            let _ = self.inbound_tx.send(AcceptorInbound::EncryptedMessage {
                                msg: message,
                            }).await;
                        }
                        Ok(MessageResponse::BackfillComplete { .. }
                            | MessageResponse::Stored) => {}

                        Ok(MessageResponse::Error(e)) => {
                            tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "message stream error from acceptor");
                        }
                        Err(e) => {
                            tracing::warn!(acceptor_id = ?self.acceptor_id, ?e, "failed to decode message response");
                            message_reader_opt = None;
                            message_writer_opt = None;
                        }
                    }
                }
            }
        };

        if matches!(result, ConnectionResult::Disconnected { .. }) {
            let _ = self
                .inbound_tx
                .send(AcceptorInbound::Disconnected {
                    acceptor_id: self.acceptor_id,
                })
                .await;
        }

        result
    }
}
