//! Acceptor run loop.

use std::pin::pin;

use futures::stream::FusedStream;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, instrument, trace, warn};

use super::handler::{AcceptOutcome, AcceptorHandler, InvalidProposal, PromiseOutcome};
use crate::Learner;
use crate::fuse::Fuse;
use crate::messages::{AcceptorMessage, AcceptorRequest};
use crate::traits::{AcceptorStateStore, Proposal};

/// Run the acceptor loop with epoch-aware waiting.
///
/// Waits for the learner to catch up when receiving proposals for future epochs.
/// Needed when proposal validation depends on learned state (e.g., MLS roster
/// for signature validation).
#[allow(clippy::too_many_lines)]
#[instrument(skip_all, name = "acceptor_epoch_aware", fields(node_id = ?handler.node_id(), proposer = ?proposer_id))]
pub async fn run_acceptor_with_epoch_waiter<A, S, C>(
    mut handler: AcceptorHandler<A, S>,
    conn: C,
    proposer_id: <A::Proposal as Proposal>::NodeId,
    mut epoch_rx: watch::Receiver<<A::Proposal as Proposal>::RoundId>,
    mut current_epoch_fn: impl FnMut() -> <A::Proposal as Proposal>::RoundId,
) -> Result<(), A::Error>
where
    A: Learner,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>,
{
    debug!("acceptor with epoch waiter started");

    // sync is terminated until we receive the initial Prepare
    let mut sync = pin!(Fuse::<S::Subscription>::terminated());
    let mut conn = pin!(conn);

    loop {
        let msg = select! {
            msg = conn.next() => msg,
            Some((proposal, message)) = sync.next() => {
                trace!(round = ?proposal.round(), "forwarding accepted value");
                conn.send(AcceptorMessage {
                    promised: proposal.clone(),
                    accepted: Some((proposal, message)),
                }).await?;
                continue;
            }
        };

        let Some(msg) = msg else {
            debug!("connection closed");
            return Ok(());
        };

        match msg? {
            AcceptorRequest::Prepare(proposal) => {
                let proposal_round = proposal.round();
                trace!(?proposal_round, "received prepare");

                let mut current = current_epoch_fn();
                if proposal_round > current {
                    debug!(
                        current = ?current,
                        target = ?proposal_round,
                        "waiting for learning to catch up"
                    );
                    loop {
                        if epoch_rx.changed().await.is_err() {
                            debug!("epoch notifier closed");
                            return Ok(());
                        }
                        let new_epoch = current_epoch_fn();
                        if new_epoch >= proposal_round {
                            debug!(epoch = ?new_epoch, "caught up, applying learned values");
                            let learned = handler.state().get_accepted_from(current).await;
                            for (p, m) in learned {
                                if let Err(e) = handler.acceptor_mut().apply(p, m).await {
                                    warn!(?e, "failed to apply learned value");
                                }
                            }
                            current = new_epoch;
                            break;
                        }
                    }
                }

                let response = match handler.handle_prepare(&proposal).await {
                    Ok(PromiseOutcome::Promised(msg)) => {
                        debug!(?proposal_round, "promised");
                        if sync.is_terminated() {
                            sync.set(Fuse::new(
                                handler.state().subscribe_from(proposal_round).await,
                            ));
                            debug!("subscribed to state");
                        }
                        msg
                    }
                    Ok(PromiseOutcome::Outdated(msg)) => {
                        trace!("promise rejected - outdated");
                        // Still subscribe so the learner can sync
                        if sync.is_terminated() {
                            sync.set(Fuse::new(
                                handler.state().subscribe_from(proposal_round).await,
                            ));
                            debug!("subscribed to state");
                        }
                        if msg.promised.round() != proposal_round {
                            continue;
                        }
                        msg
                    }
                    Err(InvalidProposal) => {
                        warn!("rejecting invalid prepare");
                        continue;
                    }
                };

                conn.send(response).await?;
            }
            AcceptorRequest::Accept(proposal, message) => {
                if sync.is_terminated() {
                    trace!("ignoring accept before initial prepare");
                    continue;
                }

                let proposal_round = proposal.round();
                trace!(?proposal_round, "received accept");

                let mut current = current_epoch_fn();
                if proposal_round > current {
                    debug!(
                        current = ?current,
                        target = ?proposal_round,
                        "waiting for learning to catch up for accept"
                    );
                    loop {
                        if epoch_rx.changed().await.is_err() {
                            debug!("epoch notifier closed");
                            return Ok(());
                        }
                        let new_epoch = current_epoch_fn();
                        if new_epoch >= proposal_round {
                            debug!(epoch = ?new_epoch, "caught up, applying learned values");
                            let learned = handler.state().get_accepted_from(current).await;
                            for (p, m) in learned {
                                if let Err(e) = handler.acceptor_mut().apply(p, m).await {
                                    warn!(?e, "failed to apply learned value");
                                }
                            }
                            current = new_epoch;
                            break;
                        }
                    }
                }

                let response = match handler.handle_accept(&proposal, &message).await {
                    Ok(AcceptOutcome::Accepted(msg)) => {
                        debug!(?proposal_round, "accepted");
                        msg
                    }
                    Ok(AcceptOutcome::Outdated(msg)) => {
                        trace!("accept rejected - outdated");
                        if msg.promised.round() != proposal_round {
                            continue;
                        }
                        msg
                    }
                    Err(InvalidProposal) => {
                        warn!("rejecting invalid accept");
                        continue;
                    }
                };

                conn.send(response).await?;
            }
        }
    }
}
