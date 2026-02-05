//! Acceptor run loop

use std::pin::pin;

use futures::stream::FusedStream;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, instrument, trace, warn};

use super::handler::{AcceptOutcome, AcceptorHandler, InvalidProposal, PromiseOutcome};
use crate::fuse::Fuse;
use crate::messages::{AcceptorMessage, AcceptorRequest};
use crate::traits::{Acceptor, AcceptorStateStore, Proposal};

/// Run the acceptor loop with shared state.
///
/// When a `Prepare` is received, the acceptor:
/// 1. Handles the prepare and subscribes to the state
/// 2. Sends the promise response
/// 3. Forwards historical + live accepted values via the subscription
///
/// Uses shared state allowing multiple connections to the same acceptor to
/// properly coordinate promises and accepts. When a proposal is accepted,
/// the state automatically broadcasts to all subscribed learners.
///
/// # Errors
///
/// Returns an error if:
/// - Communication with the client fails
/// - The acceptor fails to persist an accepted proposal
#[instrument(skip_all, name = "acceptor", fields(node_id = ?handler.node_id(), proposer = ?proposer_id))]
pub async fn run_acceptor<A, S, C>(
    mut handler: AcceptorHandler<A, S>,
    conn: C,
    proposer_id: <A::Proposal as Proposal>::NodeId,
) -> Result<(), A::Error>
where
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>,
{
    debug!("acceptor started");

    // sync is terminated until we receive the initial Prepare
    let mut sync = pin!(Fuse::<S::Subscription>::terminated());
    let mut conn = pin!(conn);

    loop {
        // Poll both conn and sync
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

        // Handle connection message
        let Some(msg) = msg else {
            debug!("connection closed");
            return Ok(());
        };

        match msg? {
            AcceptorRequest::Prepare(proposal) => {
                trace!(round = ?proposal.round(), "received prepare");

                let response = match handler.handle_prepare(&proposal).await {
                    Ok(PromiseOutcome::Promised(msg)) => {
                        debug!(round = ?proposal.round(), "promised");
                        // Subscribe on first successful promise
                        if sync.is_terminated() {
                            sync.set(Fuse::new(
                                handler.state().subscribe_from(proposal.round()).await,
                            ));
                            debug!("subscribed to state");
                        }
                        msg
                    }
                    Ok(PromiseOutcome::Outdated(msg)) => {
                        trace!("promise rejected - outdated");
                        // Also subscribe on first outdated (learner still needs sync)
                        if sync.is_terminated() {
                            sync.set(Fuse::new(
                                handler.state().subscribe_from(proposal.round()).await,
                            ));
                            debug!("subscribed to state");
                        }
                        if msg.promised.round() != proposal.round() {
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
                // Ignore Accept before initial Prepare
                if sync.is_terminated() {
                    trace!("ignoring accept before initial prepare");
                    continue;
                }

                trace!(round = ?proposal.round(), "received accept");

                let response = match handler.handle_accept(&proposal, &message).await {
                    Ok(AcceptOutcome::Accepted(msg)) => {
                        debug!(round = ?proposal.round(), "accepted");
                        msg
                    }
                    Ok(AcceptOutcome::Outdated(msg)) => {
                        trace!("accept rejected - outdated");
                        if msg.promised.round() != proposal.round() {
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

/// Run the acceptor loop with epoch-aware waiting.
///
/// Like [`run_acceptor`], but additionally waits for the learner to catch up
/// when receiving proposals for future epochs. This is needed when proposal
/// validation depends on learned state (e.g., MLS roster for signature validation).
///
/// The `epoch_rx` channel should receive notifications whenever a new epoch
/// is learned. The acceptor will wait on this channel when it receives a
/// proposal for an epoch beyond the learner's current round.
///
/// # Arguments
///
/// * `handler` - The acceptor handler
/// * `conn` - The connection stream/sink
/// * `proposer_id` - The ID of the proposer
/// * `epoch_rx` - Watch channel that receives the current learned epoch
/// * `current_epoch_fn` - Function to get the current learned epoch
///
/// # Errors
///
/// Returns an error if communication fails or the acceptor fails to persist.
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
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>,
{
    debug!("acceptor with epoch waiter started");

    // sync is terminated until we receive the initial Prepare
    let mut sync = pin!(Fuse::<S::Subscription>::terminated());
    let mut conn = pin!(conn);

    loop {
        // Poll both conn and sync
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

        // Handle connection message
        let Some(msg) = msg else {
            debug!("connection closed");
            return Ok(());
        };

        match msg? {
            AcceptorRequest::Prepare(proposal) => {
                let proposal_round = proposal.round();
                trace!(?proposal_round, "received prepare");

                // Wait for epoch to catch up if proposal is for a future epoch
                let current = current_epoch_fn();
                if proposal_round > current {
                    debug!(
                        current = ?current,
                        target = ?proposal_round,
                        "waiting for learning to catch up"
                    );
                    // Wait for epoch to advance
                    loop {
                        if epoch_rx.changed().await.is_err() {
                            debug!("epoch notifier closed");
                            return Ok(());
                        }
                        let new_epoch = current_epoch_fn();
                        if new_epoch >= proposal_round {
                            debug!(epoch = ?new_epoch, "caught up");
                            break;
                        }
                    }
                }

                let response = match handler.handle_prepare(&proposal).await {
                    Ok(PromiseOutcome::Promised(msg)) => {
                        debug!(?proposal_round, "promised");
                        // Subscribe on first successful promise
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
                        // Also subscribe on first outdated (learner still needs sync)
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
                // Ignore Accept before initial Prepare
                if sync.is_terminated() {
                    trace!("ignoring accept before initial prepare");
                    continue;
                }

                let proposal_round = proposal.round();
                trace!(?proposal_round, "received accept");

                // Wait for epoch to catch up if proposal is for a future epoch
                let current = current_epoch_fn();
                if proposal_round > current {
                    debug!(
                        current = ?current,
                        target = ?proposal_round,
                        "waiting for learning to catch up for accept"
                    );
                    // Wait for epoch to advance
                    loop {
                        if epoch_rx.changed().await.is_err() {
                            debug!("epoch notifier closed");
                            return Ok(());
                        }
                        let new_epoch = current_epoch_fn();
                        if new_epoch >= proposal_round {
                            debug!(epoch = ?new_epoch, "caught up");
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
