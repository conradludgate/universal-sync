//! Acceptor run loop

use std::pin::pin;

use futures::stream::FusedStream;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::select;
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
