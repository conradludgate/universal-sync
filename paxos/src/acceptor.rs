//! Acceptor implementation

use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::select;
use tracing::{debug, instrument, trace, warn};

use crate::{Acceptor, AcceptorMessage, AcceptorRequest, AcceptorStateStore, Proposal};

/// Run the acceptor loop with shared state.
///
/// When a `Prepare` is received, the acceptor:
/// 1. Sends all historical accepted values from that round onwards
/// 2. Sends the promise response
/// 3. Switches to bidirectional streaming mode (handles Accept requests + broadcasts)
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
#[instrument(skip_all, name = "acceptor", fields(node_id = ?acceptor.node_id(), proposer = ?proposer_id))]
pub async fn run_acceptor<A, S, C>(
    mut acceptor: A,
    state: S,
    mut conn: C,
    proposer_id: <A::Proposal as Proposal>::NodeId,
) -> Result<(), A::Error>
where
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>
        + Unpin,
{
    debug!("acceptor started, waiting for initial prepare");

    // Wait for initial Prepare to establish the sync point
    let from_round = loop {
        let Some(msg) = conn.next().await else {
            debug!("connection closed before initial prepare");
            return Ok(());
        };
        let AcceptorRequest::Prepare(proposal) = msg? else {
            trace!("ignoring non-prepare message before initial prepare");
            continue;
        };

        // Validate the signed proposal
        if !acceptor.validate(&proposal) {
            warn!("rejecting invalid proposal");
            continue;
        }

        let from_round = proposal.round();
        debug!(?from_round, "received initial prepare");

        // Try to promise
        let round_state = match state.promise(&proposal) {
            Ok(()) => {
                trace!("promise succeeded");
                state.get(proposal.round())
            }
            Err(current) => {
                trace!("promise failed, returning current state");
                current
            }
        };

        // Send historical accepted values first
        // Skip from_round since it's included in the promise response below.
        // This prevents learners from double-counting the same value.
        let historical = state.accepted_from(from_round);
        let historical_count = historical
            .iter()
            .filter(|(p, _)| p.round() != from_round)
            .count();
        debug!(count = historical_count, "sending historical values");
        for (p, m) in historical {
            if p.round() == from_round {
                continue;
            }
            trace!(round = ?p.round(), "sending historical value");
            // For historical values, the accepted proposal is also the promised one
            conn.send(AcceptorMessage {
                promised: p.clone(),
                accepted: Some((p, m)),
            })
            .await?;
        }

        // Then send the promise response
        trace!("sending promise response");
        conn.send(AcceptorMessage::from_round_state(round_state))
            .await?;

        break from_round;
    };

    debug!("entering streaming mode");
    // Now run bidirectional streaming: handle requests + broadcast accepts
    run_streaming(&mut acceptor, &state, &mut conn, from_round).await
}

/// Bidirectional streaming mode.
///
/// Handles incoming Prepare/Accept requests while also forwarding broadcast accepts.
async fn run_streaming<A, S, C>(
    acceptor: &mut A,
    state: &S,
    conn: &mut C,
    from_round: <A::Proposal as Proposal>::RoundId,
) -> Result<(), A::Error>
where
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>
        + Unpin,
{
    let mut receiver = state.subscribe();
    trace!("subscribed to broadcast channel");

    loop {
        select! {
            // Handle incoming requests
            msg = conn.next() => {
                let Some(msg) = msg else {
                    debug!("connection closed");
                    return Ok(());
                };
                handle_request(acceptor, state, conn, msg?).await?;
            }
            // Forward broadcasts (accepts from other connections)
            broadcast = receiver.next() => {
                let Some((proposal, message)) = broadcast else {
                    debug!("broadcast channel closed");
                    return Ok(());
                };
                // Only forward if it's >= our sync point
                if proposal.round() >= from_round {
                    trace!(round = ?proposal.round(), "forwarding broadcast");
                    // For broadcast accepts, the accepted proposal is also the promised one
                    conn.send(AcceptorMessage {
                        promised: proposal.clone(),
                        accepted: Some((proposal, message)),
                    }).await?;
                }
            }
        }
    }
}

/// Handle a single Prepare or Accept request.
async fn handle_request<A, S, C>(
    acceptor: &mut A,
    state: &S,
    conn: &mut C,
    msg: AcceptorRequest<A>,
) -> Result<(), A::Error>
where
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Sink<AcceptorMessage<A>, Error = A::Error> + Unpin,
{
    match msg {
        AcceptorRequest::Prepare(proposal) => {
            trace!(round = ?proposal.round(), "received prepare");
            if !acceptor.validate(&proposal) {
                warn!("rejecting invalid prepare");
                return Ok(());
            }

            let round_state = match state.promise(&proposal) {
                Ok(()) => {
                    debug!(round = ?proposal.round(), "promised");
                    state.get(proposal.round())
                }
                Err(current) => {
                    trace!("promise failed, returning current state");
                    current
                }
            };

            conn.send(AcceptorMessage::from_round_state(round_state))
                .await?;
        }
        AcceptorRequest::Accept(proposal, message) => {
            trace!(round = ?proposal.round(), "received accept");
            if !acceptor.validate(&proposal) {
                warn!("rejecting invalid accept");
                return Ok(());
            }

            let round_state = match state.accept(&proposal, &message) {
                Ok(()) => {
                    debug!(round = ?proposal.round(), "accepted");
                    acceptor.accept(proposal.clone(), message).await?;
                    state.get(proposal.round())
                }
                Err(current) => {
                    trace!("accept failed, returning current state");
                    current
                }
            };

            conn.send(AcceptorMessage::from_round_state(round_state))
                .await?;
        }
    }

    Ok(())
}
