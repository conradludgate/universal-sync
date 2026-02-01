//! Acceptor implementation

use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::select;

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
pub async fn run_acceptor<A, S, C>(mut acceptor: A, state: S, mut conn: C) -> Result<(), A::Error>
where
    A: Acceptor,
    S: AcceptorStateStore<A>,
    C: Stream<Item = Result<AcceptorRequest<A>, A::Error>>
        + Sink<AcceptorMessage<A>, Error = A::Error>
        + Unpin,
{
    // Wait for initial Prepare to establish the sync point
    let from_round = loop {
        let Some(msg) = conn.next().await else {
            return Ok(());
        };
        let AcceptorRequest::Prepare(proposal) = msg? else {
            // Ignore Accept before Prepare
            continue;
        };

        // Validate the signed proposal
        if !acceptor.validate(&proposal) {
            continue;
        }

        let from_round = proposal.round();

        // Try to promise
        let round_state = match state.promise(&proposal) {
            Ok(()) => state.get(proposal.round()),
            Err(current) => current,
        };

        // Send historical accepted values first
        for (p, m) in state.accepted_from(from_round) {
            conn.send(AcceptorMessage {
                promised: None,
                accepted: Some((p, m)),
            })
            .await?;
        }

        // Then send the promise response
        conn.send(round_state.into()).await?;

        break from_round;
    };

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

    loop {
        select! {
            // Handle incoming requests
            msg = conn.next() => {
                let Some(msg) = msg else {
                    return Ok(());
                };
                handle_request(acceptor, state, conn, msg?).await?;
            }
            // Forward broadcasts (accepts from other connections)
            broadcast = receiver.next() => {
                let Some((proposal, message)) = broadcast else {
                    return Ok(());
                };
                // Only forward if it's >= our sync point
                if proposal.round() >= from_round {
                    conn.send(AcceptorMessage {
                        promised: None,
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
            if !acceptor.validate(&proposal) {
                return Ok(());
            }

            let round_state = match state.promise(&proposal) {
                Ok(()) => state.get(proposal.round()),
                Err(current) => current,
            };

            conn.send(round_state.into()).await?;
        }
        AcceptorRequest::Accept(proposal, message) => {
            if !acceptor.validate(&proposal) {
                return Ok(());
            }

            let round_state = match state.accept(&proposal, &message) {
                Ok(()) => {
                    acceptor.accept(proposal.clone(), message).await?;
                    state.get(proposal.round())
                }
                Err(current) => current,
            };

            conn.send(round_state.into()).await?;
        }
    }

    Ok(())
}
