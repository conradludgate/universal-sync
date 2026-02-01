//! Acceptor implementation

use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};

use crate::{Acceptor, AcceptorMessage, AcceptorRequest, AcceptorStateStore, Proposal};

/// Run the acceptor loop with shared state.
///
/// Handles both proposer requests (Prepare/Accept) and learner requests (Sync).
/// When a `Sync` request is received, the connection switches to streaming mode
/// and sends all historical + live accepted values.
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
    while let Some(msg) = conn.try_next().await? {
        match msg {
            AcceptorRequest::Prepare(proposal) => {
                // Validate the signed proposal
                if !acceptor.validate(&proposal) {
                    continue;
                }

                // Try to promise - on success we get Ok, on rejection we get the current state
                let round_state = match state.promise(&proposal) {
                    Ok(()) => state.get(proposal.round()),
                    Err(current) => current,
                };

                conn.send(round_state.into()).await?;
            }
            AcceptorRequest::Accept(proposal, message) => {
                // Validate the signed proposal
                if !acceptor.validate(&proposal) {
                    continue;
                }

                // Try to accept - on success persist, state broadcasts to learners automatically
                let round_state = match state.accept(&proposal, &message) {
                    Ok(()) => {
                        // Persist before responding
                        acceptor.accept(proposal.clone(), message).await?;
                        state.get(proposal.round())
                    }
                    Err(current) => current,
                };

                conn.send(round_state.into()).await?;
            }
            AcceptorRequest::Sync { from_round } => {
                // Switch to learner streaming mode
                return stream_to_learner(&state, &mut conn, from_round).await;
            }
        }
    }

    Ok(())
}

/// Stream accepted values to a learner connection.
///
/// Sends all historical values from `from_round`, then streams live broadcasts.
async fn stream_to_learner<L, S, C>(
    state: &S,
    conn: &mut C,
    from_round: <L::Proposal as Proposal>::RoundId,
) -> Result<(), L::Error>
where
    L: crate::Learner,
    S: AcceptorStateStore<L>,
    C: Sink<AcceptorMessage<L>, Error = L::Error> + Unpin,
{
    // Send all historical accepted values
    let historical = state.accepted_from(from_round);
    for (proposal, message) in historical {
        conn.send(AcceptorMessage {
            promised: None,
            accepted: Some((proposal, message)),
        })
        .await?;
    }

    // Subscribe and stream live broadcasts
    let mut receiver = state.subscribe();
    while let Some((proposal, message)) = receiver.next().await {
        conn.send(AcceptorMessage {
            promised: None,
            accepted: Some((proposal, message)),
        })
        .await?;
    }

    Ok(())
}
