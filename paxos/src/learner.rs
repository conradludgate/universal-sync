//! Learner connection to acceptors for syncing and receiving broadcasts

use futures::{Sink, SinkExt, Stream, StreamExt, stream::SelectAll};

use crate::{AcceptorMessage, AcceptorRequest, Learner, Proposal, quorum::QuorumTracker};

/// Persistent learner state that can be reused across multiple `learn_one` calls.
///
/// Holds the merged connection streams and quorum tracker between runs.
pub struct LearnerSession<L: Learner, C> {
    merged: SelectAll<C>,
    tracker: QuorumTracker<L>,
    current_round: <L::Proposal as Proposal>::RoundId,
}

impl<L, C> LearnerSession<L, C>
where
    L: Learner,
    C: Stream<Item = Result<AcceptorMessage<L>, L::Error>>
        + Sink<AcceptorRequest<L>, Error = L::Error>
        + Unpin,
{
    /// Create a new learner session and send sync requests to all acceptors.
    ///
    /// # Errors
    ///
    /// Returns an error if sending the sync request fails.
    pub async fn new<I>(learner: &L, connections: I) -> Result<Self, L::Error>
    where
        I: IntoIterator<Item = C>,
    {
        let mut connections: Vec<C> = connections.into_iter().collect();
        let tracker: QuorumTracker<L> = QuorumTracker::new(connections.len());

        // Request sync from our current round on all connections
        let current_round = learner.current_round();
        for conn in &mut connections {
            conn.send(AcceptorRequest::Sync {
                from_round: current_round,
            })
            .await?;
        }

        // Merge all streams into one
        let merged: SelectAll<C> = connections.into_iter().collect();

        Ok(Self {
            merged,
            tracker,
            current_round,
        })
    }

    /// Learn a single value from the acceptors.
    ///
    /// Returns `Some((proposal, message))` when a value is learned with quorum,
    /// or `None` if all connections are closed.
    ///
    /// The session can be reused to learn additional values by calling this again.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with acceptors fails.
    pub async fn learn_one(
        &mut self,
        learner: &mut L,
    ) -> Result<Option<(L::Proposal, L::Message)>, L::Error> {
        while let Some(result) = self.merged.next().await {
            let msg = result?;

            // Learners only care about accepted (proposal, message) pairs
            let Some((proposal, message)) = msg.accepted else {
                continue;
            };

            if !learner.validate(&proposal) {
                continue;
            }

            let round = proposal.round();

            // Skip rounds we've already learned
            if round < self.current_round {
                continue;
            }

            // Track and check for quorum
            if let Some((learned_p, learned_m)) = self.tracker.track(proposal, message) {
                let result = (learned_p.clone(), learned_m.clone());
                learner.apply(result.0.clone(), result.1.clone()).await?;
                self.tracker.clear_round(round);
                self.current_round = learner.current_round();
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
}

/// Run the learner sync loop with multiple acceptor connections.
///
/// Connects to multiple acceptors, requests sync from current round,
/// and only applies proposals once a quorum of acceptors have confirmed them.
///
/// Runs until all connections are closed.
///
/// Uses `AcceptorMessage` (same as proposers) - learners just look at the
/// `accepted` field and ignore `promised`.
///
/// # Errors
///
/// Returns an error if:
/// - Communication with acceptors fails
/// - The learner fails to apply a proposal
pub async fn run_learner<L, C, I>(mut learner: L, connections: I) -> Result<(), L::Error>
where
    L: Learner,
    C: Stream<Item = Result<AcceptorMessage<L>, L::Error>>
        + Sink<AcceptorRequest<L>, Error = L::Error>
        + Unpin,
    I: IntoIterator<Item = C>,
{
    let mut session = LearnerSession::new(&learner, connections).await?;

    // Learn values until all connections close
    while session.learn_one(&mut learner).await?.is_some() {}

    Ok(())
}
