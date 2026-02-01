//! Learner connection to acceptors for syncing and receiving broadcasts

use futures::{Sink, SinkExt, Stream, StreamExt, stream::SelectAll};
use tracing::{debug, instrument, trace, warn};

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
    /// Create a new learner session and send prepare requests to all acceptors.
    ///
    /// Sends a "dummy" prepare with the current round to trigger sync.
    /// Acceptors will respond with all accepted values from that round onwards
    /// and then stream live updates.
    ///
    /// # Errors
    ///
    /// Returns an error if sending the prepare request fails.
    pub async fn new<I>(learner: &L, connections: I) -> Result<Self, L::Error>
    where
        I: IntoIterator<Item = C>,
    {
        let mut connections: Vec<C> = connections.into_iter().collect();
        let num_connections = connections.len();
        let tracker: QuorumTracker<L> = QuorumTracker::new(num_connections);

        // Send a dummy prepare to trigger sync from our current round
        // Using default attempt (0) - this is just for sync, not actual proposing
        let current_round = learner.current_round();
        debug!(
            ?current_round,
            num_connections,
            quorum = tracker.quorum(),
            "creating learner session"
        );

        let dummy_proposal = learner.propose(<L::Proposal as Proposal>::AttemptId::default());
        for conn in &mut connections {
            conn.send(AcceptorRequest::Prepare(dummy_proposal.clone()))
                .await?;
        }
        trace!("sent prepare to all acceptors");

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
        // First check if current_round already has quorum from previously tracked messages
        // This handles the case where future rounds were tracked before we advanced to them
        if let Some((learned_p, learned_m)) = self.tracker.get_with_quorum(self.current_round) {
            let result = (learned_p.clone(), learned_m.clone());
            debug!(round = ?self.current_round, "learned value with quorum (already tracked)");
            learner.apply(result.0.clone(), result.1.clone()).await?;
            self.tracker.clear_round(self.current_round);
            self.current_round = learner.current_round();
            trace!(new_round = ?self.current_round, "advanced to next round");
            return Ok(Some(result));
        }

        trace!(current_round = ?self.current_round, "learn_one waiting for messages");

        while let Some(result) = self.merged.next().await {
            let msg = result?;

            // Learners only care about accepted (proposal, message) pairs
            let Some((proposal, message)) = msg.accepted else {
                trace!("received non-accepted message, skipping");
                continue;
            };

            if !learner.validate(&proposal) {
                warn!("received invalid proposal, skipping");
                continue;
            }

            let round = proposal.round();

            // Skip rounds we've already learned
            if round < self.current_round {
                trace!(?round, current = ?self.current_round, "skipping old round");
                continue;
            }

            trace!(?round, "tracking message");
            // Track the message (for current or future rounds)
            self.tracker.track(proposal, message);

            // Only try to learn the current round (to maintain ordering)
            // Even if a higher round has quorum, we must wait for current_round first
            if let Some((learned_p, learned_m)) = self.tracker.get_with_quorum(self.current_round) {
                let result = (learned_p.clone(), learned_m.clone());
                debug!(round = ?self.current_round, "learned value with quorum");
                learner.apply(result.0.clone(), result.1.clone()).await?;
                self.tracker.clear_round(self.current_round);
                self.current_round = learner.current_round();
                trace!(new_round = ?self.current_round, "advanced to next round");
                return Ok(Some(result));
            }
        }

        debug!("all connections closed");
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
#[instrument(skip_all, name = "learner", fields(node_id = ?learner.node_id()))]
pub async fn run_learner<L, C, I>(mut learner: L, connections: I) -> Result<(), L::Error>
where
    L: Learner,
    C: Stream<Item = Result<AcceptorMessage<L>, L::Error>>
        + Sink<AcceptorRequest<L>, Error = L::Error>
        + Unpin,
    I: IntoIterator<Item = C>,
{
    debug!("learner started");
    let mut session = LearnerSession::new(&learner, connections).await?;

    // Learn values until all connections close
    let mut count = 0;
    while session.learn_one(&mut learner).await?.is_some() {
        count += 1;
    }

    debug!(learned_count = count, "learner finished");
    Ok(())
}
