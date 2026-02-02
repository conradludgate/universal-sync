//! Proposer/learner runtime implementation
//!
//! This module provides the runtime components for running a Paxos proposer:
//!
//! - [`Proposer`]: Main proposer/learner that connects to acceptors
//! - [`QuorumTracker`]: Tracks quorum for learning values
//!
//! # Example
//!
//! ```ignore
//! use paxos::proposer::Proposer;
//! use paxos::config::ProposerConfig;
//!
//! let proposer = Proposer::new(node_id, connector, ProposerConfig::default());
//! proposer.sync_actors(acceptor_ids);
//!
//! // Propose a value
//! let (proposal, message) = proposer.propose(&learner, my_message).await?;
//! learner.apply(proposal, message).await?;
//! ```

mod actor;
mod quorum;

use std::time::Duration;

use actor::{ActorManager, ActorMessage};
pub use quorum::QuorumTracker;
use rand::Rng;
use rand::rngs::StdRng;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::config::{BackoffConfig, ProposerConfig, Sleep};
use crate::core::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};
use crate::messages::AcceptorMessage;
use crate::traits::{Connector, Learner, Proposal, Proposer as ProposerTrait};

// ============================================================================
// Proposer Implementation
// ============================================================================

/// Result of a single proposal attempt
enum ProposeResult<L: Learner> {
    /// Proposal accepted with quorum
    Accepted(L::Proposal, L::Message),
    /// Rejected - need to retry with higher attempt
    Rejected {
        min_attempt: <L::Proposal as Proposal>::AttemptId,
    },
    /// All connections closed
    Closed,
}

/// A proposer/learner that connects to acceptors and drives consensus.
///
/// The proposer maintains persistent connections to acceptors via an actor model.
/// It can be used to either:
/// - **Propose values**: Call [`propose`](Self::propose) to drive a value to consensus
/// - **Learn values**: Call [`learn_one`](Self::learn_one) to wait for a quorum-confirmed value
///
/// Learners are just proposers that never call `propose()`.
pub struct Proposer<P: ProposerTrait, C: Connector<P>, S: Sleep, R: Rng = StdRng>
where
    P::Message: Send + Sync + 'static,
    C::ConnectFuture: Send,
    C::Connection: Send,
{
    manager: ActorManager<P, C>,
    msg_rx: mpsc::UnboundedReceiver<ActorMessage<P>>,
    attempt: <P::Proposal as Proposal>::AttemptId,
    /// Quorum tracker for learning - persists across `learn_one` calls to buffer future rounds
    learn_tracker: QuorumTracker<P>,
    /// Backoff configuration for retries
    backoff: BackoffConfig,
    /// Timeout for prepare/accept phases (None = no timeout)
    phase_timeout: Option<Duration>,
    /// Sleep implementation
    sleep: S,
    /// RNG for jitter
    rng: R,
}

impl<P, C, S, R> Proposer<P, C, S, R>
where
    P: ProposerTrait + Send + Sync + 'static,
    P::Message: Send + Sync + 'static,
    C: Connector<P> + Send + 'static,
    C::ConnectFuture: Send,
    C::Connection: Send,
    S: Sleep,
    R: Rng,
{
    /// Create a new proposer with the given configuration.
    ///
    /// The proposer starts with no active connections. Call [`sync_actors`](Self::sync_actors)
    /// to establish connections to acceptors.
    #[must_use]
    pub fn new(
        node_id: <P::Proposal as Proposal>::NodeId,
        connector: C,
        config: ProposerConfig<S, R>,
    ) -> Self {
        debug!("creating proposer");
        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<ActorMessage<P>>();
        let manager = ActorManager::new(node_id, connector, msg_tx);
        // Initialize with 0 actors; learn_one() will reset with correct count
        Self {
            manager,
            msg_rx,
            attempt: <P::Proposal as Proposal>::AttemptId::default(),
            learn_tracker: QuorumTracker::new(0),
            backoff: config.backoff,
            phase_timeout: config.phase_timeout,
            sleep: config.sleep,
            rng: config.rng,
        }
    }

    /// Sync the actor set to match the current acceptors.
    ///
    /// Spawns new actors for acceptors not in the current set, and aborts
    /// actors for acceptors no longer in the set.
    pub fn sync_actors(
        &mut self,
        acceptors: impl IntoIterator<Item = <P::Proposal as Proposal>::NodeId>,
    ) {
        self.manager.sync_actors(acceptors);
    }

    /// Get the number of active acceptor connections.
    #[must_use]
    pub fn num_actors(&self) -> usize {
        self.manager.num_actors()
    }

    /// Start the sync process by sending a prepare to all acceptors.
    ///
    /// This triggers acceptors to send historical values and then stream live updates.
    /// Call this once before calling `learn_one` in a loop.
    ///
    /// Note: Requires a [`ProposerTrait`] implementation to create the sync proposal.
    pub fn start_sync(
        &mut self,
        proposer: &impl ProposerTrait<Proposal = P::Proposal, Message = P::Message, Error = P::Error>,
    ) {
        // Reset the quorum tracker for learning
        let num_acceptors = self.manager.num_actors();
        self.learn_tracker = QuorumTracker::new(num_acceptors);

        // Start a "dummy" prepare to trigger sync from acceptors.
        // Using default attempt (0) - this is just for sync, not actual proposing.
        let sync_proposal = proposer.propose(<P::Proposal as Proposal>::AttemptId::default());
        self.manager.start_prepare(sync_proposal);
        debug!(num_acceptors, "started sync");
    }

    /// Learn one value from acceptors (waits until quorum reached).
    ///
    /// Returns `Some((proposal, message))` when a value is learned with quorum,
    /// or `None` if all connections are closed.
    ///
    /// This does not apply the value - the caller is responsible for calling
    /// `learner.apply()` after receiving the result.
    ///
    /// For dedicated learners: call [`start_sync`](Self::start_sync) once before
    /// calling this in a loop to trigger historical sync.
    ///
    /// For proposers: this can be used without `start_sync` to learn from live
    /// broadcasts while waiting for messages to propose.
    ///
    /// # Cancellation Safety
    ///
    /// This method is cancellation-safe. Messages received before cancellation are
    /// buffered in an internal tracker that persists across calls. If cancelled,
    /// calling `learn_one` again will first check for quorum from buffered messages
    /// before waiting for new ones, so no progress is lost.
    pub async fn learn_one(&mut self, learner: &P) -> Option<(P::Proposal, P::Message)> {
        let current_round = learner.current_round();

        // Reinitialize tracker if actor count changed
        let num_acceptors = self.manager.num_actors();
        if self.learn_tracker.num_acceptors() != num_acceptors {
            self.learn_tracker = QuorumTracker::new(num_acceptors);
        }
        let tracker = &mut self.learn_tracker;
        debug!(?current_round, quorum = tracker.quorum(), "learning");

        // First check if we already have quorum from previously buffered messages
        if let Some((learned_p, learned_m)) = tracker.check_quorum(current_round) {
            debug!(?current_round, "learned from buffered messages");
            return Some((learned_p.clone(), learned_m.clone()));
        }

        loop {
            let actor_msg = self.msg_rx.recv().await?;

            // Track all accepted values for rounds >= current_round
            let Some((proposal, message)) = actor_msg.msg.accepted else {
                continue;
            };

            // Skip rounds we've already learned
            if proposal.round() < current_round {
                trace!(round = ?proposal.round(), ?current_round, "skipping old round");
                continue;
            }

            if !learner.validate(&proposal) {
                trace!("ignoring invalid proposal");
                continue;
            }

            // Track the message (for current or future rounds)
            let round = proposal.round();
            trace!(?round, "tracking message");
            tracker.track(proposal, message);

            // Only return when current_round reaches quorum (to maintain ordering)
            if let Some((learned_p, learned_m)) = tracker.check_quorum(current_round) {
                debug!(?current_round, "learned value with quorum");
                return Some((learned_p.clone(), learned_m.clone()));
            }
        }
    }

    /// Propose a value and drive it to consensus.
    ///
    /// Returns the accepted `(proposal, message)` pair. Note that due to Paxos
    /// semantics, the returned message may be different from the input if a
    /// higher-numbered proposal was already in progress.
    ///
    /// This does not apply the value - the caller is responsible for calling
    /// `learner.apply()` after receiving the result.
    ///
    /// Returns `None` if all connections are closed.
    ///
    /// # Cancellation Safety
    ///
    /// This method is cancellation-safe but progress may be lost. If cancelled
    /// mid-proposal, the in-flight protocol state (which acceptors have promised
    /// or accepted) is discarded. The internal attempt counter is preserved, so
    /// calling `propose` again will use an appropriate proposal number. No messages
    /// are lost from the channel, but you will need to restart the proposal from
    /// scratch.
    pub async fn propose(
        &mut self,
        proposer: &P,
        message: P::Message,
    ) -> Option<(P::Proposal, P::Message)> {
        let round = proposer.current_round();
        let num_acceptors = self.manager.num_actors();
        let mut tracker: QuorumTracker<P> = QuorumTracker::new(num_acceptors);
        debug!(?round, quorum = tracker.quorum(), "proposing");

        // Proposal loop with retries
        for consecutive_rejections in 0.. {
            let proposal = proposer.propose(self.attempt);
            debug!(attempt = ?self.attempt, consecutive_rejections, "attempting proposal");

            let result = self
                .run_proposal(
                    &mut tracker,
                    proposer,
                    proposal.clone(),
                    message.clone(),
                    round,
                )
                .await;

            match result {
                ProposeResult::Accepted(proposal, message) => {
                    debug!("proposal accepted");
                    self.attempt = <P::Proposal as Proposal>::AttemptId::default();
                    return Some((proposal, message));
                }
                ProposeResult::Rejected { min_attempt } => {
                    debug!(?min_attempt, "proposal rejected, will retry");
                    self.attempt = min_attempt;
                }
                ProposeResult::Closed => {
                    debug!("connections closed during proposal");
                    return None;
                }
            }

            // Backoff before retry
            let backoff = self.backoff.duration(consecutive_rejections, &mut self.rng);
            trace!(?backoff, "backing off before retry");
            self.sleep.sleep(backoff).await;
        }

        unreachable!("infinite loop should return from inside")
    }

    /// Receive and validate next actor message for proposal phase.
    async fn recv_for_phase(&mut self, expected_seq: u64) -> RecvResult<P> {
        let recv = if let Some(t) = self.phase_timeout {
            tokio::select! {
                biased;
                msg = self.msg_rx.recv() => msg,
                () = self.sleep.sleep(t) => return RecvResult::Timeout,
            }
        } else {
            self.msg_rx.recv().await
        };

        let Some(actor_msg) = recv else {
            return RecvResult::Closed;
        };

        if actor_msg.seq != expected_seq {
            trace!(
                expected = expected_seq,
                got = actor_msg.seq,
                "ignoring stale message"
            );
            return RecvResult::Stale;
        }

        RecvResult::Message {
            acceptor_id: actor_msg.acceptor_id,
            msg: actor_msg.msg,
        }
    }

    /// Run a single proposal attempt through both phases using `ProposerCore`
    async fn run_proposal(
        &mut self,
        tracker: &mut QuorumTracker<P>,
        learner: &impl Learner<Proposal = P::Proposal, Message = P::Message, Error = P::Error>,
        proposal: P::Proposal,
        message: P::Message,
        round: <P::Proposal as Proposal>::RoundId,
    ) -> ProposeResult<P> {
        let proposal_key = proposal.key();
        let mut core = ProposerCore::new(proposal_key, message.clone(), self.manager.num_actors());
        trace!(quorum = core.quorum(), "initialized proposer core");

        self.manager.start_prepare(proposal.clone());
        let seq = self.manager.current_seq();
        trace!(seq, quorum = core.quorum(), "collecting promises");

        // Prepare phase: collect promises
        let chosen_message = loop {
            let (acc_id, msg) = match self.recv_for_phase(seq).await {
                RecvResult::Message { acceptor_id, msg } => (acceptor_id, msg),
                RecvResult::Stale => continue,
                RecvResult::Timeout => {
                    debug!("prepare phase timed out");
                    let min_attempt = P::Proposal::next_attempt(proposal.attempt());
                    return ProposeResult::Rejected { min_attempt };
                }
                RecvResult::Closed => {
                    debug!("message channel closed during prepare phase");
                    return ProposeResult::Closed;
                }
            };

            // Check for already accepted values in this round (learning from others)
            if let Some((accepted_p, accepted_m)) = &msg.accepted
                && accepted_p.round() == round
                && learner.validate(accepted_p)
            {
                trace!("found already accepted value in this round");
                if let Some((learned_p, learned_m)) =
                    tracker.track(accepted_p.clone(), accepted_m.clone())
                    && learned_p.key() != proposal_key
                {
                    debug!("learned different value during prepare");
                    return ProposeResult::Accepted(learned_p.clone(), learned_m.clone());
                }

                // Reject if a higher proposal was already accepted in this round
                if accepted_p.key() >= proposal_key {
                    debug!("rejected: higher proposal already accepted");
                    let min_attempt = P::Proposal::next_attempt(accepted_p.attempt());
                    return ProposeResult::Rejected { min_attempt };
                }
            }

            // Validate the promise
            if !learner.validate(&msg.promised) {
                trace!("ignoring invalid promise");
                continue;
            }

            // Use ProposerCore to handle the promise
            let result =
                core.handle_promise(acc_id, msg.promised.key(), msg.accepted, Proposal::key);

            match result {
                PreparePhaseResult::Quorum { value } => break value,
                PreparePhaseResult::Rejected { superseded_by, .. } => {
                    debug!(?superseded_by, "rejected: higher proposal");
                    let min_attempt = P::Proposal::next_attempt(superseded_by.attempt());
                    return ProposeResult::Rejected { min_attempt };
                }
                // Need more promises
                PreparePhaseResult::Pending => {}
            }
        };

        debug!(quorum = core.quorum(), "prepare phase complete");

        // Start accept phase with the chosen message
        self.manager
            .transition_to_accept(proposal.clone(), chosen_message.clone());
        trace!("collecting accepts");

        // Accept phase: collect accepts
        loop {
            let (acceptor_id, msg) = match self.recv_for_phase(seq).await {
                RecvResult::Message { acceptor_id, msg } => (acceptor_id, msg),
                RecvResult::Stale => continue,
                RecvResult::Timeout => {
                    debug!("accept phase timed out");
                    let min_attempt = P::Proposal::next_attempt(proposal.attempt());
                    return ProposeResult::Rejected { min_attempt };
                }
                RecvResult::Closed => {
                    debug!("message channel closed during accept phase");
                    return ProposeResult::Closed;
                }
            };

            // Get the accepted key if present
            let Some((accepted_p, _)) = &msg.accepted else {
                continue;
            };

            if !learner.validate(accepted_p) {
                trace!("ignoring invalid accepted");
                continue;
            }

            let result =
                core.handle_accepted(acceptor_id, Some(accepted_p.key()), proposal.clone());
            match result {
                AcceptPhaseResult::Learned { proposal, value } => {
                    debug!(quorum = core.quorum(), "accept phase complete");
                    return ProposeResult::Accepted(proposal, value);
                }
                AcceptPhaseResult::Rejected { superseded_by } => {
                    debug!(?superseded_by, "rejected: higher proposal accepted");
                    let min_attempt = P::Proposal::next_attempt(superseded_by.attempt());
                    return ProposeResult::Rejected { min_attempt };
                }
                // Need more accepts
                AcceptPhaseResult::Pending => {}
            }
        }
    }
}

/// Result of receiving a message for a proposal phase.
enum RecvResult<P: Learner> {
    /// Received a valid message for current seq
    Message {
        acceptor_id: <P::Proposal as Proposal>::NodeId,
        msg: AcceptorMessage<P>,
    },
    /// Message was stale (wrong seq), caller should continue
    Stale,
    /// Phase timed out
    Timeout,
    /// Channel closed
    Closed,
}
