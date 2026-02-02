//! Proposer implementation
//!
//! Uses a persistent actor model where each acceptor connection runs as an
//! independent spawned task, managed via `JoinMap`. Actors persist across rounds.

use std::{collections::HashSet, pin::pin, time::Duration};

use futures::{SinkExt, Stream, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, watch};
use tokio_util::task::JoinMap;
use tracing::{debug, instrument, trace, warn};

use crate::{
    AcceptorMessage, AcceptorRequest, Connector, LazyConnection, Learner, Proposal, ProposerConfig,
    Sleep,
    core::{PreparePhaseResult, ProposerCore},
    quorum::QuorumTracker,
};

// ============================================================================
// Shared State Types
// ============================================================================

/// Command sent from coordinator to actors
enum Command<L: Learner> {
    /// Idle - wait for next command
    Idle,
    /// Start prepare phase for this proposal
    Prepare(L::Proposal),
    /// Accept this proposal + message (after successful prepare)
    Accept {
        proposal: L::Proposal,
        message: L::Message,
    },
}

impl<L: Learner> Clone for Command<L> {
    fn clone(&self) -> Self {
        match self {
            Self::Idle => Self::Idle,
            Self::Prepare(p) => Self::Prepare(p.clone()),
            Self::Accept { proposal, message } => Self::Accept {
                proposal: proposal.clone(),
                message: message.clone(),
            },
        }
    }
}

#[expect(
    clippy::derivable_impls,
    reason = "derive(Default) doesn't work with generic bounds"
)]
impl<L: Learner> Default for Command<L> {
    fn default() -> Self {
        Self::Idle
    }
}

/// Messages from actors back to coordinator
struct ActorMessage<L: Learner> {
    /// Which acceptor sent this message
    acceptor_id: <L::Proposal as Proposal>::NodeId,
    /// Sequence number for this proposal attempt
    seq: u64,
    /// The actual message
    msg: AcceptorMessage<L>,
}

/// Shared state broadcast from coordinator to all actors
struct CoordinatorState<L: Learner> {
    /// Current command
    command: Command<L>,
    /// Sequence number - increments on each state change
    seq: u64,
}

impl<L: Learner> Clone for CoordinatorState<L> {
    fn clone(&self) -> Self {
        Self {
            command: self.command.clone(),
            seq: self.seq,
        }
    }
}

// ============================================================================
// Actor Implementation
// ============================================================================

/// Run an actor for a single acceptor connection.
///
/// This task lives as long as the acceptor is in the active set.
/// It handles connection lifecycle and responds to coordinator commands.
#[instrument(skip_all, name = "actor", fields(node_id = ?proposer_id, acceptor = ?acceptor_id))]
async fn run_actor<L, C>(
    proposer_id: <L::Proposal as Proposal>::NodeId,
    acceptor_id: <L::Proposal as Proposal>::NodeId,
    connector: C,
    mut state_rx: watch::Receiver<CoordinatorState<L>>,
    msg_tx: mpsc::UnboundedSender<ActorMessage<L>>,
) where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
    let mut conn: LazyConnection<L, C> = LazyConnection::new(acceptor_id, connector);
    let mut last_seq = 0u64;

    trace!("actor started");

    loop {
        // Wait for state change
        let state = {
            let Ok(state) = state_rx.wait_for(|s| s.seq != last_seq).await else {
                trace!("actor stopping: coordinator dropped");
                return;
            };
            last_seq = state.seq;
            trace!(seq = last_seq, "actor received state change");
            state.clone()
        };

        match state.command {
            Command::Prepare(proposal) => {
                let proposal_key = proposal.key();
                trace!("actor sending prepare");
                // Send prepare
                if conn.send(AcceptorRequest::Prepare(proposal)).await.is_err() {
                    warn!("actor prepare send failed, retrying on next command");
                    continue;
                }

                // Wait for a valid promise for THIS proposal.
                // Forward ALL messages to coordinator (for learning historical values),
                // but only break out of the loop when we get our expected promise.
                loop {
                    let Some(Ok(msg)) = conn.next().await else {
                        warn!("actor connection closed while waiting for promise");
                        break;
                    };

                    // Always forward the message to coordinator for potential learning
                    let _ = msg_tx.send(ActorMessage {
                        acceptor_id,
                        seq: last_seq,
                        msg: msg.clone(),
                    });

                    // Only break when we get our expected promise or higher
                    // (lower = stale from previous prepare, but still useful for learning)
                    if msg.promised.key() >= proposal_key {
                        trace!("actor received promise for current proposal");
                        break;
                    }
                    trace!("actor received historical value, forwarded to coordinator");
                }

                // Wait for Accept command while also forwarding any connection messages
                // (for learners who need to receive live broadcasts)
                'accept_wait: loop {
                    tokio::select! {
                        biased;
                        // Check for new commands
                        result = state_rx.changed() => {
                            if result.is_err() {
                                trace!("actor stopping: coordinator dropped during accept_wait");
                                return;
                            }
                            let new_state = state_rx.borrow_and_update().clone();

                            // New round or new seq means restart
                            if new_state.seq != last_seq {
                                trace!(
                                    old_seq = last_seq,
                                    new_seq = new_state.seq,
                                    "actor restarting: seq changed"
                                );
                                break 'accept_wait;
                            }

                            if let Command::Accept {
                                proposal,
                                message: msg_value,
                            } = new_state.command
                            {
                                let proposal_key = proposal.key();
                                trace!("actor sending accept");

                                // Send accept
                                if conn
                                    .send(AcceptorRequest::Accept(proposal, msg_value))
                                    .await
                                    .is_err()
                                {
                                    warn!("actor accept send failed");
                                    break 'accept_wait;
                                }

                                // Wait for accepted response (also forward any other messages)
                                loop {
                                    let Some(Ok(msg)) = conn.next().await else {
                                        warn!("actor connection closed while waiting for accepted");
                                        break 'accept_wait;
                                    };

                                    // Always forward the message
                                    let _ = msg_tx.send(ActorMessage {
                                        acceptor_id,
                                        seq: last_seq,
                                        msg: msg.clone(),
                                    });

                                    if msg
                                        .accepted
                                        .as_ref()
                                        .is_some_and(|(p, _)| p.key() >= proposal_key)
                                    {
                                        trace!("actor received accepted");
                                        break 'accept_wait;
                                    }
                                    trace!("actor received non-accepted message, forwarded");
                                }
                            }
                        }
                        // Also read from connection and forward any messages (for learners)
                        msg_result = conn.next() => {
                            if let Some(Ok(msg)) = msg_result {
                                let _ = msg_tx.send(ActorMessage {
                                    acceptor_id,
                                    seq: last_seq,
                                    msg,
                                });
                                trace!("actor forwarded broadcast message");
                            } else {
                                warn!("actor connection closed during accept_wait");
                                break 'accept_wait;
                            }
                        }
                    }
                }
            }
            Command::Idle | Command::Accept { .. } => {
                trace!("actor received idle/accept command, ignoring");
            }
        }
    }
}

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

/// Manages the set of actor tasks
struct ActorManager<L: Learner, C: Connector<L>> {
    proposer_id: <L::Proposal as Proposal>::NodeId,
    actors: JoinMap<<L::Proposal as Proposal>::NodeId, ()>,
    connector: C,
    state_tx: watch::Sender<CoordinatorState<L>>,
    state_rx: watch::Receiver<CoordinatorState<L>>,
    msg_tx: mpsc::UnboundedSender<ActorMessage<L>>,
}

impl<L: Learner, C: Connector<L>> Drop for ActorManager<L, C> {
    fn drop(&mut self) {
        // Abort all actors when manager is dropped to release connections
        self.actors.abort_all();
    }
}

impl<L, C> ActorManager<L, C>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
    fn new(
        proposer_id: <L::Proposal as Proposal>::NodeId,
        connector: C,
        msg_tx: mpsc::UnboundedSender<ActorMessage<L>>,
    ) -> Self {
        debug!("creating actor manager");
        let (state_tx, state_rx) = watch::channel(CoordinatorState {
            command: Command::Idle,
            seq: 0,
        });

        Self {
            proposer_id,
            actors: JoinMap::new(),
            connector,
            state_tx,
            state_rx,
            msg_tx,
        }
    }

    /// Update the actor set to match the current acceptors
    fn sync_actors(
        &mut self,
        acceptors: impl IntoIterator<Item = <L::Proposal as Proposal>::NodeId>,
    ) {
        let desired: HashSet<_> = acceptors.into_iter().collect();

        // Remove actors no longer in the set
        let before = self.actors.len();
        self.actors
            .abort_matching(|node_id| !desired.contains(node_id));
        let removed = before - self.actors.len();

        // Spawn new actors
        let mut spawned = 0;
        for id in desired {
            if !self.actors.contains_key(&id) {
                let proposer_id = self.proposer_id;
                let connector = self.connector.clone();
                let state_rx = self.state_rx.clone();
                let msg_tx = self.msg_tx.clone();
                self.actors
                    .spawn(id, run_actor(proposer_id, id, connector, state_rx, msg_tx));
                spawned += 1;
            }
        }
        if removed > 0 || spawned > 0 {
            debug!(removed, spawned, total = self.actors.len(), "synced actors");
        }
    }

    fn num_actors(&self) -> usize {
        self.actors.len()
    }

    /// Start a new proposal (Prepare phase). Increments seq to signal new proposal.
    fn start_prepare(&self, proposal: L::Proposal) {
        self.state_tx.send_modify(|s| {
            s.command = Command::Prepare(proposal);
            s.seq += 1;
        });
        debug!(seq = self.state_tx.borrow().seq, "started prepare phase");
    }

    /// Transition to Accept phase. Does NOT increment seq so actors don't restart.
    fn transition_to_accept(&self, proposal: L::Proposal, message: L::Message) {
        self.state_tx.send_modify(|s| {
            s.command = Command::Accept { proposal, message };
        });
        debug!("transitioned to accept phase");
    }

    fn current_seq(&self) -> u64 {
        self.state_tx.borrow().seq
    }
}

// ============================================================================
// Proposer Struct
// ============================================================================

/// A proposer/learner that connects to acceptors and drives consensus.
///
/// The proposer maintains persistent connections to acceptors via an actor model.
/// It can be used to either:
/// - **Propose values**: Call [`propose`](Self::propose) to drive a value to consensus
/// - **Learn values**: Call [`learn_one`](Self::learn_one) to wait for a quorum-confirmed value
///
/// Learners are just proposers that never call `propose()`.
pub struct Proposer<L: Learner, C: Connector<L>>
where
    L::Message: Send + Sync + 'static,
    C::ConnectFuture: Unpin + Send,
    C::Connection: Unpin + Send,
{
    manager: ActorManager<L, C>,
    msg_rx: mpsc::UnboundedReceiver<ActorMessage<L>>,
    attempt: <L::Proposal as Proposal>::AttemptId,
    /// Quorum tracker for learning - persists across `learn_one` calls to buffer future rounds
    learn_tracker: QuorumTracker<L>,
}

impl<L, C> Proposer<L, C>
where
    L: Learner + Send + Sync + 'static,
    L::Message: Send + Sync + 'static,
    C: Connector<L> + Send + 'static,
    C::ConnectFuture: Unpin + Send,
    C::Connection: Unpin + Send,
{
    /// Create a new proposer.
    ///
    /// The proposer starts with no active connections. Call [`sync_actors`](Self::sync_actors)
    /// to establish connections to acceptors.
    #[must_use]
    pub fn new(node_id: <L::Proposal as Proposal>::NodeId, connector: C) -> Self {
        debug!("creating proposer");
        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<ActorMessage<L>>();
        let manager = ActorManager::new(node_id, connector, msg_tx);
        // Initialize with 0 actors; learn_one() will reset with correct count
        Self {
            manager,
            msg_rx,
            attempt: <L::Proposal as Proposal>::AttemptId::default(),
            learn_tracker: QuorumTracker::new(0),
        }
    }

    /// Sync the actor set to match the current acceptors.
    ///
    /// Spawns new actors for acceptors not in the current set, and aborts
    /// actors for acceptors no longer in the set.
    pub fn sync_actors(
        &mut self,
        acceptors: impl IntoIterator<Item = <L::Proposal as Proposal>::NodeId>,
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
    pub fn start_sync(&mut self, learner: &L) {
        // Reset the quorum tracker for learning
        let num_acceptors = self.manager.num_actors();
        self.learn_tracker = QuorumTracker::new(num_acceptors);

        // Start a "dummy" prepare to trigger sync from acceptors.
        // Using default attempt (0) - this is just for sync, not actual proposing.
        let dummy_proposal = learner.propose(<L::Proposal as Proposal>::AttemptId::default());
        self.manager.start_prepare(dummy_proposal);
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
    pub async fn learn_one(&mut self, learner: &L) -> Option<(L::Proposal, L::Message)> {
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
    pub async fn propose<S: Sleep, R: Rng>(
        &mut self,
        learner: &L,
        message: L::Message,
        config: &mut ProposerConfig<S, R>,
    ) -> Option<(L::Proposal, L::Message)> {
        let round = learner.current_round();
        let num_acceptors = self.manager.num_actors();
        let mut tracker: QuorumTracker<L> = QuorumTracker::new(num_acceptors);
        debug!(?round, quorum = tracker.quorum(), "proposing");

        // Proposal loop with retries
        for consecutive_rejections in 0.. {
            let proposal = learner.propose(self.attempt);
            debug!(attempt = ?self.attempt, consecutive_rejections, "attempting proposal");

            let result = run_proposal(
                &self.manager,
                &mut self.msg_rx,
                learner,
                proposal.clone(),
                message.clone(),
                &mut tracker,
                round,
                &config.sleep,
                config.phase_timeout,
            )
            .await;

            match result {
                ProposeResult::Accepted(proposal, message) => {
                    debug!("proposal accepted");
                    self.attempt = <L::Proposal as Proposal>::AttemptId::default();
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
            let backoff = config
                .backoff
                .duration(consecutive_rejections, &mut config.rng);
            trace!(?backoff, "backing off before retry");
            config.sleep.sleep(backoff).await;
        }

        unreachable!("infinite loop should return from inside")
    }
}

/// Run a proposer loop that proposes messages from a stream.
///
/// This is a convenience wrapper around [`Proposer`] that runs a loop:
/// 1. Wait for a message from `messages` (learning from broadcasts while waiting)
/// 2. Propose the message
/// 3. Apply the result
/// 4. Repeat
///
/// # Errors
///
/// Returns an error if the learner fails to apply a learned proposal.
#[instrument(skip_all, name = "proposer", fields(node_id = ?learner.node_id()))]
pub async fn run_proposer<L, C, M, S, R>(
    mut learner: L,
    connector: C,
    mut messages: M,
    mut config: ProposerConfig<S, R>,
) -> Result<(), L::Error>
where
    L: Learner + Send + Sync + 'static,
    L::Message: Send + Sync + 'static,
    C: Connector<L> + Send + 'static,
    C::ConnectFuture: Unpin + Send,
    C::Connection: Unpin + Send,
    M: Stream<Item = L::Message> + Unpin,
    S: Sleep,
    R: Rng,
{
    debug!("proposer started");
    let mut proposer = Proposer::new(learner.node_id(), connector);

    loop {
        let round = learner.current_round();
        debug!(?round, "starting new round");

        proposer.sync_actors(learner.acceptors());

        // Wait for a message to propose, or learn from background
        let msg = loop {
            tokio::select! {
                biased;
                learn_result = proposer.learn_one(&learner) => {
                    let Some((p, m)) = learn_result else {
                        debug!("connections closed, proposer exiting");
                        return Ok(());
                    };
                    debug!("learned value from background broadcast");
                    learner.apply(p, m).await?;
                    // Continue waiting for message to propose (with updated round)
                    proposer.sync_actors(learner.acceptors());
                }
                msg = messages.next() => {
                    let Some(msg) = msg else {
                        debug!("message stream closed, proposer exiting");
                        return Ok(());
                    };
                    debug!("received message to propose");
                    break msg;
                }
            }
        };

        // Propose and apply
        let Some((p, m)) = proposer.propose(&learner, msg, &mut config).await else {
            debug!("connections closed during proposal, exiting");
            return Ok(());
        };
        learner.apply(p, m).await?;
    }
}

/// Run a single proposal attempt through both phases using `ProposerCore`
async fn run_proposal<L, C, S>(
    manager: &ActorManager<L, C>,
    msg_rx: &mut mpsc::UnboundedReceiver<ActorMessage<L>>,
    learner: &L,
    proposal: L::Proposal,
    message: L::Message,
    tracker: &mut QuorumTracker<L>,
    round: <L::Proposal as Proposal>::RoundId,
    sleep: &S,
    phase_timeout: Option<Duration>,
) -> ProposeResult<L>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
    S: Sleep,
{
    let num_acceptors = manager.num_actors();
    let proposal_key = proposal.key();

    // Initialize ProposerCore for this attempt
    let mut core = ProposerCore::new(proposal_key, message.clone(), num_acceptors);

    trace!(quorum = core.quorum(), "initialized proposer core");

    // Start prepare phase
    manager.start_prepare(proposal.clone());
    let seq = manager.current_seq();
    trace!(seq, quorum = core.quorum(), "collecting promises");

    // Create timeout future for prepare phase
    let prepare_timeout = async {
        if let Some(timeout) = phase_timeout {
            sleep.sleep(timeout).await;
            true
        } else {
            std::future::pending::<bool>().await
        }
    };
    let mut prepare_timeout = pin!(prepare_timeout);

    // Prepare phase: collect promises
    let chosen_message = loop {
        let actor_msg = tokio::select! {
            biased;
            msg = msg_rx.recv() => msg,
            _ = &mut prepare_timeout => {
                debug!("prepare phase timed out");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(proposal.attempt()),
                };
            }
        };

        let Some(actor_msg) = actor_msg else {
            debug!("message channel closed during prepare phase");
            return ProposeResult::Closed;
        };

        if actor_msg.seq != seq {
            trace!(
                expected = seq,
                got = actor_msg.seq,
                "ignoring stale message"
            );
            continue;
        }

        let msg = actor_msg.msg;
        let acceptor_id = actor_msg.acceptor_id;

        // Check for already accepted values in this round (learning from others)
        if let Some((accepted_p, accepted_m)) = msg.accepted.clone()
            && accepted_p.round() == round
            && learner.validate(&accepted_p)
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
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                };
            }
        }

        let promised_p = &msg.promised;

        // Validate the promise
        if !learner.validate(promised_p) {
            trace!("ignoring invalid promise");
            continue;
        }

        // Use ProposerCore to handle the promise
        let promised_key = promised_p.key();
        let accepted = msg.accepted.clone();

        let result = core.handle_promise(acceptor_id, promised_key, accepted, Proposal::key);

        match result {
            PreparePhaseResult::Quorum { value } => {
                debug!(quorum = core.quorum(), "prepare phase complete");
                break value;
            }
            PreparePhaseResult::Rejected { superseded_by, .. } => {
                debug!(?superseded_by, "rejected: higher proposal");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(superseded_by.attempt()),
                };
            }
            PreparePhaseResult::Pending => {
                // Need more promises
            }
        }
    };

    // Start accept phase with the chosen message
    manager.transition_to_accept(proposal.clone(), chosen_message.clone());
    trace!("collecting accepts");

    // Create timeout future for accept phase
    let accept_timeout = async {
        if let Some(timeout) = phase_timeout {
            sleep.sleep(timeout).await;
            true
        } else {
            std::future::pending::<bool>().await
        }
    };
    let mut accept_timeout = pin!(accept_timeout);

    // Accept phase: collect accepts
    loop {
        let actor_msg = tokio::select! {
            biased;
            msg = msg_rx.recv() => msg,
            _ = &mut accept_timeout => {
                debug!("accept phase timed out");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(proposal.attempt()),
                };
            }
        };

        let Some(actor_msg) = actor_msg else {
            debug!("message channel closed during accept phase");
            return ProposeResult::Closed;
        };

        if actor_msg.seq != seq {
            trace!(
                expected = seq,
                got = actor_msg.seq,
                "ignoring stale message"
            );
            continue;
        }

        let msg = actor_msg.msg;
        let acceptor_id = actor_msg.acceptor_id;

        // Get the accepted key if present
        let Some((accepted_p, _)) = &msg.accepted else {
            continue;
        };

        if !learner.validate(accepted_p) {
            trace!("ignoring invalid accepted");
            continue;
        }

        let accepted_key = Some(accepted_p.key());

        let result = core.handle_accepted(acceptor_id, accepted_key, proposal.clone());

        match result {
            crate::core::AcceptPhaseResult::Learned { proposal, value } => {
                debug!(quorum = core.quorum(), "accept phase complete");
                return ProposeResult::Accepted(proposal, value);
            }
            crate::core::AcceptPhaseResult::Rejected { superseded_by } => {
                debug!(?superseded_by, "rejected: higher proposal accepted");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(superseded_by.attempt()),
                };
            }
            crate::core::AcceptPhaseResult::Pending => {
                // Need more accepts
            }
        }
    }
}
