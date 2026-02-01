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
    Sleep, quorum::QuorumTracker,
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
    seq: u64,
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
#[allow(clippy::too_many_lines)]
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

                // Wait for a valid promise for THIS proposal (skip stale promises from previous prepares)
                let promise = loop {
                    let Some(Ok(msg)) = conn.next().await else {
                        break None;
                    };
                    // Only accept promises for our proposal or higher
                    // (lower = stale from previous prepare)
                    if msg.promised.key() >= proposal_key {
                        trace!("actor received promise");
                        break Some(msg);
                    }
                    trace!("actor received stale promise, waiting for current");
                };

                let Some(promise) = promise else {
                    warn!("actor connection closed while waiting for promise");
                    continue;
                };

                // Send to coordinator
                let _ = msg_tx.send(ActorMessage {
                    seq: last_seq,
                    msg: promise,
                });
                trace!("actor sent promise to coordinator");

                // Wait for Accept command
                'accept_wait: loop {
                    if state_rx.changed().await.is_err() {
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
                            break;
                        }

                        // Wait for accepted response
                        let accepted = loop {
                            let Some(Ok(msg)) = conn.next().await else {
                                break None;
                            };
                            if msg
                                .accepted
                                .as_ref()
                                .is_some_and(|(p, _)| p.key() >= proposal_key)
                            {
                                trace!("actor received accepted");
                                break Some(msg);
                            }
                            trace!("actor received non-accepted message, waiting");
                        };

                        let Some(accepted) = accepted else {
                            warn!("actor connection closed while waiting for accepted");
                            break;
                        };

                        let _ = msg_tx.send(ActorMessage {
                            seq: last_seq,
                            msg: accepted,
                        });
                        trace!("actor sent accepted to coordinator");
                        break;
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

/// Run the proposer loop.
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
    let mut attempt = <L::Proposal as Proposal>::AttemptId::default();
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ActorMessage<L>>();
    let mut manager: ActorManager<L, C> = ActorManager::new(learner.node_id(), connector, msg_tx);

    loop {
        let round = learner.current_round();
        debug!(?round, "starting new round");

        // Sync actors to current acceptor set
        manager.sync_actors(learner.acceptors());

        let num_acceptors = manager.num_actors();
        let mut tracker: QuorumTracker<L> = QuorumTracker::new(num_acceptors);
        debug!(
            num_acceptors,
            quorum = tracker.quorum(),
            "tracker initialized"
        );

        // Wait for a message to propose, or learn from background
        let msg = loop {
            tokio::select! {
                biased;
                Some(actor_msg) = msg_rx.recv() => {
                    trace!("received background message from actor");
                    // Process background messages (broadcasts from acceptors)
                    if let Some((p, m)) = process_background_message(&mut tracker, &learner, actor_msg, round) {
                        debug!("learned value from background broadcast");
                        learner.apply(p, m).await?;
                        attempt = <L::Proposal as Proposal>::AttemptId::default();
                    }
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

        // Proposal loop with retries
        for consecutive_rejections in 0.. {
            let proposal = learner.propose(attempt);
            debug!(?attempt, consecutive_rejections, "attempting proposal");

            let result = run_proposal(
                &manager,
                &mut msg_rx,
                &learner,
                proposal.clone(),
                msg.clone(),
                &mut tracker,
                round,
                &config.sleep,
                config.phase_timeout,
            )
            .await;

            match result {
                ProposeResult::Accepted(proposal, message) => {
                    debug!("proposal accepted");
                    learner.apply(proposal, message).await?;
                    attempt = <L::Proposal as Proposal>::AttemptId::default();
                    break;
                }
                ProposeResult::Rejected { min_attempt } => {
                    debug!(?min_attempt, "proposal rejected, will retry");
                    attempt = min_attempt;
                }
            }

            // Backoff before retry
            let backoff = config
                .backoff
                .duration(consecutive_rejections, &mut config.rng);
            trace!(?backoff, "backing off before retry");
            config.sleep.sleep(backoff).await;
        }
    }
}

/// Process a background message (broadcasts)
fn process_background_message<L: Learner>(
    tracker: &mut QuorumTracker<L>,
    learner: &L,
    actor_msg: ActorMessage<L>,
    round: <L::Proposal as Proposal>::RoundId,
) -> Option<(L::Proposal, L::Message)> {
    let (proposal, message) = actor_msg.msg.accepted?;
    if proposal.round() != round {
        return None;
    }
    if !learner.validate(&proposal) {
        return None;
    }
    let (learned_p, learned_m) = tracker.track(proposal, message)?;
    Some((learned_p.clone(), learned_m.clone()))
}

/// Run a single proposal attempt through both phases
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
    let quorum = tracker.quorum();
    let proposal_key = proposal.key();

    // Start prepare phase
    manager.start_prepare(proposal.clone());
    let seq = manager.current_seq();
    trace!(seq, quorum, "collecting promises");

    // Collect promises with optional timeout
    let mut promises = 0;
    let mut highest_accepted: Option<(L::Proposal, L::Message)> = None;

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

    loop {
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
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            };
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

        // Check for already accepted values in this round
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

            if accepted_p.key() >= proposal_key {
                debug!("rejected: higher proposal already accepted");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                };
            }

            highest_accepted = match &highest_accepted {
                Some((h, _)) if h.key() >= accepted_p.key() => highest_accepted,
                _ => Some((accepted_p, accepted_m)),
            };
        }

        let promised_p = &msg.promised;

        if !learner.validate(promised_p) {
            trace!("ignoring invalid promise");
            continue;
        }

        if promised_p.key() > proposal_key {
            debug!("rejected: higher proposal promised");
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(promised_p.attempt()),
            };
        }

        if promised_p.key() != proposal_key {
            trace!("ignoring promise for different proposal");
            continue;
        }

        promises += 1;
        trace!(promises, quorum, "received promise");
        if promises >= quorum {
            debug!(promises, "prepare phase complete");
            break;
        }
    }

    // Decide what value to accept
    let message = highest_accepted.map_or(message, |(_, m)| m);
    let proposal_key = proposal.key();

    // Start accept phase
    manager.transition_to_accept(proposal.clone(), message.clone());
    trace!("collecting accepts");

    // Collect accepts with optional timeout
    let mut accepts = 0;

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
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            };
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

        if let Some((accepted_p, _)) = &msg.accepted {
            if !learner.validate(accepted_p) {
                trace!("ignoring invalid accepted");
                continue;
            }
            if accepted_p.key() == proposal_key {
                accepts += 1;
                trace!(accepts, quorum, "received accept");
                if accepts >= quorum {
                    debug!(accepts, "accept phase complete");
                    return ProposeResult::Accepted(proposal, message);
                }
            } else if accepted_p.key() > proposal_key {
                debug!("rejected: higher proposal accepted");
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                };
            }
        }
    }
}
