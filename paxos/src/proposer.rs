//! Proposer implementation
//!
//! Uses a fully actor-based model where each acceptor connection runs as an
//! independent state machine, coordinated via watch channels.

use std::{
    collections::{HashMap, hash_map},
    future::poll_fn,
    task::Poll,
};

use futures::{SinkExt, Stream, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, watch};

use crate::{
    AcceptorConn, AcceptorMessage, AcceptorRequest, Connector, LazyConnection, Learner, Proposal,
    ProposerConfig, Sleep, quorum::QuorumTracker,
};

// ============================================================================
// Shared State Types
// ============================================================================

/// Phase of the current proposal attempt
#[derive(Default)]
enum Phase<L: Learner> {
    /// No active proposal
    #[default]
    Idle,
    /// Prepare phase - actors should send this proposal
    Prepare(L::Proposal),
    /// Accept phase - actors should send this proposal + message
    Accept {
        proposal: L::Proposal,
        message: L::Message,
    },
}

impl<L: Learner> Clone for Phase<L> {
    fn clone(&self) -> Self {
        match self {
            Phase::Idle => Phase::Idle,
            Phase::Prepare(p) => Phase::Prepare(p.clone()),
            Phase::Accept { proposal, message } => Phase::Accept {
                proposal: proposal.clone(),
                message: message.clone(),
            },
        }
    }
}

/// Shared state broadcast from coordinator to all actors
struct CoordinatorState<L: Learner> {
    /// Current round we're operating in
    round: <L::Proposal as Proposal>::RoundId,
    /// Current phase and proposal data
    phase: Phase<L>,
    /// Sequence number - increments on each state change
    /// Actors use this to detect they need to restart their state machine
    seq: u64,
}

impl<L: Learner> Clone for CoordinatorState<L> {
    fn clone(&self) -> Self {
        Self {
            round: self.round,
            phase: self.phase.clone(),
            seq: self.seq,
        }
    }
}

impl<L: Learner> CoordinatorState<L> {
    fn new(round: <L::Proposal as Proposal>::RoundId) -> Self {
        Self {
            round,
            phase: Phase::Idle,
            seq: 0,
        }
    }
}

/// Messages from actors back to coordinator
enum ActorMessage<L: Learner> {
    /// Received a promise response
    Promise { seq: u64, msg: AcceptorMessage<L> },
    /// Received an accepted response
    Accepted { seq: u64, msg: AcceptorMessage<L> },
}

// ============================================================================
// Actor Implementation
// ============================================================================

/// Run an actor for a single acceptor connection.
///
/// The actor watches for state changes from the coordinator and reacts accordingly.
/// It handles prepare and accept phases across multiple rounds.
async fn run_actor<L, A>(
    conn: &mut A,
    mut state_rx: watch::Receiver<CoordinatorState<L>>,
    msg_tx: mpsc::UnboundedSender<ActorMessage<L>>,
) where
    L: Learner,
    A: AcceptorConn<L>,
{
    let mut last_seq = 0u64;

    loop {
        // Wait for state change using wait_for
        let state = {
            match state_rx.wait_for(|s| s.seq != last_seq).await {
                Ok(state) => {
                    last_seq = state.seq;
                    state.clone()
                }
                Err(_) => return, // Coordinator dropped
            }
        };

        match state.phase {
            Phase::Prepare(proposal) => {
                // Send prepare
                if conn.send(AcceptorRequest::Prepare(proposal)).await.is_err() {
                    continue; // Connection failed, will retry on next state change
                }

                // Wait for a valid promise (skip broadcasts that have no `promised` field)
                let promise = loop {
                    let Some(Ok(msg)) = conn.next().await else {
                        break None;
                    };
                    // A valid promise response has `promised` set
                    if msg.promised.is_some() {
                        break Some(msg);
                    }
                    // Otherwise it's a broadcast - skip it
                };

                let Some(promise) = promise else {
                    continue; // Connection failed
                };

                // Send to coordinator
                let _ = msg_tx.send(ActorMessage::Promise {
                    seq: last_seq,
                    msg: promise,
                });

                // Now wait for Accept phase
                'accept_wait: loop {
                    if state_rx.changed().await.is_err() {
                        return;
                    }
                    let new_state = state_rx.borrow_and_update().clone();

                    // Check if a new proposal started (different seq = restart)
                    if new_state.seq != last_seq {
                        // Don't update last_seq here - let outer loop handle it
                        break 'accept_wait;
                    }

                    // Check if we moved to Accept phase for current proposal
                    if let Phase::Accept {
                        proposal,
                        message: msg_value,
                    } = new_state.phase
                    {
                        let proposal_key = proposal.key();

                        // Send accept
                        if conn
                            .send(AcceptorRequest::Accept(proposal, msg_value))
                            .await
                            .is_err()
                        {
                            break; // Connection failed
                        }

                        // Wait for accepted response (skip stale broadcasts)
                        let accepted = loop {
                            let Some(Ok(msg)) = conn.next().await else {
                                break None;
                            };
                            // Check if this is a response for our proposal or a higher one
                            if let Some((accepted_p, _)) = &msg.accepted {
                                // Skip broadcasts for lower proposals (stale)
                                // Accept broadcasts for our proposal or higher (need to handle)
                                if accepted_p.key() >= proposal_key {
                                    break Some(msg);
                                }
                            }
                            // Otherwise it's a stale broadcast - skip it
                        };

                        let Some(accepted) = accepted else {
                            break; // Connection failed
                        };

                        // Send to coordinator
                        let _ = msg_tx.send(ActorMessage::Accepted {
                            seq: last_seq,
                            msg: accepted,
                        });
                        break;
                    }

                    // Phase went back to Idle or stayed at Prepare - keep waiting
                }
            }
            // Idle or Accept without Prepare - wait for next state change
            Phase::Idle | Phase::Accept { .. } => {}
        }
    }
}

// ============================================================================
// Coordinator Implementation
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

/// Run the proposer loop.
///
/// # Errors
///
/// Returns an error if:
/// - Too many acceptors are unreachable
/// - The learner fails to apply a learned proposal
pub async fn run_proposer<L, C, M, S, R>(
    mut learner: L,
    connector: C,
    mut messages: M,
    mut config: ProposerConfig<S, R>,
) -> Result<(), ProposerError<L::Error>>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
    M: Stream<Item = L::Message> + Unpin,
    S: Sleep,
    R: Rng,
{
    let mut attempt = <L::Proposal as Proposal>::AttemptId::default();
    let mut cached_connections = HashMap::new();
    let mut scratch_connections = HashMap::new();

    loop {
        let round = learner.current_round();

        // Update connection cache
        for addr in learner.acceptors() {
            let hash_map::Entry::Vacant(e) = scratch_connections.entry(addr) else {
                continue;
            };
            let conn = cached_connections
                .remove(e.key())
                .unwrap_or_else(|| LazyConnection::new(e.key().clone(), connector.clone()));
            e.insert(conn);
        }
        std::mem::swap(&mut cached_connections, &mut scratch_connections);
        scratch_connections.clear();

        let num_acceptors = cached_connections.len();
        let mut tracker: QuorumTracker<L> = QuorumTracker::new(num_acceptors);

        // Run this round with actors
        let round_result = run_round(
            &mut cached_connections,
            &learner,
            &mut tracker,
            round,
            &mut attempt,
            &mut messages,
            &mut config,
        )
        .await;

        match round_result {
            RoundResult::Continue => {}
            RoundResult::Learned(proposal, message) => {
                attempt = <L::Proposal as Proposal>::AttemptId::default();
                learner
                    .apply(proposal, message)
                    .await
                    .map_err(ProposerError::Learner)?;
            }
            RoundResult::Error(e) => return Err(ProposerError::Learner(e)),
            RoundResult::StreamEnded => return Ok(()),
        }
    }
}

/// Result of processing a round
enum RoundResult<L: Learner> {
    Continue,
    Learned(L::Proposal, L::Message),
    Error(L::Error),
    StreamEnded,
}

/// Run a single round with multiple proposal attempts.
/// Actors are created for this round and handle all proposals within it.
async fn run_round<L, A, M, S, R>(
    connections: &mut HashMap<L::NodeId, A>,
    learner: &L,
    tracker: &mut QuorumTracker<L>,
    round: <L::Proposal as Proposal>::RoundId,
    attempt: &mut <L::Proposal as Proposal>::AttemptId,
    messages: &mut M,
    config: &mut ProposerConfig<S, R>,
) -> RoundResult<L>
where
    L: Learner,
    A: AcceptorConn<L>,
    M: Stream<Item = L::Message> + Unpin,
    S: Sleep,
    R: Rng,
{
    // Create coordinator state channel for this round
    let (state_tx, state_rx) = watch::channel(CoordinatorState::new(round));

    // Create message channel from actors
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ActorMessage<L>>();

    // Spawn actors for each connection
    let mut actor_handles = Vec::new();
    for (_, conn) in connections.iter_mut() {
        let state_rx = state_rx.clone();
        let msg_tx = msg_tx.clone();
        actor_handles.push(run_actor(conn, state_rx, msg_tx));
    }
    drop(msg_tx); // Drop our copy so channel closes when actors finish

    // Run actors in background
    let actors = async {
        futures::future::join_all(actor_handles).await;
    };
    tokio::pin!(actors);

    // Wait for a message to propose, or learn from background
    let msg = tokio::select! {
        biased;
        () = &mut actors => {
            // All actors finished - should not happen during normal operation
            return RoundResult::Continue;
        }
        res = learn_from_actors(&mut msg_rx, learner, tracker, round) => {
            match res {
                Ok((proposal, message)) => return RoundResult::Learned(proposal, message),
                Err(e) => return RoundResult::Error(e),
            }
        }
        msg = messages.next() => {
            match msg {
                Some(msg) => msg,
                None => return RoundResult::StreamEnded,
            }
        }
    };

    // Proposal loop with retries - actors persist across attempts within this round
    for consecutive_rejections in 0.. {
        let proposal = learner.propose(*attempt);

        // Run proposal while also driving actors
        let result = tokio::select! {
            biased;
            () = &mut actors => {
                // Actors finished without quorum
                ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(proposal.attempt()),
                }
            }
            result = run_proposal(
                &state_tx,
                &mut msg_rx,
                learner,
                proposal.clone(),
                msg.clone(),
                tracker,
            ) => result,
        };

        match result {
            ProposeResult::Accepted(proposal, message) => {
                // Reset to Idle before returning so actors can clean up
                state_tx.send_modify(|s| {
                    s.seq += 1;
                    s.phase = Phase::Idle;
                });
                return RoundResult::Learned(proposal, message);
            }
            ProposeResult::Rejected { min_attempt } => {
                *attempt = min_attempt;
            }
        }

        // Backoff before retry
        let backoff = config
            .backoff
            .duration(consecutive_rejections, &mut config.rng);
        config.sleep.sleep(backoff).await;
    }

    // Shouldn't reach here (infinite loop above)
    RoundResult::Continue
}

/// Learn values from actor messages (background listening)
async fn learn_from_actors<L: Learner>(
    msg_rx: &mut mpsc::UnboundedReceiver<ActorMessage<L>>,
    learner: &L,
    tracker: &mut QuorumTracker<L>,
    round: <L::Proposal as Proposal>::RoundId,
) -> Result<(L::Proposal, L::Message), L::Error> {
    loop {
        let Some(actor_msg) = msg_rx.recv().await else {
            // Channel closed - this shouldn't happen during normal operation
            return poll_fn(|_| Poll::Pending).await;
        };

        let msg = match actor_msg {
            ActorMessage::Promise { msg, .. } | ActorMessage::Accepted { msg, .. } => msg,
        };

        if let Some((proposal, message)) = msg.accepted {
            if proposal.round() != round {
                continue;
            }
            if !learner.validate(&proposal) {
                continue;
            }
            if let Some((learned_p, learned_m)) = tracker.track(proposal, message) {
                return Ok((learned_p.clone(), learned_m.clone()));
            }
        }
    }
}

/// Run a single proposal attempt through both phases
async fn run_proposal<L: Learner>(
    state_tx: &watch::Sender<CoordinatorState<L>>,
    msg_rx: &mut mpsc::UnboundedReceiver<ActorMessage<L>>,
    learner: &L,
    proposal: L::Proposal,
    message: L::Message,
    tracker: &mut QuorumTracker<L>,
) -> ProposeResult<L> {
    let quorum = tracker.quorum();
    let proposal_key = proposal.key();
    let round = proposal.round();

    // Start prepare phase
    let seq = state_tx.borrow().seq + 1;
    state_tx.send_modify(|s| {
        s.seq = seq;
        s.phase = Phase::Prepare(proposal.clone());
    });

    // Collect promises
    let mut promises = 0;
    let mut highest_accepted: Option<(L::Proposal, L::Message)> = None;

    loop {
        let Some(actor_msg) = msg_rx.recv().await else {
            // All actors finished without quorum
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            };
        };

        let ActorMessage::Promise { seq: msg_seq, msg } = actor_msg else {
            continue; // Wrong phase message
        };

        if msg_seq != seq {
            continue; // Stale message from previous attempt
        }

        // Check for already accepted values in this round
        if let Some((accepted_p, accepted_m)) = msg.accepted
            && accepted_p.round() == round
            && learner.validate(&accepted_p)
        {
            // Track for quorum detection
            if let Some((learned_p, learned_m)) =
                tracker.track(accepted_p.clone(), accepted_m.clone())
                && learned_p.key() != proposal_key
            {
                return ProposeResult::Accepted(learned_p.clone(), learned_m.clone());
            }

            if accepted_p.key() >= proposal_key {
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                };
            }

            highest_accepted = match &highest_accepted {
                Some((h, _)) if h.key() >= accepted_p.key() => highest_accepted,
                _ => Some((accepted_p, accepted_m)),
            };
        }

        // Check promise - must have a promised value to count as a valid promise
        let Some(promised_p) = msg.promised else {
            // No promise in message - this is just a broadcast, ignore it
            continue;
        };

        if !learner.validate(&promised_p) {
            continue;
        }

        // Check if the acceptor promised a higher proposal
        if promised_p.key() > proposal_key {
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(promised_p.attempt()),
            };
        }

        // Only count if acceptor promised exactly our proposal
        if promised_p.key() != proposal_key {
            continue;
        }

        promises += 1;
        if promises >= quorum {
            break;
        }
    }

    // Decide what value to accept - we use OUR proposal but adopt THEIR message if any
    let message = highest_accepted.map_or(message, |(_, m)| m);
    let proposal_key = proposal.key();

    // Start accept phase
    state_tx.send_modify(|s| {
        s.phase = Phase::Accept {
            proposal: proposal.clone(),
            message: message.clone(),
        };
    });

    // Collect accepts
    let mut accepts = 0;

    loop {
        let Some(actor_msg) = msg_rx.recv().await else {
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            };
        };

        let ActorMessage::Accepted { seq: msg_seq, msg } = actor_msg else {
            continue;
        };

        if msg_seq != seq {
            continue;
        }

        if let Some((accepted_p, _)) = &msg.accepted {
            if !learner.validate(accepted_p) {
                continue;
            }
            if accepted_p.key() == proposal_key {
                accepts += 1;
                if accepts >= quorum {
                    return ProposeResult::Accepted(proposal, message);
                }
            } else if accepted_p.key() > proposal_key {
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                };
            }
        }
    }
}

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug)]
pub enum ProposerError<E> {
    Learner(E),
    QuorumUnreachable {
        failed: usize,
        total: usize,
        failure_threshold: u32,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for ProposerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposerError::Learner(e) => write!(f, "learner error: {e}"),
            ProposerError::QuorumUnreachable {
                failed,
                total,
                failure_threshold,
            } => write!(
                f,
                "quorum unreachable: {failed}/{total} acceptors failed after {failure_threshold} attempts each"
            ),
        }
    }
}

impl<E> std::error::Error for ProposerError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProposerError::Learner(e) => Some(e),
            ProposerError::QuorumUnreachable { .. } => None,
        }
    }
}
