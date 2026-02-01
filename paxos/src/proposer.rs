//! Proposer implementation
//!
//! Uses a per-connection actor model where each acceptor connection
//! runs both phases independently, synchronized via quorum barriers.

use std::{
    collections::{HashMap, hash_map},
    future::poll_fn,
    sync::Arc,
    task::Poll,
};

use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use rand::Rng;
use tokio::sync::{mpsc, watch};

use crate::{
    AcceptorConn, AcceptorMessage, AcceptorRequest, Connector, LazyConnection, Learner, Proposal,
    ProposerConfig, Sleep, quorum::QuorumTracker,
};

/// Run the proposer loop with backoff on rejection.
///
/// # Errors
///
/// Returns an error if:
/// - The connector fails to establish a connection to an acceptor
/// - The learner fails to apply a learned proposal
/// - Communication with acceptors fails
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

        // Create a tracker for this round
        let mut tracker: QuorumTracker<L> = QuorumTracker::new(cached_connections.len());

        let msg = tokio::select! {
            res = learn_from(&mut cached_connections, &learner, &mut tracker, round) => {
                let (proposal, message) = res.map_err(ProposerError::Learner)?;
                attempt = <L::Proposal as Proposal>::AttemptId::default();
                learner.apply(proposal, message).await.map_err(ProposerError::Learner)?;
                continue;
            }
            msg = messages.next() => {
                match msg {
                    Some(msg) => msg,
                    None => return Ok(()),
                }
            }
        };

        for consecutive_rejections in 0.. {
            // Check if too many acceptors have failed to connect
            const FAILURE_THRESHOLD: u32 = 10;
            let quorum = tracker.quorum();
            let total = cached_connections.len();
            let failed = cached_connections
                .values()
                .filter(|c| c.consecutive_failures() >= FAILURE_THRESHOLD)
                .count();
            if failed > total - quorum {
                return Err(ProposerError::QuorumUnreachable {
                    failed,
                    total,
                    failure_threshold: FAILURE_THRESHOLD,
                });
            }

            let proposal = learner.propose(attempt);
            match propose(
                &mut cached_connections,
                &learner,
                proposal,
                msg.clone(),
                &mut tracker,
            )
            .await
            {
                ProposeResult::Accepted(proposal, message) => {
                    attempt = <L::Proposal as Proposal>::AttemptId::default();
                    learner
                        .apply(proposal, message)
                        .await
                        .map_err(ProposerError::Learner)?;
                    break;
                }
                ProposeResult::Rejected { min_attempt } => {
                    attempt = min_attempt;
                }
            }

            // Apply backoff before proposing if we've been rejected
            let backoff = config
                .backoff
                .duration(consecutive_rejections, &mut config.rng);
            config.sleep.sleep(backoff).await;
        }
    }
}

#[derive(Debug)]
pub enum ProposerError<E> {
    Learner(E),
    /// Too many acceptors are unreachable - cannot form a quorum
    QuorumUnreachable {
        /// Number of acceptors that failed to connect
        failed: usize,
        /// Total number of acceptors
        total: usize,
        /// Threshold for failure detection
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

enum ProposeResult<L: Learner> {
    /// Our proposal was accepted,
    /// Or we discovered another proposal was already accepted by quorum
    Accepted(L::Proposal, L::Message),
    /// Our proposal was rejected, retry with higher attempt
    Rejected {
        min_attempt: <L::Proposal as Proposal>::AttemptId,
    },
}

/// Learn a value from acceptors (background listening)
async fn learn_from<L, A>(
    acceptors: &mut HashMap<L::AcceptorAddr, A>,
    learner: &L,
    tracker: &mut QuorumTracker<L>,
    round: <L::Proposal as Proposal>::RoundId,
) -> Result<(L::Proposal, L::Message), L::Error>
where
    L: Learner,
    A: AcceptorConn<L>,
{
    poll_fn(|cx| {
        for acceptor in acceptors.values_mut() {
            loop {
                match acceptor.try_poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        if let Some((proposal, message)) = msg.accepted {
                            if proposal.round() != round {
                                continue;
                            }
                            if !learner.validate(&proposal) {
                                continue;
                            }
                            if let Some((learned_p, learned_m)) = tracker.track(proposal, message) {
                                return Poll::Ready(Ok((learned_p.clone(), learned_m.clone())));
                            }
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
        }
        Poll::Pending
    })
    .await
}

/// A barrier that releases once a quorum of waiters arrive.
/// Unlike `tokio::Barrier`, this doesn't require ALL waiters to arrive.
struct QuorumBarrier {
    inner: tokio::sync::watch::Sender<usize>,
}

impl QuorumBarrier {
    fn new(quorum: usize) -> Self {
        Self {
            inner: tokio::sync::watch::channel(quorum).0,
        }
    }

    /// Signal arrival and wait until quorum is reached.
    async fn arrive_and_wait(&self) {
        self.inner.send_modify(|x| *x = x.saturating_sub(1));
        _ = self.inner.subscribe().wait_for(|x| *x == 0).await;
    }
}

/// Propose a value to acceptors using concurrent per-connection actors
#[expect(
    clippy::too_many_lines,
    reason = "function is cohesive, splitting would hurt readability"
)]
async fn propose<L, A>(
    acceptors: &mut HashMap<L::AcceptorAddr, A>,
    learner: &L,
    proposal: L::Proposal,
    message: L::Message,
    tracker: &mut QuorumTracker<L>,
) -> ProposeResult<L>
where
    L: Learner,
    A: AcceptorConn<L>,
{
    let quorum = tracker.quorum();
    let proposal_key = proposal.key();

    // Channel for actors to send their promise responses
    let (promise_tx, mut promise_rx) = mpsc::unbounded_channel::<AcceptorMessage<L>>();

    // Channel for actors to send their accepted responses
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel::<AcceptorMessage<L>>();

    // Barrier for prepare phase - releases when quorum promises received
    let prepare_barrier = Arc::new(QuorumBarrier::new(quorum));

    // Watch channel to signal what message to use in accept phase
    // (might change if we discover a previously accepted value)
    let (accept_msg_tx, accept_msg_rx) = watch::channel(None::<(L::Proposal, L::Message)>);

    // Spawn actor for each acceptor
    let mut actor_handles = Vec::new();
    for (_, conn) in acceptors.iter_mut() {
        let prepare_msg = AcceptorRequest::Prepare(proposal.clone());
        let promise_tx = promise_tx.clone();
        let accepted_tx = accepted_tx.clone();
        let prepare_barrier = Arc::clone(&prepare_barrier);
        let mut accept_msg_rx = accept_msg_rx.clone();

        // This runs both phases for this connection
        let actor = async move {
            // Phase 1: Send prepare, receive promise
            if conn.send(prepare_msg).await.is_err() {
                return;
            }
            let Some(Ok(promise)) = conn.next().await else {
                return;
            };

            // Send promise to coordinator and wait for quorum
            let _ = promise_tx.send(promise);
            prepare_barrier.arrive_and_wait().await;

            // Get the accept message (coordinator decides based on promises)
            let accept_msg = loop {
                if let Some(msg) = accept_msg_rx.borrow_and_update().clone() {
                    break msg;
                }
                if accept_msg_rx.changed().await.is_err() {
                    return;
                }
            };

            // Phase 2: Send accept, receive accepted
            if conn
                .send(AcceptorRequest::Accept(accept_msg.0, accept_msg.1))
                .await
                .is_err()
            {
                return;
            }
            let Some(Ok(accepted)) = conn.next().await else {
                return;
            };

            // Send accepted to coordinator
            let _ = accepted_tx.send(accepted);
        };

        actor_handles.push(actor);
    }

    // Drop our copies of the senders so channels close when actors finish
    drop(promise_tx);
    drop(accepted_tx);

    // Run all actors concurrently in background
    let actors = async {
        futures::future::join_all(actor_handles).await;
    };
    tokio::pin!(actors);

    // Save attempt for fallback case (before proposal moves into coordinator)
    let fallback_attempt = proposal.attempt();

    // Coordinator: collect promises, then accepts
    let coordinator = async {
        // ========== Phase 1: Collect promises ==========
        let mut promises = 0;
        let mut highest_accepted: Option<(L::Proposal, L::Message)> = None;

        while let Some(msg) = promise_rx.recv().await {
            // Check if someone already accepted a higher proposal in THIS round
            if let Some((accepted_p, accepted_m)) = msg.accepted
                && accepted_p.round() == proposal.round()
                && learner.validate(&accepted_p)
            {
                // Track acceptance counts using shared tracker
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

            // Check promise - validate before trusting
            if let Some(promised) = msg.promised
                && learner.validate(&promised)
                && promised.key() > proposal_key
            {
                return ProposeResult::Rejected {
                    min_attempt: L::Proposal::next_attempt(promised.attempt()),
                };
            }

            promises += 1;
            if promises >= quorum {
                break;
            }
        }

        if promises < quorum {
            return ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            };
        }

        // Decide what message to use and signal actors
        let (proposal, message) = highest_accepted.unwrap_or((proposal, message));
        let proposal_key = proposal.key();
        let _ = accept_msg_tx.send(Some((proposal.clone(), message.clone())));

        // ========== Phase 2: Collect accepts ==========
        let mut accepts = 0;

        while let Some(msg) = accepted_rx.recv().await {
            if let Some((accepted_p, _)) = &msg.accepted {
                // Validate before trusting
                if !learner.validate(accepted_p) {
                    continue;
                }
                if accepted_p.key() == proposal_key {
                    accepts += 1;
                    if accepts >= quorum {
                        break;
                    }
                } else if accepted_p.key() > proposal_key {
                    return ProposeResult::Rejected {
                        min_attempt: L::Proposal::next_attempt(accepted_p.attempt()),
                    };
                }
            }
        }

        if accepts >= quorum {
            ProposeResult::Accepted(proposal, message)
        } else {
            ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(proposal.attempt()),
            }
        }
    };

    // Run coordinator and actors concurrently, return when coordinator finishes
    // (actors may still be running if some connections are slow)
    tokio::select! {
        result = coordinator => result,
        () = &mut actors => {
            // All actors finished but coordinator didn't get enough responses
            ProposeResult::Rejected {
                min_attempt: L::Proposal::next_attempt(fallback_attempt),
            }
        }
    }
}
