//! Core Paxos traits

use std::{future::Future, hash::Hash};

use futures::{Sink, Stream};

use crate::messages::{AcceptorMessage, AcceptorRequest};

/// A proposal that can be ordered by (round, attempt)
pub trait Proposal: Clone {
    type RoundId: Copy + Ord + Default + core::fmt::Debug + core::hash::Hash;
    type AttemptId: Copy + Ord + Default + core::fmt::Debug;

    fn round(&self) -> Self::RoundId;
    fn attempt(&self) -> Self::AttemptId;

    /// Return the next attempt ID after the given one.
    /// Used when a proposal is rejected to retry with a higher attempt.
    fn next_attempt(attempt: Self::AttemptId) -> Self::AttemptId;

    /// Get the ordering key for this proposal (round, attempt).
    /// Used for comparing proposals in the Paxos protocol.
    fn key(&self) -> ProposalKey<Self::RoundId, Self::AttemptId> {
        ProposalKey(self.round(), self.attempt())
    }
}

/// Ordering key for proposals - compares by (round, attempt) only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalKey<R, A>(pub(crate) R, pub(crate) A);

impl<R: Copy, A: Copy> ProposalKey<R, A> {
    /// Create a new proposal key.
    #[must_use]
    pub fn new(round: R, attempt: A) -> Self {
        Self(round, attempt)
    }

    /// Get the round ID from this key.
    #[must_use]
    pub fn round(&self) -> R {
        self.0
    }

    /// Get the attempt ID from this key.
    #[must_use]
    pub fn attempt(&self) -> A {
        self.1
    }
}

/// State machine that learns from consensus
#[expect(async_fn_in_trait)]
pub trait Learner {
    type Proposal: Proposal;
    type Message: Clone;
    type Error: core::error::Error + Send;
    type AcceptorAddr: Clone + Eq + Hash;

    /// Current round (next to be learned)
    fn current_round(&self) -> <Self::Proposal as Proposal>::RoundId;

    /// Validate a proposal (signatures, authorization, etc.)
    fn validate(&self, proposal: &Self::Proposal) -> bool;

    /// Current acceptor set based on learned state
    fn acceptors(&self) -> impl IntoIterator<Item = Self::AcceptorAddr>;

    /// Create a signed proposal for the current round and attempt.
    /// The message is sent separately during Accept phase.
    fn propose(&self, attempt: <Self::Proposal as Proposal>::AttemptId) -> Self::Proposal;

    /// Apply a learned proposal + message to the state machine
    async fn apply(
        &mut self,
        proposal: Self::Proposal,
        message: Self::Message,
    ) -> Result<(), Self::Error>;
}

/// Acceptor that can persist accepted proposals
#[expect(async_fn_in_trait)]
pub trait Acceptor: Learner {
    /// Durably persist accepted proposal + message (must complete before responding)
    async fn accept(
        &mut self,
        proposal: Self::Proposal,
        message: Self::Message,
    ) -> Result<(), Self::Error>;
}

/// Shared state for an acceptor, allowing multiple connections to coordinate.
///
/// This is the key abstraction for multi-proposer support - all connections
/// to the same acceptor must share this state. State is tracked per-round
/// to support Multi-Paxos. Also handles broadcasting to learners.
pub trait AcceptorStateStore<L: Learner> {
    /// Receiver type for learner subscriptions.
    type Receiver: futures::Stream<Item = (L::Proposal, L::Message)> + Unpin;

    /// Get the state for a specific round.
    fn get(&self, round: <L::Proposal as Proposal>::RoundId) -> crate::RoundState<L>;

    /// Try to promise this proposal for a round.
    ///
    /// # Errors
    /// Returns `Err(current_state)` if the proposal is dominated by a higher promise.
    fn promise(&self, proposal: &L::Proposal) -> Result<(), crate::RoundState<L>>;

    /// Try to accept this proposal + message for a round.
    /// On success, broadcasts to all subscribed learners.
    ///
    /// # Errors
    /// Returns `Err(current_state)` if the proposal is dominated by a higher promise.
    fn accept(
        &self,
        proposal: &L::Proposal,
        message: &L::Message,
    ) -> Result<(), crate::RoundState<L>>;

    /// Subscribe to receive accepted (proposal, message) pairs.
    fn subscribe(&self) -> Self::Receiver;

    /// Get accepted (proposal, message) pairs from a given round onwards (for learner sync).
    /// Returns in round order.
    fn accepted_from(
        &self,
        from_round: <L::Proposal as Proposal>::RoundId,
    ) -> Vec<(L::Proposal, L::Message)>;

    /// Get the highest round that has been accepted (for sync complete message).
    fn highest_accepted_round(&self) -> Option<<L::Proposal as Proposal>::RoundId>;
}

/// Connects to acceptors by address.
///
/// Implementations should handle backoff/retry logic internally when connections fail.
/// The connector is cloned per-address, so `&mut self` can be used to track retry state.
pub trait Connector<L: Learner>: Clone {
    type Connection: AcceptorConn<L>;
    type Error: core::error::Error;
    type ConnectFuture: Future<Output = Result<Self::Connection, Self::Error>>;

    fn connect(&mut self, addr: &L::AcceptorAddr) -> Self::ConnectFuture;
}

/// Connection to an acceptor
pub trait AcceptorConn<L: Learner>:
    Sink<AcceptorRequest<L>, Error = L::Error>
    + Stream<Item = Result<AcceptorMessage<L>, L::Error>>
    + Unpin
{
}

impl<L, T> AcceptorConn<L> for T
where
    L: Learner,
    T: Sink<AcceptorRequest<L>, Error = L::Error>
        + Stream<Item = Result<AcceptorMessage<L>, L::Error>>
        + Unpin,
{
}
