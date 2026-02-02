//! Core Paxos traits

use core::future::Future;
use core::{fmt, hash::Hash};

use futures::{Sink, Stream};

use crate::messages::{AcceptorMessage, AcceptorRequest};

/// A proposal that can be ordered by (`node_id`, round, attempt)
pub trait Proposal: Clone {
    type NodeId: Copy + Ord + fmt::Debug + Hash + Send + Sync;
    type RoundId: Copy + Ord + Default + fmt::Debug + Hash + Send + Sync;
    type AttemptId: Copy + Ord + Default + fmt::Debug + Hash + Send + Sync;

    fn node_id(&self) -> Self::NodeId;
    fn round(&self) -> Self::RoundId;
    fn attempt(&self) -> Self::AttemptId;

    /// Return the next attempt ID after the given one.
    /// Used when a proposal is rejected to retry with a higher attempt.
    fn next_attempt(attempt: Self::AttemptId) -> Self::AttemptId;

    /// Get the ordering key for this proposal (round, attempt, `node_id`).
    /// Used for comparing proposals in the Paxos protocol.
    fn key(&self) -> ProposalKey<Self> {
        ProposalKey::new(self.round(), self.attempt(), self.node_id())
    }
}

/// Ordering key for proposals - compares by (round, attempt, `node_id`).
/// The `node_id` ensures proposals from different proposers are always unique.
#[derive(Debug)]
pub struct ProposalKey<P: Proposal>(
    pub(crate) P::RoundId,
    pub(crate) P::AttemptId,
    pub(crate) P::NodeId,
);

impl<P: Proposal> Copy for ProposalKey<P> {}

#[expect(clippy::expl_impl_clone_on_copy)]
impl<P: Proposal> Clone for ProposalKey<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: Proposal> Hash for ProposalKey<P> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let ProposalKey(round, attempt, node_id) = self;
        round.hash(state);
        attempt.hash(state);
        node_id.hash(state);
    }
}

impl<P: Proposal> Eq for ProposalKey<P> {}

impl<P: Proposal> PartialEq for ProposalKey<P> {
    fn eq(&self, other: &Self) -> bool {
        let ProposalKey(round1, attempt1, node1) = self;
        let ProposalKey(round2, attempt2, node2) = other;
        round1 == round2 && attempt1 == attempt2 && node1 == node2
    }
}

impl<P: Proposal> PartialOrd for ProposalKey<P> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<P: Proposal> Ord for ProposalKey<P> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ProposalKey(round1, attempt1, node1) = self;
        let ProposalKey(round2, attempt2, node2) = other;
        (round1.cmp(round2))
            .then(attempt1.cmp(attempt2))
            .then(node1.cmp(node2))
    }
}

impl<P: Proposal> ProposalKey<P> {
    /// Create a new proposal key.
    #[must_use]
    pub fn new(round: P::RoundId, attempt: P::AttemptId, node_id: P::NodeId) -> Self {
        Self(round, attempt, node_id)
    }

    /// Get the round ID from this key.
    #[must_use]
    pub fn round(&self) -> P::RoundId {
        self.0
    }

    /// Get the attempt ID from this key.
    #[must_use]
    pub fn attempt(&self) -> P::AttemptId {
        self.1
    }

    /// Get the node ID from this key.
    #[must_use]
    pub fn node_id(&self) -> P::NodeId {
        self.2
    }
}

/// State machine that learns from consensus
#[expect(async_fn_in_trait)]
pub trait Learner: Send + Sync + 'static {
    type Proposal: Proposal + fmt::Debug + Send + Sync + 'static;
    type Message: Clone + fmt::Debug + Send + Sync + 'static;
    type Error: core::error::Error + Send;

    /// This node's unique identifier
    fn node_id(&self) -> <Self::Proposal as Proposal>::NodeId;

    /// Current round (next to be learned)
    fn current_round(&self) -> <Self::Proposal as Proposal>::RoundId;

    /// Validate a proposal (signatures, authorization, etc.)
    fn validate(&self, proposal: &Self::Proposal) -> bool;

    /// Current acceptor set based on learned state
    fn acceptors(&self) -> impl IntoIterator<Item = <Self::Proposal as Proposal>::NodeId>;

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
///
/// # Persistence Safety
///
/// For crash recovery, implementations should:
/// 1. Persist state BEFORE returning success from `promise()`/`accept()`
/// 2. Use `fsync`/`sync_all` to ensure durability
/// 3. On restart, reload state before accepting new requests
///
/// # Concurrency Safety
///
/// Both `promise()` and `accept()` must be atomic with respect to the
/// round's state. Use appropriate locking to prevent race conditions.
pub trait AcceptorStateStore<L: Learner> {
    /// Combined subscription stream (historical + live broadcasts).
    type Subscription: futures::Stream<Item = (L::Proposal, L::Message)> + Unpin;

    /// Get the state for a specific round.
    fn get(&self, round: <L::Proposal as Proposal>::RoundId) -> crate::RoundState<L>;

    /// Try to promise this proposal for a round.
    ///
    /// # Safety Requirements
    ///
    /// MUST reject if ANY of the following are true:
    /// - A higher proposal was already promised for this round
    /// - A higher proposal was already accepted for this round
    ///
    /// The second check prevents promising to a proposal that would be
    /// superseded by an already-accepted value, maintaining consistency.
    ///
    /// # Errors
    /// Returns `Err(current_state)` if the proposal is dominated.
    fn promise(&self, proposal: &L::Proposal) -> Result<(), crate::RoundState<L>>;

    /// Try to accept this proposal + message for a round.
    /// On success, broadcasts to all subscribed learners.
    ///
    /// # Safety Requirements
    ///
    /// MUST reject if ANY of the following are true:
    /// - A higher proposal was already promised for this round
    /// - A higher proposal was already accepted for this round
    ///
    /// The second check is critical: without it, concurrent proposers can
    /// cause different acceptors to have different "winning" values for
    /// the same round, violating consensus.
    ///
    /// # Errors
    /// Returns `Err(current_state)` if the proposal is dominated.
    fn accept(
        &self,
        proposal: &L::Proposal,
        message: &L::Message,
    ) -> Result<(), crate::RoundState<L>>;

    /// Subscribe to accepted (proposal, message) pairs from a given round onwards.
    ///
    /// Returns a stream that yields:
    /// 1. Historical values first (rounds >= `from_round` that were already accepted)
    /// 2. Live broadcasts (new accepts as they happen)
    fn subscribe_from(&self, from_round: <L::Proposal as Proposal>::RoundId) -> Self::Subscription;

    /// Get the highest round that has been accepted (for sync complete message).
    fn highest_accepted_round(&self) -> Option<<L::Proposal as Proposal>::RoundId>;
}

/// Connects to acceptors by node ID.
///
/// Implementations should handle backoff/retry logic internally when connections fail.
/// The connector is cloned per-address, so `&mut self` can be used to track retry state.
pub trait Connector<L: Learner>: Clone + Send + 'static {
    type Connection: AcceptorConn<L> + Send;
    type Error: core::error::Error;
    type ConnectFuture: Future<Output = Result<Self::Connection, Self::Error>> + Send;

    fn connect(&mut self, node_id: &<L::Proposal as Proposal>::NodeId) -> Self::ConnectFuture;
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
