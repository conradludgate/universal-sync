//! Core Paxos traits.

use core::fmt;
use core::future::Future;
use core::hash::Hash;

use error_stack::Report;
use futures::{Sink, Stream};

use crate::acceptor::RoundState;
use crate::messages::{AcceptorMessage, AcceptorRequest};

#[derive(Debug)]
pub struct ValidationError;

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("proposal validation failed")
    }
}

impl core::error::Error for ValidationError {}

/// Marker type proving that validation was performed.
/// Cannot be constructed outside of validation functions.
#[derive(Debug, Clone, Copy)]
pub struct Validated(());

impl Validated {
    /// Only call this after actually performing all validation checks.
    #[must_use]
    pub fn assert_valid() -> Self {
        Self(())
    }
}

pub trait Proposal: Clone {
    type NodeId: Copy + Ord + fmt::Debug + Hash + Send + Sync;
    type RoundId: Copy + Ord + Default + fmt::Debug + Hash + Send + Sync;
    type AttemptId: Copy + Ord + Default + fmt::Debug + Hash + Send + Sync;

    fn node_id(&self) -> Self::NodeId;
    fn round(&self) -> Self::RoundId;
    fn attempt(&self) -> Self::AttemptId;
    fn next_attempt(attempt: Self::AttemptId) -> Self::AttemptId;

    fn key(&self) -> ProposalKey<Self> {
        ProposalKey::new(self.round(), self.attempt(), self.node_id())
    }
}

/// Ordering key for proposals — compares by (round, attempt, `node_id`).
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
    #[must_use]
    pub(crate) fn new(round: P::RoundId, attempt: P::AttemptId, node_id: P::NodeId) -> Self {
        Self(round, attempt, node_id)
    }

    #[must_use]
    pub fn attempt(&self) -> P::AttemptId {
        self.1
    }
}

/// State machine that learns from consensus and can create proposals.
///
/// For devices/clients, `propose()` creates a signed proposal with real content.
/// For acceptors, `propose()` creates a sync-only proposal for the learning process.
#[expect(async_fn_in_trait)]
pub trait Learner: Send + Sync + 'static {
    type Proposal: Proposal + fmt::Debug + Send + Sync + 'static;
    type Message: Clone + fmt::Debug + Send + Sync + 'static;
    type Error: fmt::Debug + Send + 'static;
    type AcceptorId: Copy + Ord + fmt::Debug + Hash + Send + Sync;

    fn node_id(&self) -> <Self::Proposal as Proposal>::NodeId;
    fn current_round(&self) -> <Self::Proposal as Proposal>::RoundId;
    fn acceptors(&self) -> impl IntoIterator<Item = Self::AcceptorId, IntoIter: ExactSizeIterator>;
    fn propose(&self, attempt: <Self::Proposal as Proposal>::AttemptId) -> Self::Proposal;
    fn validate(&self, proposal: &Self::Proposal) -> Result<Validated, Report<ValidationError>>;
    async fn apply(
        &mut self,
        proposal: Self::Proposal,
        message: Self::Message,
    ) -> Result<(), Self::Error>;
}

/// Shared state for an acceptor, allowing multiple connections to coordinate.
///
/// All connections to the same acceptor must share this state.
///
/// Implementations MUST persist state before returning success from
/// `promise()`/`accept()` (use fsync) and reload on restart for crash recovery.
/// Both `promise()` and `accept()` must be atomic per-round.
#[expect(async_fn_in_trait)]
pub trait AcceptorStateStore<L: Learner>: Send + Sync {
    type Subscription: futures::Stream<Item = (L::Proposal, L::Message)> + Send;

    async fn get(&self, round: <L::Proposal as Proposal>::RoundId) -> RoundState<L>;

    /// MUST reject if a higher proposal was already promised or accepted for this round.
    async fn promise(&self, proposal: &L::Proposal) -> Result<(), RoundState<L>>;

    /// MUST reject if a higher proposal was already promised or accepted for this round.
    /// On success, broadcasts to all subscribed learners.
    async fn accept(
        &self,
        proposal: &L::Proposal,
        message: &L::Message,
    ) -> Result<(), RoundState<L>>;

    /// Returns historical values (rounds >= `from_round`) then live broadcasts.
    async fn subscribe_from(
        &self,
        from_round: <L::Proposal as Proposal>::RoundId,
    ) -> Self::Subscription;

    async fn highest_accepted_round(&self) -> Option<<L::Proposal as Proposal>::RoundId>;

    /// Returns historical values only (no live subscription).
    async fn get_accepted_from(
        &self,
        from_round: <L::Proposal as Proposal>::RoundId,
    ) -> Vec<(L::Proposal, L::Message)>;
}

/// Connects to acceptors by their ID.
/// Implementations should handle backoff/retry logic internally.
pub trait Connector<L: Learner>: Clone + Send + 'static {
    type Connection: AcceptorConn<L> + Send;
    type Error: core::error::Error;
    type ConnectFuture: Future<Output = Result<Self::Connection, Self::Error>> + Send;

    fn connect(&mut self, acceptor_id: &L::AcceptorId) -> Self::ConnectFuture;
}

pub trait AcceptorConn<L: Learner>:
    Sink<AcceptorRequest<L>, Error = L::Error> + Stream<Item = Result<AcceptorMessage<L>, L::Error>>
{
}

impl<L, T> AcceptorConn<L> for T
where
    L: Learner,
    T: Sink<AcceptorRequest<L>, Error = L::Error>
        + Stream<Item = Result<AcceptorMessage<L>, L::Error>>,
{
}
