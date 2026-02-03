//! Shared acceptor state implementation

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use futures::{Stream, StreamExt, stream};
use tokio::sync::broadcast;

use crate::core::{AcceptResult, AcceptorCore, PrepareResult};
use crate::traits::{AcceptorStateStore, Learner, Proposal, ProposalKey};

/// Type alias for the core state machine with Learner-specific types
type CoreState<L> = AcceptorCore<
    <<L as Learner>::Proposal as Proposal>::RoundId,
    ProposalKey<<L as Learner>::Proposal>,
    <L as Learner>::Message,
>;

/// Type alias for the proposal map
type ProposalMap<L> =
    std::collections::BTreeMap<ProposalKey<<L as Learner>::Proposal>, <L as Learner>::Proposal>;

/// Per-round acceptor state.
///
/// Contains the highest promised proposal and the accepted (proposal, message) pair.
pub struct RoundState<L: Learner> {
    /// Highest promised proposal
    pub promised: Option<L::Proposal>,
    /// Accepted (proposal, message) pair
    pub accepted: Option<(L::Proposal, L::Message)>,
}

impl<L: Learner> Clone for RoundState<L> {
    fn clone(&self) -> Self {
        Self {
            promised: self.promised.clone(),
            accepted: self.accepted.clone(),
        }
    }
}

impl<L: Learner> Default for RoundState<L> {
    fn default() -> Self {
        Self {
            promised: None,
            accepted: None,
        }
    }
}

/// Default in-memory shared acceptor state using `Arc<Mutex>`
///
/// Tracks state per-round to support Multi-Paxos.
/// Also broadcasts accepted proposals to subscribed learners.
///
/// Uses [`AcceptorCore`] for the pure state machine logic, adding:
/// - Thread-safe synchronization via `Arc<Mutex>`
/// - Broadcast channel for learner notifications
#[derive(Clone)]
pub struct SharedAcceptorState<L: Learner> {
    /// Pure state machine core - contains promised/accepted maps
    core: Arc<Mutex<CoreState<L>>>,
    /// Broadcast channel for notifying learners of accepted values
    broadcast: broadcast::Sender<(L::Proposal, L::Message)>,
    /// Map from `ProposalKey` back to full Proposal (needed for API compatibility)
    proposals: Arc<Mutex<ProposalMap<L>>>,
}

impl<L: Learner> Default for SharedAcceptorState<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Learner> SharedAcceptorState<L> {
    /// Create a new shared acceptor state with default broadcast capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(16)
    }

    /// Create a new shared acceptor state with specified broadcast capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (broadcast, _) = broadcast::channel(capacity);
        Self {
            core: Arc::new(Mutex::new(AcceptorCore::new())),
            broadcast,
            proposals: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        }
    }

    /// Create with initial accepted state (e.g., loaded from persistence)
    #[must_use]
    pub fn with_accepted(proposal: &L::Proposal, message: &L::Message) -> Self {
        let mut core = AcceptorCore::new();
        let round = proposal.round();
        let key = proposal.key();
        core.promised.insert(round, key);
        core.accepted.insert(round, (key, message.clone()));

        let mut proposals = std::collections::BTreeMap::new();
        proposals.insert(key, proposal.clone());

        let (broadcast, _) = broadcast::channel(16);
        Self {
            core: Arc::new(Mutex::new(core)),
            broadcast,
            proposals: Arc::new(Mutex::new(proposals)),
        }
    }

    /// Store a proposal for later retrieval
    fn store_proposal(&self, proposal: &L::Proposal) {
        let mut proposals = self.proposals.lock().unwrap();
        proposals.insert(proposal.key(), proposal.clone());
    }

    /// Get a stored proposal by key
    fn get_proposal(&self, key: &ProposalKey<L::Proposal>) -> Option<L::Proposal> {
        let proposals = self.proposals.lock().unwrap();
        proposals.get(key).cloned()
    }
}

/// A receiver for accepted proposals, wrapping a broadcast receiver.
pub struct AcceptorReceiver<L: Learner> {
    inner: tokio_stream::wrappers::BroadcastStream<(L::Proposal, L::Message)>,
}

impl<L: Learner> Stream for AcceptorReceiver<L>
where
    (L::Proposal, L::Message): Send + 'static,
{
    type Item = (L::Proposal, L::Message);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.get_mut().inner).poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(Some(item)),
            _ => Poll::Ready(None),
        }
    }
}

/// Stream of historical accepted values for in-memory state.
pub type HistoricalStream<L> =
    stream::Iter<std::vec::IntoIter<(<L as Learner>::Proposal, <L as Learner>::Message)>>;

/// Combined subscription stream (historical + live).
pub type AcceptorSubscription<L> = stream::Chain<HistoricalStream<L>, AcceptorReceiver<L>>;

impl<L: Learner> AcceptorStateStore<L> for SharedAcceptorState<L>
where
    (L::Proposal, L::Message): Send + 'static,
{
    type Subscription = AcceptorSubscription<L>;

    async fn get(&self, round: <L::Proposal as Proposal>::RoundId) -> RoundState<L> {
        let core = self.core.lock().unwrap();
        let (promised_key, accepted) = core.get(round);

        RoundState {
            promised: promised_key.and_then(|k| self.get_proposal(&k)),
            accepted: accepted.and_then(|(k, m)| self.get_proposal(&k).map(|p| (p, m))),
        }
    }

    async fn promise(&self, proposal: &L::Proposal) -> Result<(), RoundState<L>> {
        self.store_proposal(proposal);

        let round = proposal.round();
        let key = proposal.key();

        let res = self.core.lock().unwrap().prepare(round, key);

        match res {
            PrepareResult::Promised { .. } => Ok(()),
            PrepareResult::Rejected { promised, accepted } => Err(RoundState {
                promised: self.get_proposal(&promised),
                accepted: accepted.and_then(|(k, m)| self.get_proposal(&k).map(|p| (p, m))),
            }),
        }
    }

    async fn accept(
        &self,
        proposal: &L::Proposal,
        message: &L::Message,
    ) -> Result<(), RoundState<L>> {
        self.store_proposal(proposal);

        let mut core = self.core.lock().unwrap();
        let round = proposal.round();
        let key = proposal.key();

        match core.accept(round, key, message.clone()) {
            AcceptResult::Accepted { .. } => {
                // Broadcast to learners (ignore errors - no receivers is ok)
                let _ = self.broadcast.send((proposal.clone(), message.clone()));
                Ok(())
            }
            AcceptResult::Rejected => {
                let (promised_key, accepted) = core.get(round);
                drop(core); // Release lock before calling get_proposal
                Err(RoundState {
                    promised: promised_key.and_then(|k| self.get_proposal(&k)),
                    accepted: accepted.and_then(|(k, m)| self.get_proposal(&k).map(|p| (p, m))),
                })
            }
        }
    }

    async fn subscribe_from(
        &self,
        from_round: <L::Proposal as Proposal>::RoundId,
    ) -> Self::Subscription {
        // Collect historical values, skipping from_round (included in promise response)
        let core = self.core.lock().unwrap();
        let historical: Vec<_> = core
            .accepted_from(from_round)
            .into_iter()
            .filter(|(k, _)| k.round() != from_round)
            .filter_map(|(k, m)| self.get_proposal(&k).map(|p| (p, m)))
            .collect();
        drop(core);

        // Create live broadcast receiver
        let live = AcceptorReceiver {
            inner: tokio_stream::wrappers::BroadcastStream::new(self.broadcast.subscribe()),
        };

        // Chain historical (yields first) with live (yields after)
        stream::iter(historical).chain(live)
    }

    async fn highest_accepted_round(&self) -> Option<<L::Proposal as Proposal>::RoundId> {
        let core = self.core.lock().unwrap();
        core.highest_accepted_round()
    }
}
