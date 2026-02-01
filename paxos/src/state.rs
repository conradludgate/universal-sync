//! Shared acceptor state implementation

use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

use futures::Stream;
use tokio::sync::broadcast;

use crate::{AcceptorStateStore, Learner, Proposal};

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

/// Inner state type: map from round to round state.
type StateMap<L> = BTreeMap<<<L as Learner>::Proposal as Proposal>::RoundId, RoundState<L>>;

/// Default in-memory shared acceptor state using `Arc<Mutex>`
///
/// Tracks state per-round to support Multi-Paxos.
/// Also broadcasts accepted proposals to subscribed learners.
#[derive(Clone)]
pub struct SharedAcceptorState<L: Learner> {
    inner: Arc<Mutex<StateMap<L>>>,
    broadcast: broadcast::Sender<(L::Proposal, L::Message)>,
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
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            broadcast,
        }
    }

    /// Create with initial accepted state (e.g., loaded from persistence)
    #[must_use]
    pub fn with_accepted(proposal: L::Proposal, message: L::Message) -> Self {
        let mut map = BTreeMap::new();
        let round = proposal.round();
        map.insert(
            round,
            RoundState {
                promised: Some(proposal.clone()),
                accepted: Some((proposal, message)),
            },
        );
        let (broadcast, _) = broadcast::channel(16);
        Self {
            inner: Arc::new(Mutex::new(map)),
            broadcast,
        }
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

impl<L: Learner> AcceptorStateStore<L> for SharedAcceptorState<L>
where
    (L::Proposal, L::Message): Send + 'static,
{
    type Receiver = AcceptorReceiver<L>;

    fn get(&self, round: <L::Proposal as Proposal>::RoundId) -> RoundState<L> {
        let inner = self.inner.lock().unwrap();
        inner.get(&round).cloned().unwrap_or_default()
    }

    fn promise(&self, proposal: &L::Proposal) -> Result<(), RoundState<L>> {
        let mut inner = self.inner.lock().unwrap();
        let round = proposal.round();
        let state = inner.entry(round).or_default();
        let dominated = state
            .promised
            .as_ref()
            .is_some_and(|p| p.key() > proposal.key());
        if dominated {
            return Err(state.clone());
        }

        state.promised = Some(proposal.clone());
        Ok(())
    }

    fn accept(&self, proposal: &L::Proposal, message: &L::Message) -> Result<(), RoundState<L>> {
        let mut inner = self.inner.lock().unwrap();
        let round = proposal.round();
        let state = inner.entry(round).or_default();
        let dominated = state
            .promised
            .as_ref()
            .is_some_and(|p| p.key() > proposal.key());
        if dominated {
            return Err(state.clone());
        }

        state.accepted = Some((proposal.clone(), message.clone()));
        state.promised = Some(proposal.clone());

        // Broadcast to learners (ignore errors - no receivers is ok)
        let _ = self.broadcast.send((proposal.clone(), message.clone()));
        Ok(())
    }

    fn subscribe(&self) -> Self::Receiver {
        AcceptorReceiver {
            inner: tokio_stream::wrappers::BroadcastStream::new(self.broadcast.subscribe()),
        }
    }

    fn accepted_from(
        &self,
        from_round: <L::Proposal as Proposal>::RoundId,
    ) -> Vec<(L::Proposal, L::Message)> {
        let inner = self.inner.lock().unwrap();
        inner
            .range(from_round..)
            .filter_map(|(_, state)| state.accepted.clone())
            .collect()
    }

    fn highest_accepted_round(&self) -> Option<<L::Proposal as Proposal>::RoundId> {
        let inner = self.inner.lock().unwrap();
        inner
            .iter()
            .rev()
            .find_map(|(round, state)| state.accepted.as_ref().map(|_| *round))
    }
}
