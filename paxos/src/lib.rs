use std::{
    collections::{BTreeMap, btree_map},
    future::poll_fn,
    task::Poll,
};

use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};

#[expect(async_fn_in_trait)]
pub trait Learner {
    type RoundId: Copy + Ord + 'static;
    type AttemptId: Copy + Ord + 'static;

    type Message: Clone + Ord + 'static;
    type Error: core::error::Error;

    /// Return the next round ID.
    fn next_round(&self) -> Self::RoundId;

    /// Apply the message to the state
    async fn apply(&mut self, message: Self::Message) -> Result<(), Self::Error>;
}

#[repr(C)]
pub struct VersionId<S: Learner> {
    pub round: S::RoundId,
    pub attempt: S::AttemptId,
}

/// A proposer
#[expect(async_fn_in_trait)]
pub trait Proposer {
    type Error: core::error::Error;
    type State: Learner<Error = Self::Error> + 'static;
    type Acceptor<'a>: AcceptorConn<Self::State>
    where
        Self: 'a;

    /// Return the list of acceptors this proposer will use to form consensus.
    fn acceptors(&mut self) -> impl IntoIterator<Item = Self::Acceptor<'_>>;

    /// Resync the current state with the peers.
    ///
    /// This proposer has fallen behind on the current protocol state and cannot
    /// propose again until it catches back up.
    async fn sync(
        &mut self,
        state: &mut Self::State,
        round: <Self::State as Learner>::RoundId,
    ) -> Result<(), Self::Error>;

    /// Wait for the next proposal to make.
    ///
    /// Must be cancel-safe
    async fn next_proposal(
        state: &mut Self::State,
        min_attempt: Option<<Self::State as Learner>::AttemptId>,
    ) -> Result<
        (
            <Self::State as Learner>::AttemptId,
            <Self::State as Learner>::Message,
        ),
        Self::Error,
    >;
}

pub trait AcceptorConn<State: Learner>:
    Sink<Prepare<State>, Error = State::Error>
    + Sink<Accept<State>, Error = State::Error>
    + Stream<Item = Result<AcceptorMessage<State>, State::Error>>
    + Unpin
{
    type Id: Copy + Ord + 'static;
    const MIN_ID: Self::Id;
    const MAX_ID: Self::Id;

    fn id(&self) -> Self::Id;
}

pub struct Prepare<S: Learner>(pub VersionId<S>);
pub struct Accept<S: Learner>(pub VersionId<S>, pub S::Message);
pub struct AcceptorMessage<S: Learner> {
    pub promised: VersionId<S>,
    pub accepted: VersionId<S>,
    pub msg: S::Message,
}

pub async fn run_proposer<P>(mut p: P, mut state: P::State) -> Result<(), P::Error>
where
    P: Proposer,
{
    let mut p_state = LearningState {
        learned: BTreeMap::new(),
        acceptors: BTreeMap::new(),
    };

    loop {
        let round = state.next_round();
        p.sync(&mut state, round).await?;

        let mut acceptors: Vec<_> = p.acceptors().into_iter().collect();

        p_state.prune(round);
        let should_propose = p_state.should_propose(round);
        let min_attempt = p_state.min_attempt(round);

        tokio::select! {
            res = learn(&mut acceptors, &mut p_state, round) => {
                // todo: sync if learn could not get the next ID.
                state.apply(res?).await?
            }
            res = P::next_proposal(&mut state, min_attempt), if should_propose => {
                let (attempt, msg) = res?;
                let version = VersionId{round, attempt};
                propose(&mut acceptors, &mut p_state, version, msg, &mut state).await?
            }
        }
    }
}

struct LearningState<S: Learner, AcceptorId> {
    learned: BTreeMap<S::RoundId, LearnedState<S>>,
    acceptors: BTreeMap<AcceptorId, AcceptorState<S>>,
}

struct LearnedState<S: Learner> {
    attempt: S::AttemptId,
    accepted: usize,
    msg: S::Message,
}

struct AcceptorState<S: Learner> {
    accepted: Option<S::RoundId>,
}

impl<S: Learner> Default for AcceptorState<S> {
    fn default() -> Self {
        Self { accepted: None }
    }
}

impl<S: Learner, AcceptorId: Ord> LearningState<S, AcceptorId> {
    fn prune(&mut self, round_id: S::RoundId) {
        self.learned.retain(|k, _| *k >= round_id);
    }

    fn should_propose(&self, round_id: S::RoundId) -> bool {
        self.learned
            .last_key_value()
            .is_none_or(|(k, _)| *k <= round_id)
    }

    fn min_attempt(&self, round_id: S::RoundId) -> Option<S::AttemptId> {
        self.learned
            .first_key_value()
            .filter(|(k, _)| **k == round_id)
            .map(|(_, v)| v.attempt)
    }

    fn push(
        &mut self,
        acceptor: AcceptorId,
        VersionId { round, attempt }: VersionId<S>,
        accepted: usize,
        msg: S::Message,
    ) -> btree_map::OccupiedEntry<'_, S::RoundId, LearnedState<S>> {
        self.acceptors.entry(acceptor).or_default().accepted = Some(round);

        let inner = LearnedState {
            attempt,
            accepted,
            msg,
        };
        match self.learned.entry(round) {
            btree_map::Entry::Vacant(entry) => entry.insert_entry(inner),
            btree_map::Entry::Occupied(mut entry) => {
                let val = entry.get_mut();
                if val.attempt < inner.attempt {
                    *val = inner;
                } else if val.attempt == inner.attempt {
                    val.accepted += inner.accepted;
                    val.msg = inner.msg;
                }
                entry
            }
        }
    }

    fn min_round(&self) -> Option<S::RoundId> {
        self.acceptors
            .iter()
            .fold(None, |min, (_, s)| match (min, s.accepted) {
                (None, next) => next,
                (Some(_), None) => min,
                (Some(a), Some(b)) => Some(Ord::min(a, b)),
            })
    }
}

/// Try learn a new value from the acceptors
///
/// Must be cancel-safe
async fn learn<A, S, E>(
    acceptors: &mut [A],
    state: &mut LearningState<S, A::Id>,
    round: S::RoundId,
) -> Result<S::Message, E>
where
    E: core::error::Error,
    S: Learner<Error = E>,
    A: AcceptorConn<S>,
{
    let quorum = acceptors.len().div_ceil(2);

    // cancel safe poll fn
    poll_fn(|cx| {
        for acceptor in &mut *acceptors {
            loop {
                let Poll::Ready(Some(AcceptorMessage {
                    accepted: id, msg, ..
                })) = acceptor.try_poll_next_unpin(cx)?
                else {
                    break;
                };

                if id.round <= round {
                    continue;
                }

                let entry = state.push(acceptor.id(), id, 1, msg);
                if *entry.key() == round && entry.get().accepted >= quorum {
                    return Poll::Ready(Ok(entry.remove().msg));
                }
            }
        }

        Poll::Pending
    })
    .await
}

async fn propose<A, S, E>(
    acceptors: &mut [A],
    p_state: &mut LearningState<S, A::Id>,
    current_id: VersionId<S>,
    proposal: S::Message,
    state: &mut S,
) -> Result<(), E>
where
    E: core::error::Error,
    S: Learner<Error = E>,
    A: AcceptorConn<S>,
{
    let quorum = acceptors.len().div_ceil(2);

    // start sending the proposals
    futures::future::try_join_all(acceptors.iter_mut().map(|a| a.send(Prepare(current_id))))
        .await?;

    let mut accept_stream = futures::stream::iter(&mut *acceptors).flatten_unordered(None);

    let mut ready = 0;

    while ready < quorum
        && let Some(AcceptorMessage {
            promised,
            accepted,
            msg,
        }) = accept_stream.try_next().await?
    {
        if accepted >= current_id {
            let entry = p_state.push(accepted, 1, msg);
            if *entry.key() == current_id.round && entry.get().accepted >= quorum {
                state.apply(entry.remove().msg).await?;
            }

            return Ok(());
        }

        if promised >= current_id {
            // make sure this proposal version is tracked.
            p_state.push(accepted, 0, msg);
            return Ok(());
        }

        ready += 1;
    }

    drop(accept_stream);

    // start sending the accepts
    futures::future::try_join_all(
        acceptors
            .iter_mut()
            .map(|a| a.send(Accept(current_id, proposal.clone()))),
    )
    .await?;

    // make sure this proposal version is tracked.
    p_state.push(current_id, 0, proposal);

    Ok(())
}

impl<S: Learner> Copy for VersionId<S> {}
impl<S: Learner> Clone for VersionId<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: Learner> PartialOrd for VersionId<S> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl<S: Learner> PartialEq for VersionId<S> {
    fn eq(&self, other: &Self) -> bool {
        self.round == other.round && self.attempt == other.attempt
    }
}

impl<S: Learner> Ord for VersionId<S> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        Ord::cmp(&self.round, &other.round).then_with(|| Ord::cmp(&self.attempt, &other.attempt))
    }
}

impl<S: Learner> Eq for VersionId<S> {}
