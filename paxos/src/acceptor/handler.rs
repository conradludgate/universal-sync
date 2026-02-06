//! Acceptor handler for processing Paxos protocol messages.

use std::fmt;

use tracing::trace;

use super::{AcceptorMessage, AcceptorStateStore};
use crate::{Learner, Proposal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidProposal;

impl fmt::Display for InvalidProposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid proposal")
    }
}

impl std::error::Error for InvalidProposal {}

pub(crate) enum PromiseOutcome<A: Learner> {
    Promised(AcceptorMessage<A>),
    Outdated(AcceptorMessage<A>),
}

pub(crate) enum AcceptOutcome<A: Learner> {
    Accepted(AcceptorMessage<A>),
    Outdated(AcceptorMessage<A>),
}

pub struct AcceptorHandler<A, S> {
    acceptor: A,
    state: S,
}

impl<A, S> AcceptorHandler<A, S>
where
    A: Learner,
    S: AcceptorStateStore<A>,
{
    pub fn new(acceptor: A, state: S) -> Self {
        Self { acceptor, state }
    }

    pub(crate) fn node_id(&self) -> <A::Proposal as Proposal>::NodeId {
        self.acceptor.node_id()
    }

    pub(crate) fn state(&self) -> &S {
        &self.state
    }

    /// Used by the runner to apply learned values.
    pub(crate) fn acceptor_mut(&mut self) -> &mut A {
        &mut self.acceptor
    }

    pub(crate) async fn handle_prepare(
        &mut self,
        proposal: &A::Proposal,
    ) -> Result<PromiseOutcome<A>, InvalidProposal> {
        if self.acceptor.validate(proposal).is_err() {
            return Err(InvalidProposal);
        }

        match self.state.promise(proposal).await {
            Ok(()) => {
                trace!(round = ?proposal.round(), "promised");
                let state = self.state.get(proposal.round()).await;
                Ok(PromiseOutcome::Promised(AcceptorMessage::from_round_state(
                    state,
                )))
            }
            Err(round_state) => {
                trace!(round = ?proposal.round(), "promise rejected - outdated");
                Ok(PromiseOutcome::Outdated(AcceptorMessage::from_round_state(
                    round_state,
                )))
            }
        }
    }

    /// Value is NOT applied to the learner here — that only happens when
    /// quorum is confirmed via the learning process.
    pub(crate) async fn handle_accept(
        &mut self,
        proposal: &A::Proposal,
        message: &A::Message,
    ) -> Result<AcceptOutcome<A>, InvalidProposal> {
        if self.acceptor.validate(proposal).is_err() {
            return Err(InvalidProposal);
        }

        match self.state.accept(proposal, message).await {
            Ok(()) => {
                trace!(round = ?proposal.round(), "accepted");
                let state = self.state.get(proposal.round()).await;
                Ok(AcceptOutcome::Accepted(AcceptorMessage::from_round_state(
                    state,
                )))
            }
            Err(round_state) => {
                trace!(round = ?proposal.round(), "accept rejected - outdated");
                Ok(AcceptOutcome::Outdated(AcceptorMessage::from_round_state(
                    round_state,
                )))
            }
        }
    }
}
