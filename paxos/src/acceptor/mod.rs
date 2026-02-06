//! Acceptor runtime — handles protocol messages and state persistence.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::traits::Learner;

mod handler;
mod runner;

pub use handler::AcceptorHandler;
pub use runner::run_acceptor_with_epoch_waiter;

pub struct RoundState<L: Learner> {
    pub promised: Option<L::Proposal>,
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

/// Messages from proposer/learner to acceptor.
///
/// Learners can send a "dummy" prepare with default `round`/`attempt`/`node_id`
/// to initiate sync without proposing.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(bound(
        serialize = "L::Proposal: Serialize, L::Message: Serialize",
        deserialize = "L::Proposal: Deserialize<'de>, L::Message: Deserialize<'de>"
    ))
)]
pub enum AcceptorRequest<L: Learner> {
    Prepare(L::Proposal),
    Accept(L::Proposal, L::Message),
}

impl<L: Learner> Clone for AcceptorRequest<L> {
    fn clone(&self) -> Self {
        match self {
            Self::Prepare(p) => Self::Prepare(p.clone()),
            Self::Accept(p, m) => Self::Accept(p.clone(), m.clone()),
        }
    }
}

/// Messages from acceptor to proposer/learner.
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(bound(
        serialize = "L::Proposal: Serialize, L::Message: Serialize",
        deserialize = "L::Proposal: Deserialize<'de>, L::Message: Deserialize<'de>"
    ))
)]
pub struct AcceptorMessage<L: Learner> {
    pub promised: L::Proposal,
    pub accepted: Option<(L::Proposal, L::Message)>,
}

impl<L: Learner> fmt::Debug for AcceptorMessage<L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let AcceptorMessage { promised, accepted } = self;
        f.debug_struct("AcceptorMessage")
            .field("promised", promised)
            .field("accepted", accepted)
            .finish()
    }
}

impl<L: Learner> Clone for AcceptorMessage<L> {
    fn clone(&self) -> Self {
        Self {
            promised: self.promised.clone(),
            accepted: self.accepted.clone(),
        }
    }
}

impl<L: Learner> AcceptorMessage<L> {
    /// # Panics
    /// Panics if `promised` is None, which should never happen for valid responses.
    #[must_use]
    pub(crate) fn from_round_state(state: RoundState<L>) -> Self {
        Self {
            promised: state
                .promised
                .expect("response must have a promised proposal"),
            accepted: state.accepted,
        }
    }
}
