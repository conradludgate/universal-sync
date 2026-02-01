//! Paxos protocol messages

use crate::{Learner, RoundState};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Messages from proposer/learner to acceptor
///
/// Both proposers and learners use `Prepare` to initiate a connection.
/// The acceptor responds with historical values and then streams live updates.
/// Learners can send a "dummy" prepare with default `round`/`attempt`/`node_id`.
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
    /// Phase 1: Prepare request - sends signed proposal (can be validated)
    /// Also triggers sync: acceptor sends all accepted values from this round onwards.
    Prepare(L::Proposal),
    /// Phase 2: Accept request - sends signed proposal + message value
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

/// Messages from acceptor to proposer/learner
///
/// For proposers: both `promised` and `accepted` are relevant.
/// For learners: only `accepted` is used (learners just track accepted values).
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(bound(
        serialize = "L::Proposal: Serialize, L::Message: Serialize",
        deserialize = "L::Proposal: Deserialize<'de>, L::Message: Deserialize<'de>"
    ))
)]
pub struct AcceptorMessage<L: Learner> {
    /// Highest proposal this acceptor has promised (proposers only)
    pub promised: Option<L::Proposal>,
    /// Highest accepted proposal + message
    pub accepted: Option<(L::Proposal, L::Message)>,
}

impl<L: Learner> Clone for AcceptorMessage<L> {
    fn clone(&self) -> Self {
        Self {
            promised: self.promised.clone(),
            accepted: self.accepted.clone(),
        }
    }
}

impl<L: Learner> From<RoundState<L>> for AcceptorMessage<L> {
    fn from(state: RoundState<L>) -> Self {
        Self {
            promised: state.promised,
            accepted: state.accepted,
        }
    }
}
