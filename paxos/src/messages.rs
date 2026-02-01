//! Paxos protocol messages

use crate::{Learner, Proposal, RoundState};

/// Messages from proposer/learner to acceptor
#[derive(Debug)]
pub enum AcceptorRequest<L: Learner> {
    /// Phase 1: Prepare request - sends signed proposal (can be validated)
    Prepare(L::Proposal),
    /// Phase 2: Accept request - sends signed proposal + message value
    Accept(L::Proposal, L::Message),
    /// Sync request from learner - switches connection to streaming mode
    Sync {
        /// The round to sync from (inclusive)
        from_round: <L::Proposal as Proposal>::RoundId,
    },
}

impl<L: Learner> Clone for AcceptorRequest<L> {
    fn clone(&self) -> Self {
        match self {
            Self::Prepare(p) => Self::Prepare(p.clone()),
            Self::Accept(p, m) => Self::Accept(p.clone(), m.clone()),
            Self::Sync { from_round } => Self::Sync {
                from_round: *from_round,
            },
        }
    }
}

/// Messages from acceptor to proposer/learner
///
/// For proposers: both `promised` and `accepted` are relevant.
/// For learners: only `accepted` is used (learners just track accepted values).
#[derive(Debug)]
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
