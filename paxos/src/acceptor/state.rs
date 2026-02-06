//! Shared acceptor state.

use crate::traits::Learner;

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
