//! Pure state machine core for Paxos — no I/O, no async.
//! Shared between the async runtime and the Stateright model checker.

use std::hash::Hash;

pub(crate) mod acceptor;
pub(crate) mod proposer;

pub use acceptor::{AcceptorCore, decision};
pub use proposer::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProposalKey<R, A, N> {
    pub round: R,
    pub attempt: A,
    pub node: N,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AcceptorRequest<P, M> {
    Prepare(P),
    Accept(P, M),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AcceptorResponse<P, M> {
    pub for_proposal: P,
    pub promised: Option<P>,
    pub accepted: Option<(P, M)>,
}

#[cfg(test)]
mod stateright_tests;
