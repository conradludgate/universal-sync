//! Pure state machine core for Paxos — no I/O, no async.
//! Shared between the async runtime and the Stateright model checker.

pub(crate) mod acceptor;
pub(crate) mod proposer;
pub(crate) mod types;

pub use acceptor::{AcceptorCore, decision};
pub use proposer::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};
pub use types::{AcceptorRequest, AcceptorResponse, ProposalKey};

#[cfg(test)]
mod stateright_tests;
