//! Pure state machine core for Paxos - no I/O, no async
//!
//! This module contains the core state transition logic that is shared between:
//! - The async runtime implementation
//! - The Stateright model checker tests
//!
//! By extracting this logic, we ensure the model checker verifies the exact
//! same state transitions as the production code.
//!
//! # Modules
//!
//! - [`types`]: Core type definitions (`ProposalKey`, message types)
//! - [`acceptor`]: Acceptor state machine (`AcceptorCore`)
//! - [`proposer`]: Proposer state machine (`ProposerCore`)
//! - [`quorum`]: Quorum tracking (`QuorumCore`)

pub(crate) mod acceptor;
pub(crate) mod proposer;
pub(crate) mod types;

pub use acceptor::{AcceptorCore, decision};
pub use proposer::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};
pub use types::{AcceptorRequest, AcceptorResponse, ProposalKey};

#[cfg(test)]
mod stateright_tests;
