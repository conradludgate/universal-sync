//! Multi-Paxos consensus library
//!
//! This library provides an implementation of Multi-Paxos for building
//! distributed consensus systems with append-only log semantics.
//!
//! # Architecture
//!
//! - **Proposers**: Drive consensus by proposing values
//! - **Acceptors**: Respond to proposals and persist accepted values
//! - **Learners**: Track accepted values using [`proposer::QuorumTracker`]
//!
//! # Quick Start
//!
//! ```ignore
//! use paxos::acceptor::{AcceptorHandler, SharedAcceptorState, run_acceptor};
//! use paxos::proposer::{Proposer, ProposeResult};
//!
//! // Acceptor side
//! let state = SharedAcceptorState::new();
//! let handler = AcceptorHandler::new(my_acceptor, state.clone());
//! run_acceptor(handler, connection, proposer_id).await?;
//!
//! // Proposer side (push-based)
//! let mut proposer = Proposer::new();
//! let result = proposer.propose(&learner, attempt, message);
//! // Send messages to acceptors, then call proposer.receive() with responses
//! ```

#![warn(clippy::pedantic)]

// Submodules
pub mod acceptor;
pub mod core;
mod fuse;
mod messages;
pub mod proposer;
mod traits;

pub use messages::{AcceptorMessage, AcceptorRequest};
pub use proposer::{ProposeResult, Proposer, QuorumTracker};
pub use traits::{
    Acceptor, AcceptorConn, AcceptorStateStore, Connector, Learner, Proposal, ProposalKey,
    Validated, ValidationError,
};
