//! Multi-Paxos consensus library
//!
//! This library provides an implementation of Multi-Paxos for building
//! distributed consensus systems with append-only log semantics.
//!
//! # Architecture
//!
//! - **Proposers**: Drive consensus by proposing values (also learn)
//! - **Acceptors**: Respond to proposals and persist accepted values
//! - **Learners**: Proposers that only call [`proposer::Proposer::learn_one`],
//!   never [`proposer::Proposer::propose`]
//!
//! # Quick Start
//!
//! ```ignore
//! use paxos::acceptor::{AcceptorHandler, SharedAcceptorState, run_acceptor};
//! use paxos::proposer::Proposer;
//! use paxos::config::ProposerConfig;
//!
//! // Acceptor side
//! let state = SharedAcceptorState::new();
//! let handler = AcceptorHandler::new(my_acceptor, state.clone());
//! run_acceptor(handler, connection, proposer_id).await?;
//!
//! // Proposer side
//! let proposer = Proposer::new(node_id, connector, ProposerConfig::default());
//! proposer.sync_actors(acceptor_ids);
//! let (proposal, message) = proposer.propose(&learner, my_message).await?;
//! learner.apply(proposal, message).await?;
//! ```

#![warn(clippy::pedantic)]

// Submodules
pub mod acceptor;
pub mod config;
mod connection;
pub mod core;
mod fuse;
mod messages;
pub mod proposer;
mod traits;

pub use messages::{AcceptorMessage, AcceptorRequest};
pub use traits::{
    Acceptor, AcceptorConn, AcceptorStateStore, Connector, Learner, Proposal, ProposalKey,
    Validated, ValidationError,
};
