//! Basic Paxos consensus library
//!
//! This library provides an implementation of Multi-Paxos for building
//! distributed consensus systems with append-only log semantics.
//!
//! # Architecture
//!
//! - **Proposers**: Drive consensus by proposing values (also learn)
//! - **Acceptors**: Respond to proposals and persist accepted values
//! - **Learners**: Proposers that only call [`Proposer::learn_one`], never [`Proposer::propose`]

#![warn(clippy::pedantic)]

mod acceptor;
mod config;
mod connection;
pub mod core;
mod fuse;
mod messages;
mod proposer;
mod quorum;
mod state;
mod traits;

pub use acceptor::{
    AcceptError, AcceptOutcome, AcceptorHandler, InvalidProposal, PromiseOutcome, run_acceptor,
};
pub use config::{BackoffConfig, ProposerConfig, Sleep, TokioSleep};
pub use connection::LazyConnection;
pub use messages::{AcceptorMessage, AcceptorRequest};
pub use proposer::{Proposer, run_proposer};
pub use state::{AcceptorReceiver, RoundState, SharedAcceptorState};
pub use traits::{
    Acceptor, AcceptorConn, AcceptorStateStore, Connector, Learner, Proposal, ProposalKey,
};
