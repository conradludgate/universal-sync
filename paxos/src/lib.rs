//! Basic Paxos consensus library
//!
//! This library provides an implementation of Multi-Paxos for building
//! distributed consensus systems with append-only log semantics.
//!
//! # Architecture
//!
//! - **Proposers**: Drive consensus by proposing values
//! - **Acceptors**: Respond to proposals and persist accepted values
//! - **Learners**: Learn accepted values from acceptors (can sync when offline)

#![warn(clippy::pedantic)]

mod acceptor;
mod config;
mod connection;
mod learner;
mod messages;
mod proposer;
mod quorum;
mod state;
mod traits;

pub use acceptor::run_acceptor;
pub use config::{BackoffConfig, ProposerConfig, Sleep, TokioSleep};
pub use connection::LazyConnection;
pub use learner::{LearnerSession, run_learner};
pub use messages::{AcceptorMessage, AcceptorRequest};
pub use proposer::run_proposer;
pub use state::{AcceptorReceiver, RoundState, SharedAcceptorState};
pub use traits::{
    Acceptor, AcceptorConn, AcceptorStateStore, Connector, Learner, Proposal, ProposalKey,
};
