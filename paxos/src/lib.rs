//! Multi-Paxos consensus library with signed proposals.

#![warn(clippy::pedantic)]

pub mod acceptor;
pub mod core;
mod fuse;
mod messages;
pub mod proposer;
mod traits;

pub use messages::{AcceptorMessage, AcceptorRequest};
pub use proposer::{ProposeResult, Proposer};
pub use traits::{
    AcceptorConn, AcceptorStateStore, Connector, Learner, Proposal, ProposalKey, Validated,
    ValidationError,
};
