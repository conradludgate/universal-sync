//! Universal Sync - Group membership over Paxos with MLS
//!
//! This crate provides group membership management using:
//! - MLS (Messaging Layer Security) for cryptographic group state
//! - Paxos for consensus on group changes
//!
//! # Architecture
//!
//! - **Devices** are group members with full MLS `Group` state
//! - **Acceptors** are federated servers with `ExternalGroup` state (can verify, can't decrypt)

pub mod acceptor;
pub mod extension;
pub mod learner;
pub mod message;
pub mod proposal;
pub mod state_store;

pub use acceptor::GroupAcceptor;
pub use extension::{
    AcceptorAdd, AcceptorRemove, ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE,
};
pub use learner::GroupLearner;
pub use message::GroupMessage;
pub use proposal::{AcceptorId, GroupProposal, MemberId};
pub use state_store::SharedFjallStateStore;
