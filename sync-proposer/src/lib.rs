//! Universal Sync Proposer - client/device-side group membership
//!
//! This crate provides the proposer (client/device) implementation for
//! Universal Sync, including:
//!
//! - [`Group`] - High-level API for synchronized MLS groups
//! - [`GroupLearner`] - MLS group member that participates in Paxos
//! - [`IrohConnector`] - P2P QUIC connections to acceptors
//! - High-level flows for creating and joining groups

#![warn(clippy::pedantic)]

pub mod connector;
pub mod error;
pub mod flows;
pub mod group;
pub mod learner;
pub mod repl;
pub mod store;

pub use connector::{
    ConnectorError, IrohConnection, IrohConnector, PAXOS_ALPN, register_group,
    register_group_with_addr,
};
pub use error::GroupError;
#[allow(deprecated)]
pub use flows::{
    CreatedGroup, FlowError, JoinedGroup, acceptors_extension, create_group, join_group,
};
pub use group::{Group, GroupContext, GroupEvent};
pub use learner::{GroupLearner, LearnerError};
// Re-export core types for convenience
pub use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    AcceptorAdd, AcceptorId, AcceptorRemove, AcceptorsExt, Attempt, Epoch, GroupId, GroupMessage,
    GroupProposal, Handshake, HandshakeResponse, MemberId, UnsignedProposal,
};
