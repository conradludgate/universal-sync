//! Universal Sync Proposer - client/device-side group membership
//!
//! This crate provides the proposer (client/device) implementation for
//! Universal Sync, including:
//!
//! - [`GroupClient`] - High-level client abstraction for creating/joining groups
//! - [`Group`] - High-level API for synchronized MLS groups
//! - [`GroupLearner`] - Lower-level MLS group member (for advanced use)
//! - [`IrohConnector`] - P2P QUIC connections to acceptors

#![warn(clippy::pedantic)]

pub(crate) mod client;
pub(crate) mod connection;
pub(crate) mod connector;
pub(crate) mod error;
pub(crate) mod flows;
pub(crate) mod group;
pub(crate) mod group_state;
pub(crate) mod learner;
pub(crate) mod rendezvous;

/// REPL module for the CLI binary. Not part of the public API.
#[doc(hidden)]
pub mod repl;

// pub(crate) use client::GroupClient;
// Re-export core types for convenience
// pub(crate) use universal_sync_core::{
//     ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
//     AcceptorAdd, AcceptorId, AcceptorRemove, AcceptorsExt, Attempt, Epoch, GroupId, GroupMessage,
//     GroupProposal, Handshake, HandshakeResponse, MEMBER_ADDR_EXTENSION_TYPE, MemberAddrExt,
//     MemberId, UnsignedProposal,
// };
pub use client::GroupClient;
pub use connection::ConnectionManager;
pub use connector::{ConnectorError, IrohConnection, IrohConnector};
pub(crate) use connector::{register_group, register_group_with_addr};
pub use error::GroupError;
pub(crate) use flows::acceptors_extension;
pub use group::{Group, GroupContext, GroupEvent};
pub(crate) use group::{ReceivedAppMessage, wait_for_welcome};
pub(crate) use group_state::{FjallGroupStateStorage, GroupStateError};
pub use learner::{GroupLearner, LearnerError};
//  LearnerError, MemberAddrExt,
// acceptors_extension, register_group, register_group_with_addr, wait_for_welcome,
