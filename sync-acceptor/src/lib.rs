//! Universal Sync Acceptor - server/federation-side group membership
//!
//! This crate provides the acceptor (server/federation) implementation for
//! Universal Sync, including:
//!
//! - [`GroupAcceptor`] - External group observer that validates proposals
//! - [`FjallStateStore`] / [`GroupStateStore`] - Persistent state storage
//! - [`AcceptorRegistry`] - Multi-group management
//! - Connection handling via iroh

#![warn(clippy::pedantic)]

pub mod acceptor;
pub mod connector;
pub mod epoch_roster;
pub mod registry;
pub mod server;
pub mod state_store;

pub use acceptor::{AcceptorError, GroupAcceptor};
pub use connector::{ConnectorError, PAXOS_ALPN};
pub use epoch_roster::EpochRoster;
pub use registry::AcceptorRegistry;
pub use server::{GroupRegistry, IrohAcceptorConnection, accept_connection};
pub use state_store::{FjallStateStore, GroupStateStore, SharedFjallStateStore};
// Re-export core types for convenience
pub use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    AcceptorAdd, AcceptorId, AcceptorRemove, AcceptorsExt, Attempt, Epoch, GroupId, GroupMessage,
    GroupProposal, Handshake, HandshakeResponse, MemberId, UnsignedProposal,
};
