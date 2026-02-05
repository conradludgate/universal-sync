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

pub(crate) mod acceptor;
pub(crate) mod connector;
pub(crate) mod epoch_roster;
pub(crate) mod learner;
pub(crate) mod registry;
pub(crate) mod server;
pub(crate) mod state_store;

pub(crate) use acceptor::AcceptorChangeEvent;
pub use acceptor::{AcceptorError, GroupAcceptor};
pub use connector::ConnectorError;
pub(crate) use connector::{ProposalRequest, ProposalResponse};
pub(crate) use epoch_roster::EpochRoster;
pub(crate) use learner::{
    GroupLearningActor, LearningCommand, LearningEvent, PeerEvent, learning_channels,
};
pub use registry::AcceptorRegistry;
pub use server::{GroupRegistry, IrohAcceptorConnection, accept_connection};
pub(crate) use state_store::FjallStateStore;
pub use state_store::GroupStateStore;
// Re-export core types for convenience
pub use state_store::SharedFjallStateStore;
pub(crate) use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    AcceptorAdd, AcceptorId, AcceptorRemove, AcceptorsExt, Attempt, Epoch, GroupId, GroupMessage,
    GroupProposal, Handshake, HandshakeResponse, MemberId, UnsignedProposal,
};
