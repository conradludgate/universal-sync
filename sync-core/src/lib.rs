//! Universal Sync Core - shared types for proposers and acceptors
//!
//! This crate provides the common types used by both proposer (client/device)
//! and acceptor (server/federation) implementations.

#![warn(clippy::pedantic)]

pub mod extension;
pub mod handshake;
pub mod message;
pub mod proposal;

pub use extension::{
    AcceptorAdd, AcceptorRemove, AcceptorsExt, ACCEPTORS_EXTENSION_TYPE,
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE,
};
pub use handshake::{GroupId, Handshake, HandshakeResponse};
pub use message::GroupMessage;
pub use proposal::{AcceptorId, Attempt, Epoch, GroupProposal, MemberId, UnsignedProposal};
pub mod util;

pub use util::{load_secret_key, KeyLoadError};
