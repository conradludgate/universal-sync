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
pub(crate) mod learner;
pub(crate) mod rendezvous;

/// REPL module for the CLI binary. Not part of the public API.
#[doc(hidden)]
pub mod repl;

pub use client::GroupClient;
pub use connection::ConnectionManager;
pub use connector::{ConnectorError, IrohConnection, IrohConnector};
pub use error::GroupError;
pub use group::{Group, GroupContext, GroupEvent};
pub use learner::{GroupLearner, LearnerError};
pub use repl::ReplContext;
