//! Client/device-side group membership for Universal Sync.
//!
//! Key types: [`GroupClient`], [`Group`], [`GroupLearner`], [`IrohConnector`].

#![warn(clippy::pedantic)]

pub(crate) mod client;
pub(crate) mod connection;
pub(crate) mod connector;
pub(crate) mod error;
pub(crate) mod flows;
pub(crate) mod group;
pub(crate) mod learner;
pub(crate) mod rendezvous;

#[doc(hidden)]
pub mod repl;

pub use client::GroupClient;
pub use connection::ConnectionManager;
pub use connector::{ConnectorError, IrohConnection, IrohConnector};
pub use error::GroupError;
pub use group::{Group, GroupContext, GroupEvent};
pub use learner::{GroupLearner, LearnerError};
pub use repl::ReplContext;
