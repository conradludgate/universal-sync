//! Server/federation-side acceptor for Universal Sync groups.

#![warn(clippy::pedantic)]

pub(crate) mod acceptor;
pub(crate) mod connector;
pub(crate) mod epoch_roster;
pub(crate) mod learner;
pub(crate) mod registry;
pub(crate) mod server;
pub(crate) mod state_store;

pub use acceptor::{AcceptorError, GroupAcceptor};
pub use registry::AcceptorRegistry;
pub use server::{IrohAcceptorConnection, accept_connection};
pub use state_store::{GroupStateStore, SharedFjallStateStore};
