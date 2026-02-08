//! Server/federation-side acceptor for Universal Sync groups.

#![warn(clippy::pedantic)]

pub(crate) mod acceptor;
pub mod api;
pub(crate) mod learner;
pub mod metrics;
pub(crate) mod registry;
pub(crate) mod server;
pub(crate) mod state_store;

pub use acceptor::{AcceptorError, GroupAcceptor};
pub use metrics::{AcceptorMetrics, MetricsEncoder, SharedMetrics};
pub use registry::AcceptorRegistry;
pub use server::{IrohAcceptorConnection, accept_connection};
pub use state_store::{GroupStateStore, GroupStorageSizes, SharedFjallStateStore};
