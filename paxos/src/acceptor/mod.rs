//! Acceptor runtime — handles protocol messages and state persistence.

mod handler;
mod runner;
mod state;

pub use handler::AcceptorHandler;
pub use runner::run_acceptor_with_epoch_waiter;
pub use state::RoundState;
