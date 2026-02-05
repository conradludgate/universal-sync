//! Acceptor runtime implementation
//!
//! This module provides the runtime components for running a Paxos acceptor:
//!
//! - [`AcceptorHandler`]: Handles individual protocol messages
//! - [`SharedAcceptorState`]: Thread-safe shared state for multi-connection acceptors
//! - [`run_acceptor`]: Main acceptor loop that processes connections
//! - [`AcceptorLearner`]: Tracks quorum across acceptors and learns values on consensus
//!
//! # Single vs Multi-Acceptor
//!
//! For single-acceptor deployments, acceptance equals consensus. The
//! [`run_acceptor`] function handles the protocol and state persistence.
//!
//! For multi-acceptor deployments, use [`AcceptorLearner`] to track quorum:
//!
//! ```ignore
//! use paxos::acceptor::{AcceptorLearner, SharedAcceptorState};
//!
//! // Create learner
//! let mut learner: AcceptorLearner<MyLearner, _> = AcceptorLearner::new();
//!
//! // Subscribe to all acceptor state stores
//! let subscriptions = acceptor_states
//!     .iter()
//!     .map(|s| s.subscribe_from(current_round));
//! learner.sync_acceptors(subscriptions);
//!
//! // Learn values in a loop
//! while let Some((proposal, message)) = learner.learn_one(&my_learner).await {
//!     my_learner.apply(proposal, message).await?;
//! }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use paxos::acceptor::{AcceptorHandler, SharedAcceptorState, run_acceptor};
//!
//! let state = SharedAcceptorState::new();
//! let handler = AcceptorHandler::new(my_acceptor, state.clone());
//! run_acceptor(handler, connection, proposer_id).await?;
//! ```

mod handler;
mod runner;
mod state;

pub use handler::AcceptorHandler;
pub(crate) use handler::{AcceptOutcome, InvalidProposal, PromiseOutcome};
pub use runner::{run_acceptor, run_acceptor_with_epoch_waiter};
pub use state::RoundState;
pub(crate) use state::{AcceptorReceiver, AcceptorSubscription, SharedAcceptorState};
