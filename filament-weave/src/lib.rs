//! Device/client library for the filament sync engine.
//!
//! `filament-weave` manages group membership, consensus, and encrypted CRDT
//! synchronisation on behalf of a single device. All network I/O goes through
//! [iroh](https://iroh.computer) QUIC connections to federated spool servers.
//!
//! # Architecture
//!
//! Each [`Weaver`] spawns a background actor that maintains connections to
//! every spool, drives consensus rounds, and decrypts incoming messages.
//! The caller interacts with the weaver through an async handle.
//!
//! # Typical flow
//!
//! 1. Create a [`WeaverClient`] with an identity and iroh `Endpoint`.
//! 2. Call [`WeaverClient::create`] or [`WeaverClient::join`] to obtain a
//!    [`Weaver`] handle.
//! 3. Send local CRDT changes with [`Weaver::send_update`] and receive
//!    remote changes with [`Weaver::sync`] or [`Weaver::wait_for_update`].
//! 4. Subscribe to [`WeaverEvent`]s via [`Weaver::subscribe`] and handle
//!    [`WeaverEvent::CompactionNeeded`] by calling [`Weaver::compact`].
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`WeaverClient`] | Entry point — creates and joins groups |
//! | [`Weaver`] | Handle to a live group with background actors |
//! | [`WeaverEvent`] | Notifications emitted by a weaver |
//! | [`WeaverContext`] | Snapshot of group metadata |

#![warn(clippy::pedantic)]

pub(crate) mod client;
pub(crate) mod connection;
pub(crate) mod connector;
pub(crate) mod erasure;
pub(crate) mod group;
pub(crate) mod group_state;
pub(crate) mod learner;
pub(crate) mod rendezvous;

pub use client::WeaverClient;
pub use group::{JoinInfo, MemberInfo, Weaver, WeaverContext, WeaverError, WeaverEvent};
pub use group_state::FjallGroupStateStorage;
