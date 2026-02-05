//! Universal Sync Core - shared types for proposers and acceptors
//!
//! This crate provides the common types used by both proposer (client/device)
//! and acceptor (server/federation) implementations.

#![warn(clippy::pedantic)]

pub mod codec;
pub mod crdt;
pub mod extension;
pub mod handshake;
pub mod message;
pub mod proposal;
pub mod sink_stream;
pub mod util;
pub mod welcome;

pub use crdt::{Crdt, CrdtError, CrdtFactory, NoCrdt, NoCrdtFactory};
pub use extension::{
    AcceptorAdd, AcceptorRemove, AcceptorsExt, CrdtRegistrationExt, MemberAddrExt,
    SupportedCrdtsExt, ACCEPTORS_EXTENSION_TYPE, ACCEPTOR_ADD_EXTENSION_TYPE,
    ACCEPTOR_REMOVE_EXTENSION_TYPE, CRDT_REGISTRATION_EXTENSION_TYPE, MEMBER_ADDR_EXTENSION_TYPE,
    SUPPORTED_CRDTS_EXTENSION_TYPE,
};
pub use handshake::{GroupId, Handshake, HandshakeResponse, StreamType};
pub use message::{EncryptedAppMessage, GroupMessage, MessageId, MessageRequest, MessageResponse};
pub use proposal::{AcceptorId, Attempt, Epoch, GroupProposal, MemberId, UnsignedProposal};
pub use util::{load_secret_key, KeyLoadError};
pub use welcome::{WelcomeBundle, WelcomeError};
