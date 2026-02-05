//! Sync Editor - Federated E2EE collaborative text editor
//!
//! This crate provides a collaborative text editor built on:
//! - **yrs** (Yjs in Rust) for CRDT-based text synchronization
//! - **universal-sync** for federated E2EE group messaging
//! - **MLS** for end-to-end encryption with forward secrecy
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SyncedDocument                           │
//! │  ┌─────────────┐    ┌─────────────────────────────────────┐│
//! │  │ yrs::Doc    │◄──►│ Group (MLS + Paxos + iroh)          ││
//! │  │ (text CRDT) │    │ - Encrypts updates                  ││
//! │  └─────────────┘    │ - Consensus for membership changes  ││
//! │                     │ - P2P transport                     ││
//! │                     └───────────────┬─────────────────────┘│
//! └─────────────────────────────────────┼───────────────────────┘
//!                                       │
//!                         ┌─────────────▼─────────────┐
//!                         │  Federated Acceptors      │
//!                         │  (untrusted, E2EE relay)  │
//!                         └───────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use sync_editor::{EditorClient, SyncedDocument};
//!
//! // Create a client
//! let mut client = EditorClient::new("alice", endpoint).await?;
//!
//! // Create a new document with an acceptor
//! let mut doc = client.create_document(&[acceptor_addr]).await?;
//!
//! // Edit the document
//! doc.insert(0, "Hello, world!");
//!
//! // Get current text
//! let text = doc.text();
//! assert_eq!(text, "Hello, world!");
//! ```

#![warn(clippy::pedantic)]

mod app_state;
mod commands;
mod document;
mod editor_client;

pub use app_state::{AppState, SharedAppState, shared_state};
pub use commands::{DeltaCommand, DocumentInfo, parse_group_id};
pub use document::{DocumentError, DocumentUpdateEvent, SyncHandle, SyncedDocument, TextDelta};
pub use editor_client::{create_editor_client, document_from_group, EditorClient};

#[cfg(feature = "tauri")]
pub use commands::tauri_commands;
