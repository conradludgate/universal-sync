//! CRDT (Conflict-free Replicated Data Type) support
//!
//! This module provides a trait for integrating CRDTs with MLS groups.
//! Each group can register a CRDT type, and the CRDT state is synchronized
//! via the consensus log and snapshots in welcome messages.
//!
//! # Architecture
//!
//! - **Operations**: When a group member performs an action, they encode it as
//!   a CRDT operation and include it in a commit's authenticated data or as
//!   a separate application message.
//!
//! - **Snapshots**: When a new member joins, the current CRDT state is
//!   serialized and encrypted in the welcome bundle, allowing the new member
//!   to bootstrap their state.
//!
//! - **Merging**: CRDTs support merging remote state, which handles the case
//!   where a member receives out-of-order operations or recovers from a snapshot.
//!
//! # Available Implementations
//!
//! - [`NoCrdt`] - A no-op implementation for groups without CRDT support
//! - `YrsCrdt` - Yjs/Yrs document CRDT (in `universal-sync-testing` crate)

use std::fmt;

/// Error type for CRDT operations
#[derive(Debug)]
pub struct CrdtError {
    message: String,
}

impl CrdtError {
    /// Create a new CRDT error with the given message
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CrdtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CRDT error: {}", self.message)
    }
}

impl std::error::Error for CrdtError {}

/// A conflict-free replicated data type that can be synchronized across group members.
///
/// This trait is designed to be dyn-compatible, allowing different CRDT implementations
/// to be used with the same group infrastructure.
///
/// # Implementors
///
/// Implementations should be deterministic - applying the same operations in any order
/// should result in the same final state (this is the CRDT convergence property).
pub trait Crdt: Send + Sync {
    /// Unique identifier for this CRDT type.
    ///
    /// This is used to ensure all group members use compatible CRDT implementations.
    /// Examples: "yjs", "automerge", "counter", "lww-register", "or-set"
    fn type_id(&self) -> &str;

    /// Apply an operation to the CRDT state.
    ///
    /// Called when a commit containing CRDT operations is applied after Paxos consensus.
    /// The operation format is CRDT-implementation-specific.
    ///
    /// # Errors
    /// Returns an error if the operation is malformed or cannot be applied.
    fn apply(&mut self, operation: &[u8]) -> Result<(), CrdtError>;

    /// Merge a remote state snapshot into the local state.
    ///
    /// Used when:
    /// - Joining a group (merging the snapshot from the welcome message)
    /// - Recovering from a checkpoint
    ///
    /// # Errors
    /// Returns an error if the snapshot is malformed or incompatible.
    fn merge(&mut self, snapshot: &[u8]) -> Result<(), CrdtError>;

    /// Serialize the current state to a snapshot.
    ///
    /// The snapshot should contain all information needed to reconstruct
    /// the current CRDT state. It will be encrypted and sent to new members.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    fn snapshot(&self) -> Result<Vec<u8>, CrdtError>;
}

/// Factory for creating CRDT instances.
///
/// Registered with a group to create CRDT instances for new groups or
/// when joining existing groups.
#[allow(clippy::wrong_self_convention)]
pub trait CrdtFactory: Send + Sync {
    /// The CRDT type ID this factory creates.
    ///
    /// Must match the `type_id()` of created instances.
    fn type_id(&self) -> &str;

    /// Create a new empty CRDT instance.
    ///
    /// Called when creating a new group.
    fn create(&self) -> Box<dyn Crdt>;

    /// Create a CRDT instance from a snapshot.
    ///
    /// Called when joining an existing group with the snapshot
    /// received in the welcome message.
    ///
    /// # Errors
    /// Returns an error if the snapshot is malformed or incompatible.
    fn from_snapshot(&self, snapshot: &[u8]) -> Result<Box<dyn Crdt>, CrdtError>;
}

/// A no-op CRDT implementation for groups that don't need CRDT support.
///
/// This can be used as a placeholder when creating groups without CRDT functionality.
#[derive(Debug, Default, Clone)]
pub struct NoCrdt;

impl Crdt for NoCrdt {
    fn type_id(&self) -> &'static str {
        "none"
    }

    fn apply(&mut self, _operation: &[u8]) -> Result<(), CrdtError> {
        // No-op: we don't store any state
        Ok(())
    }

    fn merge(&mut self, _snapshot: &[u8]) -> Result<(), CrdtError> {
        // No-op: nothing to merge
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, CrdtError> {
        // Empty snapshot
        Ok(Vec::new())
    }
}

/// Factory for creating [`NoCrdt`] instances.
#[derive(Debug, Default, Clone)]
pub struct NoCrdtFactory;

impl CrdtFactory for NoCrdtFactory {
    fn type_id(&self) -> &'static str {
        "none"
    }

    fn create(&self) -> Box<dyn Crdt> {
        Box::new(NoCrdt)
    }

    fn from_snapshot(&self, _snapshot: &[u8]) -> Result<Box<dyn Crdt>, CrdtError> {
        Ok(Box::new(NoCrdt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_crdt() {
        let mut crdt = NoCrdt;

        assert_eq!(crdt.type_id(), "none");
        assert!(crdt.apply(b"anything").is_ok());
        assert!(crdt.merge(b"anything").is_ok());

        let snapshot = crdt.snapshot().unwrap();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_no_crdt_factory() {
        let factory = NoCrdtFactory;

        assert_eq!(factory.type_id(), "none");

        let crdt = factory.create();
        assert_eq!(crdt.type_id(), "none");

        let crdt2 = factory.from_snapshot(b"ignored").unwrap();
        assert_eq!(crdt2.type_id(), "none");
    }
}
