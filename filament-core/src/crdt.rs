//! CRDT integration for MLS groups.
//!
//! Operations are sent as application messages, snapshots are included in
//! Welcome messages for new members.

use std::fmt;

use error_stack::Report;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct CrdtError;

impl fmt::Display for CrdtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CRDT operation failed")
    }
}

impl std::error::Error for CrdtError {}

/// Trait for CRDTs that can be synchronized across group members.
pub trait Crdt: Send + Sync + 'static {
    fn protocol_name(&self) -> &str;

    /// # Errors
    ///
    /// Returns [`CrdtError`] if the operation bytes cannot be decoded or applied.
    fn apply(&mut self, operation: &[u8]) -> Result<(), Report<CrdtError>>;

    /// # Errors
    ///
    /// Returns [`CrdtError`] if the snapshot bytes cannot be decoded or merged.
    fn merge(&mut self, snapshot: &[u8]) -> Result<(), Report<CrdtError>>;

    /// # Errors
    ///
    /// Returns [`CrdtError`] if encoding the current state fails.
    fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>>;

    /// Returns `None` if there are no changes since the last flush.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError`] if encoding the diff fails.
    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>>;

    /// Snapshot suitable for sending over the wire (may include framing).
    ///
    /// Defaults to [`Crdt::snapshot`]. Implementations that use prefix
    /// framing in [`Crdt::apply`] should override this to wrap the snapshot
    /// so that receivers can decode it via `apply()`.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError`] if encoding the snapshot fails.
    fn wire_snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        self.snapshot()
    }
}

/// Configuration for a single compaction level.
///
/// Index 0 is L0 (raw individual messages), higher indices are progressively
/// more compacted tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionLevel {
    /// Compact into *this* level when the level below accumulates this many entries.
    /// L0 should have `threshold: 0` (entries are created directly, not via compaction).
    pub threshold: u32,
    /// How many acceptors store entries at this level (0 = all).
    pub replication: u8,
}

/// Per-CRDT compaction configuration.
///
/// A `Vec<CompactionLevel>` where index = level number. Must have at least 2
/// levels (L0 for raw messages + L(max) for the full compacted snapshot).
pub type CompactionConfig = Vec<CompactionLevel>;

/// Default 3-level config: L0 → L1 after 50 messages, L1 → L2 after 10.
#[must_use]
pub fn default_compaction_config() -> CompactionConfig {
    vec![
        CompactionLevel {
            threshold: 0,
            replication: 1,
        },
        CompactionLevel {
            threshold: 50,
            replication: 2,
        },
        CompactionLevel {
            threshold: 10,
            replication: 0,
        },
    ]
}

/// No-op CRDT for groups without CRDT support.
#[derive(Debug, Default, Clone)]
pub struct NoCrdt;

impl Crdt for NoCrdt {
    fn protocol_name(&self) -> &'static str {
        "none"
    }

    fn apply(&mut self, _operation: &[u8]) -> Result<(), Report<CrdtError>> {
        Ok(())
    }

    fn merge(&mut self, _snapshot: &[u8]) -> Result<(), Report<CrdtError>> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>> {
        Ok(Vec::new())
    }

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_crdt() {
        let mut crdt = NoCrdt;

        assert_eq!(Crdt::protocol_name(&crdt), "none");
        assert!(crdt.apply(b"anything").is_ok());
        assert!(crdt.merge(b"anything").is_ok());

        let snapshot = crdt.snapshot().unwrap();
        assert!(snapshot.is_empty());

        assert!(crdt.flush_update().unwrap().is_none());
    }

    #[test]
    fn test_wire_snapshot_default() {
        let crdt = NoCrdt;
        let wire = crdt.wire_snapshot().unwrap();
        let snap = crdt.snapshot().unwrap();
        assert_eq!(wire, snap);
    }

    #[test]
    fn test_compaction_config_default() {
        let config = default_compaction_config();
        assert_eq!(config.len(), 3);
        assert_eq!(config[0].threshold, 0); // L0: no threshold
        assert_eq!(config[0].replication, 1);
        assert_eq!(config[1].threshold, 50); // L0 → L1 after 50
        assert_eq!(config[1].replication, 2);
        assert_eq!(config[2].threshold, 10); // L1 → L2 after 10
        assert_eq!(config[2].replication, 0); // L(max) → all acceptors
    }

    #[test]
    fn test_compaction_config_two_level() {
        let config: CompactionConfig = vec![
            CompactionLevel {
                threshold: 0,
                replication: 1,
            },
            CompactionLevel {
                threshold: 5,
                replication: 0,
            },
        ];
        assert_eq!(config.len(), 2);
        assert_eq!(config[1].threshold, 5);
    }

    #[test]
    fn test_compaction_config_roundtrip() {
        let config: CompactionConfig = vec![
            CompactionLevel {
                threshold: 0,
                replication: 1,
            },
            CompactionLevel {
                threshold: 100,
                replication: 2,
            },
            CompactionLevel {
                threshold: 50,
                replication: 3,
            },
            CompactionLevel {
                threshold: 10,
                replication: 0,
            },
        ];
        let bytes = postcard::to_allocvec(&config).unwrap();
        let decoded: CompactionConfig = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, config);
    }
}
