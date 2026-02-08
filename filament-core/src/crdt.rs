//! CRDT integration for MLS groups.
//!
//! Updates are sent as application messages. New members bootstrap via
//! backfill from acceptors (including compaction entries).

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
///
/// Uses a two-phase flush/confirm protocol: [`Crdt::flush`] encodes a diff
/// but does not advance internal state until [`Crdt::confirm_flush`] is called
/// after the send succeeds. This provides automatic retry and batching.
pub trait Crdt: Send + Sync + 'static {
    fn protocol_name(&self) -> &str;

    /// # Errors
    ///
    /// Returns [`CrdtError`] if the operation bytes cannot be decoded or applied.
    fn apply(&mut self, operation: &[u8]) -> Result<(), Report<CrdtError>>;

    /// Produce a diff at the given compaction level.
    ///
    /// Level 0 is for incremental L0 updates. Higher levels are for
    /// compaction. Returns `None` if there are no changes at this level
    /// since the last confirmed flush.
    ///
    /// The caller must call [`Crdt::confirm_flush`] after the send succeeds.
    /// If the send fails, calling `flush` again will re-encode from the
    /// last confirmed state, automatically including any new edits.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError`] if encoding the diff fails.
    fn flush(&mut self, level: usize) -> Result<Option<Vec<u8>>, Report<CrdtError>>;

    /// Confirm that the last [`Crdt::flush`] at the given level was
    /// successfully sent. Advances the internal state vector for that level.
    fn confirm_flush(&mut self, level: usize);
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

    fn flush(&mut self, _level: usize) -> Result<Option<Vec<u8>>, Report<CrdtError>> {
        Ok(None)
    }

    fn confirm_flush(&mut self, _level: usize) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_crdt() {
        let mut crdt = NoCrdt;

        assert_eq!(Crdt::protocol_name(&crdt), "none");
        assert!(crdt.apply(b"anything").is_ok());
        assert!(crdt.flush(0).unwrap().is_none());
        crdt.confirm_flush(0);
        assert!(crdt.flush(1).unwrap().is_none());
        crdt.confirm_flush(1);
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
