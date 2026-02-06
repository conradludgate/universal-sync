//! CRDT integration for MLS groups.
//!
//! Operations are sent as application messages, snapshots are included in
//! Welcome messages for new members.

use std::any::Any;
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

/// Dyn-compatible CRDT that can be synchronized across group members.
pub trait Crdt: Send + Sync + 'static {
    fn type_id(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn apply(&mut self, operation: &[u8]) -> Result<(), Report<CrdtError>>;
    fn merge(&mut self, snapshot: &[u8]) -> Result<(), Report<CrdtError>>;
    fn snapshot(&self) -> Result<Vec<u8>, Report<CrdtError>>;
    /// Returns `None` if there are no changes since the last flush.
    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, Report<CrdtError>>;
}

/// Factory for creating CRDT instances (registered per group).
#[allow(clippy::wrong_self_convention)]
pub trait CrdtFactory: Send + Sync {
    fn type_id(&self) -> &str;
    fn create(&self) -> Box<dyn Crdt>;
    fn from_snapshot(&self, snapshot: &[u8]) -> Result<Box<dyn Crdt>, Report<CrdtError>>;

    /// Merge an optional base snapshot with a series of updates into a single snapshot.
    fn compact(
        &self,
        base: Option<&[u8]>,
        updates: &[&[u8]],
    ) -> Result<Vec<u8>, Report<CrdtError>> {
        let mut crdt = match base {
            Some(b) => self.from_snapshot(b)?,
            None => self.create(),
        };
        for update in updates {
            crdt.apply(update)?;
        }
        crdt.snapshot()
    }

    fn compaction_config(&self) -> CompactionConfig {
        CompactionConfig::default()
    }
}

/// Per-CRDT configuration for hierarchical compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Number of compaction levels (minimum 2: L0 individual + L(max) full snapshot).
    pub levels: u8,
    /// Per-level threshold: compact when this many entries accumulate at level N.
    /// Length must be `levels - 1` (no threshold for the top level).
    pub thresholds: Vec<u32>,
    /// Per-level replication factor. 0 means replicate to all acceptors.
    /// Length must be `levels`.
    pub replication: Vec<u8>,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            thresholds: vec![10, 5],
            replication: vec![1, 2, 0],
        }
    }
}

/// No-op CRDT for groups without CRDT support.
#[derive(Debug, Default, Clone)]
pub struct NoCrdt;

impl Crdt for NoCrdt {
    fn type_id(&self) -> &'static str {
        "none"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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

#[derive(Debug, Default, Clone)]
pub struct NoCrdtFactory;

impl CrdtFactory for NoCrdtFactory {
    fn type_id(&self) -> &'static str {
        "none"
    }

    fn create(&self) -> Box<dyn Crdt> {
        Box::new(NoCrdt)
    }

    fn from_snapshot(&self, _snapshot: &[u8]) -> Result<Box<dyn Crdt>, Report<CrdtError>> {
        Ok(Box::new(NoCrdt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_crdt() {
        let mut crdt = NoCrdt;

        assert_eq!(Crdt::type_id(&crdt), "none");
        assert!(crdt.apply(b"anything").is_ok());
        assert!(crdt.merge(b"anything").is_ok());

        let snapshot = crdt.snapshot().unwrap();
        assert!(snapshot.is_empty());

        assert!(crdt.flush_update().unwrap().is_none());
    }

    #[test]
    fn test_no_crdt_factory() {
        let factory = NoCrdtFactory;

        assert_eq!(CrdtFactory::type_id(&factory), "none");

        let crdt = factory.create();
        assert_eq!(Crdt::type_id(&*crdt), "none");

        let crdt2 = factory.from_snapshot(b"ignored").unwrap();
        assert_eq!(Crdt::type_id(&*crdt2), "none");
    }

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.levels, 3);
        assert_eq!(config.thresholds.len(), 2); // levels - 1
        assert_eq!(config.replication.len(), 3); // levels
        assert_eq!(config.replication[2], 0); // L(max) → all acceptors
    }

    #[test]
    fn test_compaction_config_two_level() {
        let config = CompactionConfig {
            levels: 2,
            thresholds: vec![5],
            replication: vec![1, 0],
        };
        assert_eq!(config.thresholds.len(), 1); // levels - 1
        assert_eq!(config.replication.len(), 2);
    }

    #[test]
    fn test_no_crdt_factory_compact() {
        let factory = NoCrdtFactory;
        let result = factory.compact(None, &[b"a", b"b"]).unwrap();
        // NoCrdt compact produces an empty snapshot
        assert!(result.is_empty());
    }

    #[test]
    fn test_compaction_config_roundtrip() {
        let config = CompactionConfig {
            levels: 4,
            thresholds: vec![100, 50, 10],
            replication: vec![1, 2, 3, 0],
        };
        let bytes = postcard::to_allocvec(&config).unwrap();
        let decoded: CompactionConfig = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.levels, 4);
        assert_eq!(decoded.thresholds, vec![100, 50, 10]);
        assert_eq!(decoded.replication, vec![1, 2, 3, 0]);
    }
}
