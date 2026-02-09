//! CRDT integration for MLS groups.
//!
//! Updates are sent as application messages. New members bootstrap via
//! backfill from acceptors (including compaction entries).

use std::fmt;

use error_stack::Report;

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

    /// Reset flush state at the given `level`.
    ///
    /// After reset, the next [`Crdt::flush`] at this level will produce
    /// a full snapshot (diff from empty state) instead of an incremental diff.
    /// Used for force-compaction on member join.
    fn reset_flush(&mut self, level: usize);
}

/// Compaction threshold: compact into the next level after this many entries.
pub const COMPACTION_BASE: u32 = 20;

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

    fn reset_flush(&mut self, _level: usize) {}
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
    fn test_compaction_base() {
        assert_eq!(COMPACTION_BASE, 20);
    }
}
