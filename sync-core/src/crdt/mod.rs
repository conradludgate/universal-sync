//! CRDT integration for MLS groups.
//!
//! Operations are sent as application messages, snapshots are included in
//! Welcome messages for new members.

use std::any::Any;
use std::fmt;

#[derive(Debug)]
pub struct CrdtError {
    message: String,
}

impl CrdtError {
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

/// Dyn-compatible CRDT that can be synchronized across group members.
pub trait Crdt: Send + Sync + 'static {
    fn type_id(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn apply(&mut self, operation: &[u8]) -> Result<(), CrdtError>;
    fn merge(&mut self, snapshot: &[u8]) -> Result<(), CrdtError>;
    fn snapshot(&self) -> Result<Vec<u8>, CrdtError>;
    /// Returns `None` if there are no changes since the last flush.
    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, CrdtError>;
}

/// Factory for creating CRDT instances (registered per group).
#[allow(clippy::wrong_self_convention)]
pub trait CrdtFactory: Send + Sync {
    fn type_id(&self) -> &str;
    fn create(&self) -> Box<dyn Crdt>;
    fn from_snapshot(&self, snapshot: &[u8]) -> Result<Box<dyn Crdt>, CrdtError>;
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

    fn apply(&mut self, _operation: &[u8]) -> Result<(), CrdtError> {
        Ok(())
    }

    fn merge(&mut self, _snapshot: &[u8]) -> Result<(), CrdtError> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, CrdtError> {
        Ok(Vec::new())
    }

    fn flush_update(&mut self) -> Result<Option<Vec<u8>>, CrdtError> {
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
}
