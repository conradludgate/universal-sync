//! Error types for sync-proposer
//!
//! All errors in this crate use [`error_stack`] for rich context.

use std::fmt;

/// Error type for group operations.
///
/// This is a simple marker type - use [`error_stack::Report`] methods
/// to attach context describing what went wrong.
///
/// # Example
///
/// ```ignore
/// use error_stack::{Report, ResultExt};
///
/// async fn add_member(group: &mut Group, key_package: &[u8]) -> Result<(), Report<GroupError>> {
///     parse_key_package(key_package)
///         .change_context(GroupError)
///         .attach_printable("invalid key package format")?;
///     
///     group.propose_add(...)
///         .await
///         .change_context(GroupError)
///         .attach_printable("failed to propose member addition")?;
///     
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct GroupError;

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("group operation failed")
    }
}

impl std::error::Error for GroupError {}

/// Error type for connector operations (connecting to acceptors).
///
/// This is a simple marker type - use [`error_stack::Report`] methods
/// to attach context describing what went wrong.
#[derive(Debug)]
pub struct ConnectorError;

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("connector operation failed")
    }
}

impl std::error::Error for ConnectorError {}
