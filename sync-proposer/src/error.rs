use std::fmt;

/// Marker error for group operations. Use `error_stack::Report<GroupError>` with
/// context attachments for details.
#[derive(Debug)]
pub struct GroupError;

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("group operation failed")
    }
}

impl std::error::Error for GroupError {}
