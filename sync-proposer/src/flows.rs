//! Helper functions for group operations

use iroh::EndpointAddr;
use mls_rs::ExtensionList;
use universal_sync_core::{AcceptorsExt, CrdtSnapshotExt};

/// Create a group info extension list for a welcome message.
///
/// Includes:
/// - The acceptor addresses so the joiner knows who to connect to
/// - The CRDT state snapshot so the joiner has the current application state
///
/// # Panics
/// Panics if encoding extensions fails (should never happen).
///
/// # Example
///
/// ```ignore
/// let ext = welcome_group_info_extensions(
///     learner.acceptors().values().cloned(),
///     crdt_snapshot,
/// );
/// let commit = group
///     .commit_builder()
///     .add_member(key_package)
///     .unwrap()
///     .set_group_info_ext(ext)
///     .build()
///     .unwrap();
/// ```
#[must_use]
pub(crate) fn welcome_group_info_extensions(
    acceptors: impl IntoIterator<Item = EndpointAddr>,
    crdt_snapshot: Vec<u8>,
) -> ExtensionList {
    let mut extensions = ExtensionList::default();
    extensions
        .set_from(AcceptorsExt::new(acceptors))
        .expect("AcceptorsExt encoding should not fail");
    extensions
        .set_from(CrdtSnapshotExt::new(crdt_snapshot))
        .expect("CrdtSnapshotExt encoding should not fail");
    extensions
}
