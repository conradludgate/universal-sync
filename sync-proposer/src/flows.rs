//! Helper functions for group operations

use iroh::EndpointAddr;
use mls_rs::ExtensionList;
use universal_sync_core::AcceptorsExt;

/// Create an extension list containing the acceptor addresses.
///
/// Use this when adding members to a group to include the acceptor list
/// in the Welcome message's `GroupInfo` extensions.
///
/// # Panics
/// Panics if encoding the `AcceptorsExt` fails (should never happen).
///
/// # Example
///
/// ```ignore
/// let ext = acceptors_extension(learner.acceptors().values().cloned());
/// let commit = group
///     .commit_builder()
///     .add_member(key_package)
///     .unwrap()
///     .set_group_info_ext(ext)
///     .build()
///     .unwrap();
/// ```
#[must_use]
pub fn acceptors_extension(acceptors: impl IntoIterator<Item = EndpointAddr>) -> ExtensionList {
    let acceptors_ext = AcceptorsExt::new(acceptors);
    let mut extensions = ExtensionList::default();
    extensions
        .set_from(acceptors_ext)
        .expect("AcceptorsExt encoding should not fail");
    extensions
}
