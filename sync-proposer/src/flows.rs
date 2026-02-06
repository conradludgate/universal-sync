use iroh::EndpointAddr;
use mls_rs::ExtensionList;
use universal_sync_core::{AcceptorsExt, CrdtSnapshotExt};

/// Build GroupInfo extensions for a welcome message (acceptor addresses + CRDT snapshot).
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
