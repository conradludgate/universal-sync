//! Wire types for the proposal protocol.

use universal_sync_core::{GroupMessage, GroupProposal};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ProposalRequest {
    Prepare(GroupProposal),
    Accept(GroupProposal, GroupMessage),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProposalResponse {
    pub(crate) promised: GroupProposal,
    pub(crate) accepted: Option<(GroupProposal, GroupMessage)>,
}
