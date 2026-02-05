//! Core type definitions for Paxos protocol
//!
//! These types are shared between the async runtime and model checker.

use std::hash::Hash;

// =============================================================================
// PROPOSAL KEY
// =============================================================================

/// Ordering key for proposals - compares by (round, attempt, `node_id`).
///
/// This is a simple tuple struct with lexicographic ordering. Used by both
/// the async implementation and Stateright model checker.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProposalKey<R, A, N> {
    /// Round identifier (log slot)
    pub round: R,
    /// Attempt number within the round
    pub attempt: A,
    /// Node identifier (proposer)
    pub node: N,
}

// =============================================================================
// CORE MESSAGE TYPES
// =============================================================================

/// Request from proposer to acceptor
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AcceptorRequest<P, M> {
    /// Phase 1a: Prepare request with proposal
    Prepare(P),
    /// Phase 2a: Accept request with proposal and message
    Accept(P, M),
}

/// Response from acceptor to proposer
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AcceptorResponse<P, M> {
    /// The proposal this response is for
    pub for_proposal: P,
    /// Highest proposal this acceptor has promised
    pub promised: Option<P>,
    /// Highest accepted (proposal, message) pair
    pub accepted: Option<(P, M)>,
}
