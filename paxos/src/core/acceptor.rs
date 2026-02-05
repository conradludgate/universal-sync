//! Pure acceptor state machine - no I/O, no async, no synchronization
//!
//! This module contains the core state transition logic for a Paxos acceptor.
//! The decision logic matches the TLA+ specification in `spec/MultiPaxos.tla`.

use std::collections::BTreeMap;

use super::types::{AcceptorRequest, AcceptorResponse};

/// Pure decision functions for Paxos acceptor logic.
///
/// These functions encode the TLA+ specification and can be used by both
/// in-memory (`AcceptorCore`) and persistent (`AcceptorStateStore`) implementations.
///
/// From TLA+ spec:
/// - **Promise (Phase 1b)**: Succeeds if `proposal >= promised` AND `proposal >= accepted`
/// - **Accept (Phase 2b)**: Succeeds if `promised == proposal` (exact match) AND `proposal >= accepted`
pub mod decision {
    /// Check if a Prepare request should be accepted.
    ///
    /// From TLA+ `Promise(a, p)` (lines 127-133):
    /// ```text
    /// /\ ProposalGE(p, promised[a][r])
    /// /\ ProposalGE(p, AcceptedProposal(a, r))
    /// ```
    ///
    /// Returns `true` if the prepare should succeed (proposal is not dominated).
    #[must_use]
    pub fn should_promise<P: Ord>(
        proposal: &P,
        current_promised: Option<&P>,
        current_accepted: Option<&P>,
    ) -> bool {
        // Reject if a strictly higher proposal was already promised
        let dominated_by_promise = current_promised.is_some_and(|p| p > proposal);
        // Reject if a strictly higher proposal was already accepted
        let dominated_by_accept = current_accepted.is_some_and(|p| p > proposal);

        !dominated_by_promise && !dominated_by_accept
    }

    /// Check if an Accept request should be accepted.
    ///
    /// From TLA+ `Accept(a, p, v)` (lines 143-162):
    /// ```text
    /// /\ promised[a][r] = p                    -- exact match required
    /// /\ ProposalGE(p, AcceptedProposal(a, r))
    /// ```
    ///
    /// Returns `true` if the accept should succeed.
    #[must_use]
    pub fn should_accept<P: Ord>(
        proposal: &P,
        current_promised: Option<&P>,
        current_accepted: Option<&P>,
    ) -> bool {
        // Require exact promise match - no leadership optimization
        // Accept only succeeds if this exact proposal was promised
        let exactly_promised = current_promised.is_some_and(|p| p == proposal);
        // Reject if a strictly higher proposal was already accepted
        let dominated_by_accept = current_accepted.is_some_and(|p| p > proposal);

        exactly_promised && !dominated_by_accept
    }
}

/// Pure acceptor state - no I/O, no async, no synchronization
///
/// This is the core state machine for a Paxos acceptor. It tracks:
/// - Per-round highest promised proposal
/// - Per-round accepted (proposal, message) pairs
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AcceptorCore<R, P, M>
where
    R: Ord,
    P: Ord,
{
    /// Per-round: highest promised proposal
    pub(crate) promised: BTreeMap<R, P>,
    /// Per-round: accepted (proposal, message)
    pub(crate) accepted: BTreeMap<R, (P, M)>,
}

/// Result of handling a Prepare request
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PrepareResult<P, M> {
    /// Promised successfully - returns the currently accepted value (if any)
    Promised {
        /// The proposal we just promised
        promised: P,
        /// Currently accepted (proposal, message) for this round
        accepted: Option<(P, M)>,
    },
    /// Rejected - a higher proposal was already promised or accepted
    Rejected {
        /// The higher proposal we already promised
        promised: P,
        /// Currently accepted (proposal, message) for this round
        accepted: Option<(P, M)>,
    },
}

/// Result of handling an Accept request
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AcceptResult<P, M> {
    /// Accepted successfully
    Accepted {
        /// The proposal we accepted
        proposal: P,
        /// The message we accepted
        message: M,
    },
    /// Rejected - a higher proposal was already promised or accepted
    Rejected,
}

impl<R, P, M> AcceptorCore<R, P, M>
where
    R: Ord + Copy,
    P: Ord + Clone,
    M: Clone,
{
    /// Create a new empty acceptor state
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            promised: BTreeMap::new(),
            accepted: BTreeMap::new(),
        }
    }

    /// Get the current state for a round
    pub(crate) fn get(&self, round: R) -> (Option<P>, Option<(P, M)>) {
        (
            self.promised.get(&round).cloned(),
            self.accepted.get(&round).cloned(),
        )
    }

    /// Handle a Prepare request - pure state transition
    ///
    /// A prepare succeeds if the proposal is not dominated by:
    /// - A higher already-promised proposal
    /// - A higher already-accepted proposal
    ///
    /// On success, updates the promised value and returns the current accepted value.
    /// On failure, returns the current state without modification.
    ///
    /// Uses [`decision::should_promise`] for the TLA+-matching decision logic.
    pub(crate) fn prepare(&mut self, round: R, proposal: P) -> PrepareResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);
        let accepted_proposal = current_accepted.as_ref().map(|(p, _)| p);

        if decision::should_promise(&proposal, current_promised, accepted_proposal) {
            // Update promise
            self.promised.insert(round, proposal.clone());
            PrepareResult::Promised {
                promised: proposal,
                accepted: current_accepted.cloned(),
            }
        } else {
            PrepareResult::Rejected {
                promised: current_promised
                    .cloned()
                    .unwrap_or_else(|| proposal.clone()),
                accepted: current_accepted.cloned(),
            }
        }
    }

    /// Handle an Accept request - pure state transition
    ///
    /// An accept succeeds only if:
    /// - The exact proposal was previously promised (no leadership optimization)
    /// - No higher proposal was already accepted
    ///
    /// On success, updates the accepted value.
    /// On failure, returns Rejected without modification.
    ///
    /// Uses [`decision::should_accept`] for the TLA+-matching decision logic.
    pub(crate) fn accept(&mut self, round: R, proposal: P, message: M) -> AcceptResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);
        let accepted_proposal = current_accepted.as_ref().map(|(p, _)| p);

        if decision::should_accept(&proposal, current_promised, accepted_proposal) {
            // Update accepted value
            self.accepted
                .insert(round, (proposal.clone(), message.clone()));
            AcceptResult::Accepted { proposal, message }
        } else {
            AcceptResult::Rejected
        }
    }

    /// Handle a request and produce a response.
    ///
    /// This is a convenience method that combines `prepare` and `accept`
    /// with message-based input/output for use in both async and model-checking contexts.
    ///
    /// The `get_round` function extracts the round from a proposal.
    pub fn handle_request<F>(
        &mut self,
        request: AcceptorRequest<P, M>,
        get_round: F,
    ) -> AcceptorResponse<P, M>
    where
        F: Fn(&P) -> R,
    {
        match request {
            AcceptorRequest::Prepare(proposal) => {
                let round = get_round(&proposal);
                let result = self.prepare(round, proposal.clone());
                let (promised, accepted) = match result {
                    PrepareResult::Promised { promised, accepted }
                    | PrepareResult::Rejected { promised, accepted } => (promised, accepted),
                };
                AcceptorResponse {
                    for_proposal: proposal,
                    promised: Some(promised),
                    accepted,
                }
            }
            AcceptorRequest::Accept(proposal, message) => {
                let round = get_round(&proposal);
                match self.accept(round, proposal.clone(), message) {
                    AcceptResult::Accepted {
                        proposal: p,
                        message: m,
                    } => AcceptorResponse {
                        for_proposal: proposal,
                        promised: Some(p.clone()),
                        accepted: Some((p, m)),
                    },
                    AcceptResult::Rejected => {
                        let (promised, accepted) = self.get(round);
                        AcceptorResponse {
                            for_proposal: proposal,
                            promised,
                            accepted,
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_empty() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        let result = core.prepare(1, 100);
        assert!(matches!(
            result,
            PrepareResult::Promised {
                promised: 100,
                accepted: None
            }
        ));
        assert_eq!(core.promised.get(&1), Some(&100));
    }

    #[test]
    fn test_prepare_higher_succeeds() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 100);
        let result = core.prepare(1, 200);
        assert!(matches!(
            result,
            PrepareResult::Promised {
                promised: 200,
                accepted: None
            }
        ));
        assert_eq!(core.promised.get(&1), Some(&200));
    }

    #[test]
    fn test_prepare_lower_rejected() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 200);
        let result = core.prepare(1, 100);
        assert!(matches!(
            result,
            PrepareResult::Rejected {
                promised: 200,
                accepted: None
            }
        ));
        // State unchanged
        assert_eq!(core.promised.get(&1), Some(&200));
    }

    #[test]
    fn test_accept_after_prepare() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 100);
        let result = core.accept(1, 100, "hello".to_string());
        assert!(matches!(result, AcceptResult::Accepted { .. }));
        assert_eq!(core.accepted.get(&1), Some(&(100, "hello".to_string())));
    }

    #[test]
    fn test_accept_dominated_by_promise() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 200);
        let result = core.accept(1, 100, "hello".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert!(!core.accepted.contains_key(&1));
    }

    #[test]
    fn test_accept_dominated_by_accept() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 200);
        core.accept(1, 200, "first".to_string());
        let result = core.accept(1, 100, "second".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert_eq!(core.accepted.get(&1), Some(&(200, "first".to_string())));
    }

    #[test]
    fn test_accept_without_prepare_rejected() {
        // Accept without prior Prepare should be rejected (no leader optimization)
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        let result = core.accept(1, 100, "hello".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert!(!core.accepted.contains_key(&1));
    }

    #[test]
    fn test_accept_different_proposal_rejected() {
        // Accept with different proposal than what was promised should be rejected
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 100);
        // Try to accept proposal 101 (different from promised 100)
        let result = core.accept(1, 101, "hello".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert!(!core.accepted.contains_key(&1));
        // Original promise should still be in place
        assert_eq!(core.promised.get(&1), Some(&100));
    }
}
