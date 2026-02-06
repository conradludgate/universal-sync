//! Pure acceptor state machine — no I/O, no async, no synchronization.
//! Decision logic matches the TLA+ specification in `spec/MultiPaxos.tla`.

use std::collections::BTreeMap;

use super::types::{AcceptorRequest, AcceptorResponse};

/// Pure decision functions matching the TLA+ spec.
///
/// - **Promise (Phase 1b)**: Succeeds if `proposal >= promised` AND `proposal >= accepted`
/// - **Accept (Phase 2b)**: Succeeds if `promised == proposal` (exact match) AND `proposal >= accepted`
pub mod decision {
    /// From TLA+ `Promise(a, p)`:
    /// ```text
    /// /\ ProposalGE(p, promised[a][r])
    /// /\ ProposalGE(p, AcceptedProposal(a, r))
    /// ```
    #[must_use]
    pub fn should_promise<P: Ord>(
        proposal: &P,
        current_promised: Option<&P>,
        current_accepted: Option<&P>,
    ) -> bool {
        let dominated_by_promise = current_promised.is_some_and(|p| p > proposal);
        let dominated_by_accept = current_accepted.is_some_and(|p| p > proposal);

        !dominated_by_promise && !dominated_by_accept
    }

    /// From TLA+ `Accept(a, p, v)`:
    /// ```text
    /// /\ promised[a][r] = p                    -- exact match required
    /// /\ ProposalGE(p, AcceptedProposal(a, r))
    /// ```
    #[must_use]
    pub fn should_accept<P: Ord>(
        proposal: &P,
        current_promised: Option<&P>,
        current_accepted: Option<&P>,
    ) -> bool {
        // No leadership optimization — accept only if this exact proposal was promised
        let exactly_promised = current_promised.is_some_and(|p| p == proposal);
        let dominated_by_accept = current_accepted.is_some_and(|p| p > proposal);

        exactly_promised && !dominated_by_accept
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AcceptorCore<R, P, M>
where
    R: Ord,
    P: Ord,
{
    pub(crate) promised: BTreeMap<R, P>,
    pub(crate) accepted: BTreeMap<R, (P, M)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PrepareResult<P, M> {
    Promised {
        promised: P,
        accepted: Option<(P, M)>,
    },
    Rejected {
        promised: P,
        accepted: Option<(P, M)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AcceptResult<P, M> {
    Accepted {
        proposal: P,
        message: M,
    },
    Rejected,
}

impl<R, P, M> AcceptorCore<R, P, M>
where
    R: Ord + Copy,
    P: Ord + Clone,
    M: Clone,
{
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            promised: BTreeMap::new(),
            accepted: BTreeMap::new(),
        }
    }

    pub(crate) fn get(&self, round: R) -> (Option<P>, Option<(P, M)>) {
        (
            self.promised.get(&round).cloned(),
            self.accepted.get(&round).cloned(),
        )
    }

    pub(crate) fn prepare(&mut self, round: R, proposal: P) -> PrepareResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);
        let accepted_proposal = current_accepted.as_ref().map(|(p, _)| p);

        if decision::should_promise(&proposal, current_promised, accepted_proposal) {
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

    pub(crate) fn accept(&mut self, round: R, proposal: P, message: M) -> AcceptResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);
        let accepted_proposal = current_accepted.as_ref().map(|(p, _)| p);

        if decision::should_accept(&proposal, current_promised, accepted_proposal) {
            self.accepted
                .insert(round, (proposal.clone(), message.clone()));
            AcceptResult::Accepted { proposal, message }
        } else {
            AcceptResult::Rejected
        }
    }

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
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        let result = core.accept(1, 100, "hello".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert!(!core.accepted.contains_key(&1));
    }

    #[test]
    fn test_accept_different_proposal_rejected() {
        let mut core: AcceptorCore<u64, u64, String> = AcceptorCore::new();
        core.prepare(1, 100);
        let result = core.accept(1, 101, "hello".to_string());
        assert!(matches!(result, AcceptResult::Rejected));
        assert!(!core.accepted.contains_key(&1));
        assert_eq!(core.promised.get(&1), Some(&100));
    }
}
