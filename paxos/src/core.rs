//! Pure state machine core for Paxos - no I/O, no async
//!
//! This module contains the core state transition logic that is shared between:
//! - The async runtime implementation (`state.rs`, `acceptor.rs`)
//! - The Stateright model checker tests
//!
//! By extracting this logic, we ensure the model checker verifies the exact
//! same state transitions as the production code.

use std::collections::BTreeMap;
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

impl<R, A, N> ProposalKey<R, A, N> {
    /// Create a new proposal key.
    #[must_use]
    pub fn new(round: R, attempt: A, node: N) -> Self {
        Self {
            round,
            attempt,
            node,
        }
    }
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

// =============================================================================
// ACCEPTOR CORE
// =============================================================================

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
    pub promised: BTreeMap<R, P>,
    /// Per-round: accepted (proposal, message)
    pub accepted: BTreeMap<R, (P, M)>,
}

/// Result of handling a Prepare request
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareResult<P, M> {
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
pub enum AcceptResult<P, M> {
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
    pub fn new() -> Self {
        Self {
            promised: BTreeMap::new(),
            accepted: BTreeMap::new(),
        }
    }

    /// Get the current state for a round
    pub fn get(&self, round: R) -> (Option<P>, Option<(P, M)>) {
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
    pub fn prepare(&mut self, round: R, proposal: P) -> PrepareResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);

        // Check if dominated by a higher promise or accept
        let dominated_by_promise = current_promised.is_some_and(|p| *p > proposal);
        let dominated_by_accept = current_accepted
            .as_ref()
            .is_some_and(|(p, _)| *p > proposal);

        if dominated_by_promise || dominated_by_accept {
            PrepareResult::Rejected {
                promised: current_promised
                    .cloned()
                    .unwrap_or_else(|| proposal.clone()),
                accepted: current_accepted.cloned(),
            }
        } else {
            // Update promise
            self.promised.insert(round, proposal.clone());
            PrepareResult::Promised {
                promised: proposal,
                accepted: current_accepted.cloned(),
            }
        }
    }

    /// Handle an Accept request - pure state transition
    ///
    /// An accept succeeds if the proposal is not dominated by:
    /// - A higher already-promised proposal
    /// - A higher already-accepted proposal
    ///
    /// On success, updates both promised and accepted values.
    /// On failure, returns Rejected without modification.
    pub fn accept(&mut self, round: R, proposal: P, message: M) -> AcceptResult<P, M> {
        let current_promised = self.promised.get(&round);
        let current_accepted = self.accepted.get(&round);

        // Check if dominated by a higher promise or accept
        let dominated_by_promise = current_promised.is_some_and(|p| *p > proposal);
        let dominated_by_accept = current_accepted
            .as_ref()
            .is_some_and(|(p, _)| *p > proposal);

        if dominated_by_promise || dominated_by_accept {
            AcceptResult::Rejected
        } else {
            // Update both promise and accepted
            self.promised.insert(round, proposal.clone());
            self.accepted
                .insert(round, (proposal.clone(), message.clone()));
            AcceptResult::Accepted { proposal, message }
        }
    }

    /// Get all accepted values from a given round onwards
    #[must_use]
    pub fn accepted_from(&self, from_round: R) -> Vec<(P, M)> {
        self.accepted
            .range(from_round..)
            .map(|(_, (p, m))| (p.clone(), m.clone()))
            .collect()
    }

    /// Get the highest round that has been accepted
    #[must_use]
    pub fn highest_accepted_round(&self) -> Option<R> {
        self.accepted.keys().next_back().copied()
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

// =============================================================================
// PROPOSER CORE
// =============================================================================

/// Pure proposer state - tracks phase and quorum for a single proposal round
///
/// This extracts the phase tracking logic that is shared between:
/// - The async runtime proposer
/// - The Stateright model checker
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposerCore<K, P, M, I>
where
    K: Ord,
{
    /// Current proposal key
    proposal: K,
    /// Value being proposed (or adopted from higher proposal)
    value: M,
    /// Current phase
    phase: ProposerPhase<K, P, M, I>,
    /// Quorum size
    quorum: usize,
}

impl<K, P, M, I> Hash for ProposerCore<K, P, M, I>
where
    K: Ord + Hash,
    P: Hash,
    M: Hash,
    I: Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.proposal.hash(state);
        self.value.hash(state);
        self.phase.hash(state);
        self.quorum.hash(state);
    }
}

/// Proposer phase
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposerPhase<K, P, M, I>
where
    K: Ord,
{
    /// Collecting promises (Phase 1)
    Preparing {
        /// Map from acceptor ID to (`promised_key`, accepted value if any)
        promises: BTreeMap<I, (K, Option<(P, M)>)>,
    },
    /// Collecting accepts (Phase 2)
    Accepting {
        /// Set of acceptors that have accepted
        accepts: std::collections::BTreeSet<I>,
    },
    /// Successfully reached quorum
    Learned,
    /// Failed - was superseded by a higher proposal
    Failed {
        /// The higher proposal key that superseded us
        superseded_by: K,
    },
}

impl<K, P, M, I> Hash for ProposerPhase<K, P, M, I>
where
    K: Ord + Hash,
    P: Hash,
    M: Hash,
    I: Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Preparing { promises } => {
                for (id, (key, accepted)) in promises {
                    id.hash(state);
                    key.hash(state);
                    accepted.hash(state);
                }
            }
            Self::Accepting { accepts } => {
                for id in accepts {
                    id.hash(state);
                }
            }
            Self::Learned => {}
            Self::Failed { superseded_by } => superseded_by.hash(state),
        }
    }
}

/// Result of processing a response during the Prepare phase
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparePhaseResult<K, P, M> {
    /// Need more promises
    Pending,
    /// Got quorum - ready to accept with this value
    Quorum {
        /// The value to accept (may be adopted from a higher previous proposal)
        value: M,
    },
    /// Rejected by a higher proposal
    Rejected {
        /// The higher proposal key
        superseded_by: K,
        /// Accepted value from the higher proposal (if any)
        accepted: Option<(P, M)>,
    },
}

/// Result of processing a response during the Accept phase
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptPhaseResult<K, P, M> {
    /// Need more accepts
    Pending,
    /// Got quorum - value is learned
    Learned {
        /// The learned proposal
        proposal: P,
        /// The learned value
        value: M,
    },
    /// Rejected by a higher proposal
    Rejected {
        /// The higher proposal key
        superseded_by: K,
    },
}

impl<K, P, M, I> ProposerCore<K, P, M, I>
where
    K: Ord + Clone,
    P: Clone,
    M: Clone,
    I: Ord + Clone,
{
    /// Create a new proposer core in the Preparing phase
    #[must_use]
    pub fn new(proposal: K, value: M, num_acceptors: usize) -> Self {
        Self {
            proposal,
            value,
            phase: ProposerPhase::Preparing {
                promises: BTreeMap::new(),
            },
            quorum: num_acceptors / 2 + 1,
        }
    }

    /// Get the current proposal key
    #[must_use]
    pub fn proposal(&self) -> &K {
        &self.proposal
    }

    /// Get the current value
    #[must_use]
    pub fn value(&self) -> &M {
        &self.value
    }

    /// Check if we're still in the Preparing phase
    #[must_use]
    pub fn is_preparing(&self) -> bool {
        matches!(self.phase, ProposerPhase::Preparing { .. })
    }

    /// Check if we're in the Accepting phase
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        matches!(self.phase, ProposerPhase::Accepting { .. })
    }

    /// Check if we've learned a value
    #[must_use]
    pub fn is_learned(&self) -> bool {
        matches!(self.phase, ProposerPhase::Learned)
    }

    /// Check if we failed
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self.phase, ProposerPhase::Failed { .. })
    }

    /// Get quorum size
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// Process a promise response during the Prepare phase.
    ///
    /// - `acceptor_id`: The acceptor that sent the response
    /// - `promised_key`: The key of the proposal the acceptor promised
    /// - `accepted`: The acceptor's currently accepted (proposal, value) if any
    /// - `get_key`: Function to extract key from a proposal
    ///
    /// Returns the result of processing this response.
    pub fn handle_promise<F>(
        &mut self,
        acceptor_id: I,
        promised_key: K,
        accepted: Option<(P, M)>,
        get_key: F,
    ) -> PreparePhaseResult<K, P, M>
    where
        F: Fn(&P) -> K,
    {
        let ProposerPhase::Preparing { promises } = &mut self.phase else {
            return PreparePhaseResult::Pending; // Wrong phase
        };

        // Check if we were superseded
        if promised_key > self.proposal {
            self.phase = ProposerPhase::Failed {
                superseded_by: promised_key.clone(),
            };
            return PreparePhaseResult::Rejected {
                superseded_by: promised_key,
                accepted,
            };
        }

        // Only count promises for our proposal
        if promised_key == self.proposal {
            promises.insert(acceptor_id, (promised_key, accepted));
        }

        // Check quorum
        if promises.len() >= self.quorum {
            // Find highest accepted value
            let highest_accepted = promises
                .values()
                .filter_map(|(_, acc)| acc.as_ref())
                .max_by(|(p1, _), (p2, _)| get_key(p1).cmp(&get_key(p2)))
                .cloned();

            // Adopt the highest accepted value if any
            let value = highest_accepted.map_or_else(|| self.value.clone(), |(_, m)| m);

            self.value = value.clone();
            self.phase = ProposerPhase::Accepting {
                accepts: std::collections::BTreeSet::new(),
            };

            PreparePhaseResult::Quorum { value }
        } else {
            PreparePhaseResult::Pending
        }
    }

    /// Process an accept response during the Accept phase.
    ///
    /// - `acceptor_id`: The acceptor that sent the response
    /// - `accepted_key`: The key of the proposal the acceptor accepted (if any)
    /// - `proposal`: The actual proposal object (for returning in Learned result)
    ///
    /// Returns the result of processing this response.
    pub fn handle_accepted(
        &mut self,
        acceptor_id: I,
        accepted_key: Option<K>,
        proposal: P,
    ) -> AcceptPhaseResult<K, P, M> {
        let ProposerPhase::Accepting { accepts } = &mut self.phase else {
            return AcceptPhaseResult::Pending; // Wrong phase
        };

        // Check if they accepted our proposal
        if accepted_key.as_ref() == Some(&self.proposal) {
            accepts.insert(acceptor_id);

            if accepts.len() >= self.quorum {
                let value = self.value.clone();
                self.phase = ProposerPhase::Learned;
                AcceptPhaseResult::Learned { proposal, value }
            } else {
                AcceptPhaseResult::Pending
            }
        } else if let Some(key) = accepted_key {
            if key > self.proposal {
                // Superseded by higher proposal
                self.phase = ProposerPhase::Failed {
                    superseded_by: key.clone(),
                };
                AcceptPhaseResult::Rejected { superseded_by: key }
            } else {
                AcceptPhaseResult::Pending
            }
        } else {
            AcceptPhaseResult::Pending
        }
    }
}

// =============================================================================
// QUORUM TRACKER
// =============================================================================

/// Pure quorum tracker - counts (proposal, value) pairs and detects quorum
///
/// This is used by proposers/learners to detect when a value is learned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuorumCore<K, P, M>
where
    K: Ord,
{
    /// Map from proposal key to (count, proposal, message)
    counts: BTreeMap<K, (usize, P, M)>,
    /// Quorum threshold
    quorum: usize,
}

impl<K, P, M> QuorumCore<K, P, M>
where
    K: Ord + Clone,
    P: Clone,
    M: Clone,
{
    /// Create a new quorum tracker
    #[must_use]
    pub fn new(num_acceptors: usize) -> Self {
        Self {
            counts: BTreeMap::new(),
            quorum: num_acceptors / 2 + 1,
        }
    }

    /// Get the quorum threshold
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// Track a (proposal, message) pair.
    ///
    /// Returns `Some((&proposal, &message))` if quorum was JUST reached (exactly),
    /// `None` otherwise.
    pub fn track(&mut self, key: K, proposal: P, message: M) -> Option<(&P, &M)> {
        let entry = self
            .counts
            .entry(key)
            .or_insert_with(|| (0, proposal, message));
        entry.0 += 1;
        if entry.0 == self.quorum {
            Some((&entry.1, &entry.2))
        } else {
            None
        }
    }

    /// Check if any entry with the given key prefix has reached quorum.
    ///
    /// The `matches_prefix` function should return true if a key matches the desired prefix.
    pub fn check_quorum<F>(&self, matches_prefix: F) -> Option<(&P, &M)>
    where
        F: Fn(&K) -> bool,
    {
        self.counts
            .iter()
            .find(|(k, (count, _, _))| matches_prefix(k) && *count >= self.quorum)
            .map(|(_, (_, proposal, message))| (proposal, message))
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
}
