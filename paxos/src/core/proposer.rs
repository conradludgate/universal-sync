//! Pure proposer state machine - no I/O, no async
//!
//! This module contains the core state transition logic for a Paxos proposer.

use std::collections::BTreeMap;
use std::hash::Hash;

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
pub(crate) enum ProposerPhase<K, P, M, I>
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
    pub(crate) fn proposal(&self) -> &K {
        &self.proposal
    }

    /// Get the current value
    #[must_use]
    pub(crate) fn value(&self) -> &M {
        &self.value
    }

    /// Check if we're still in the Preparing phase
    #[must_use]
    pub(crate) fn is_preparing(&self) -> bool {
        matches!(self.phase, ProposerPhase::Preparing { .. })
    }

    /// Check if we're in the Accepting phase
    #[must_use]
    pub(crate) fn is_accepting(&self) -> bool {
        matches!(self.phase, ProposerPhase::Accepting { .. })
    }

    /// Check if we've learned a value
    #[must_use]
    pub(crate) fn is_learned(&self) -> bool {
        matches!(self.phase, ProposerPhase::Learned)
    }

    /// Check if we failed
    #[must_use]
    pub(crate) fn is_failed(&self) -> bool {
        matches!(self.phase, ProposerPhase::Failed { .. })
    }

    /// Get quorum size
    #[must_use]
    pub(crate) fn quorum(&self) -> usize {
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
    pub(crate) fn handle_promise<F>(
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
    pub(crate) fn handle_accepted(
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
