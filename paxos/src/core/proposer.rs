//! Pure proposer state machine — no I/O, no async.

use std::collections::BTreeMap;
use std::hash::Hash;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposerCore<K, P, M, I>
where
    K: Ord,
{
    proposal: K,
    value: M,
    phase: ProposerPhase<K, P, M, I>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProposerPhase<K, P, M, I>
where
    K: Ord,
{
    Preparing {
        promises: BTreeMap<I, (K, Option<(P, M)>)>,
    },
    Accepting {
        accepts: std::collections::BTreeSet<I>,
    },
    Learned,
    Failed {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparePhaseResult<K, P, M> {
    Pending,
    Quorum {
        /// May be adopted from a higher previous proposal
        value: M,
    },
    Rejected {
        superseded_by: K,
        accepted: Option<(P, M)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptPhaseResult<K, P, M> {
    Pending,
    Learned {
        proposal: P,
        value: M,
    },
    Rejected {
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

    #[must_use]
    #[cfg(test)]
    pub(crate) fn proposal(&self) -> &K {
        &self.proposal
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn value(&self) -> &M {
        &self.value
    }

    #[must_use]
    pub(crate) fn is_preparing(&self) -> bool {
        matches!(self.phase, ProposerPhase::Preparing { .. })
    }

    #[must_use]
    pub(crate) fn is_accepting(&self) -> bool {
        matches!(self.phase, ProposerPhase::Accepting { .. })
    }

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
            return PreparePhaseResult::Pending;
        };

        if promised_key > self.proposal {
            self.phase = ProposerPhase::Failed {
                superseded_by: promised_key.clone(),
            };
            return PreparePhaseResult::Rejected {
                superseded_by: promised_key,
                accepted,
            };
        }

        if promised_key == self.proposal {
            promises.insert(acceptor_id, (promised_key, accepted));
        }

        if promises.len() >= self.quorum {
            // Adopt the highest accepted value if any (Paxos value adoption rule)
            let highest_accepted = promises
                .values()
                .filter_map(|(_, acc)| acc.as_ref())
                .max_by(|(p1, _), (p2, _)| get_key(p1).cmp(&get_key(p2)))
                .cloned();

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

    pub(crate) fn handle_accepted(
        &mut self,
        acceptor_id: I,
        accepted_key: Option<K>,
        proposal: P,
    ) -> AcceptPhaseResult<K, P, M> {
        let ProposerPhase::Accepting { accepts } = &mut self.phase else {
            return AcceptPhaseResult::Pending;
        };

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
