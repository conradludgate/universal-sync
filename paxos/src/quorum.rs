//! Quorum tracking utilities shared between proposer and learner

use std::collections::BTreeMap;

use crate::{Learner, Proposal, ProposalKey};

/// Key type for quorum tracking maps.
type Key<L> = ProposalKey<
    <<L as Learner>::Proposal as Proposal>::RoundId,
    <<L as Learner>::Proposal as Proposal>::AttemptId,
>;

/// Tracks proposal counts and detects when quorum is reached.
///
/// Used by both proposer (to detect learned values) and learner
/// (to know when to apply a value).
pub struct QuorumTracker<L: Learner> {
    counts: BTreeMap<Key<L>, (usize, L::Proposal, L::Message)>,
    quorum: usize,
}

impl<L: Learner> QuorumTracker<L> {
    /// Create a new tracker for the given number of acceptors.
    #[must_use]
    pub fn new(num_acceptors: usize) -> Self {
        Self {
            counts: BTreeMap::new(),
            quorum: (num_acceptors / 2) + 1,
        }
    }

    /// Track a (proposal, message) pair. Returns `Some((&proposal, &message))` if quorum
    /// was just reached, `None` otherwise.
    pub fn track(
        &mut self,
        proposal: L::Proposal,
        message: L::Message,
    ) -> Option<(&L::Proposal, &L::Message)> {
        let key = proposal.key();
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

    /// Clear all entries for a given round.
    pub fn clear_round(&mut self, round: <L::Proposal as Proposal>::RoundId) {
        self.counts.retain(|k, _| k.round() != round);
    }

    /// Get the quorum threshold.
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }
}
