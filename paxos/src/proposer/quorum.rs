//! Quorum tracking for the proposer/learner runtime.

use std::collections::BTreeMap;

use tracing::trace;

use crate::traits::{Learner, Proposal, ProposalKey};

type Key<L> = ProposalKey<<L as Learner>::Proposal>;

/// Tracks proposal counts and detects when quorum is reached.
pub struct QuorumTracker<L: Learner> {
    counts: BTreeMap<Key<L>, (usize, L::Proposal, L::Message)>,
    quorum: usize,
}

impl<L: Learner> QuorumTracker<L> {
    #[must_use]
    pub fn new(num_acceptors: usize) -> Self {
        Self {
            counts: BTreeMap::new(),
            quorum: (num_acceptors / 2) + 1,
        }
    }

    /// Track a (proposal, message) pair. Returns `Some` if quorum was just reached.
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
        trace!(round = ?key.0, count = entry.0, quorum = self.quorum, "tracking proposal");
        if entry.0 == self.quorum {
            Some((&entry.1, &entry.2))
        } else {
            None
        }
    }

    pub fn check_quorum(
        &self,
        round: <L::Proposal as Proposal>::RoundId,
    ) -> Option<(&L::Proposal, &L::Message)> {
        self.counts
            .iter()
            .find(|(k, (count, _, _))| k.0 == round && *count >= self.quorum)
            .map(|(_, (_, proposal, message))| (proposal, message))
    }
}
