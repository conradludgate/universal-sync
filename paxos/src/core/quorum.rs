//! Pure quorum tracking - counts (proposal, value) pairs and detects quorum
//!
//! This is used by proposers/learners to detect when a value is learned.

use std::collections::BTreeMap;

/// Pure quorum tracker - counts (proposal, value) pairs and detects quorum
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct QuorumCore<K, P, M>
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
    pub(crate) fn new(num_acceptors: usize) -> Self {
        Self {
            counts: BTreeMap::new(),
            quorum: num_acceptors / 2 + 1,
        }
    }

    /// Get the quorum threshold
    #[must_use]
    pub(crate) fn quorum(&self) -> usize {
        self.quorum
    }

    /// Track a (proposal, message) pair.
    ///
    /// Returns `Some((&proposal, &message))` if quorum was JUST reached (exactly),
    /// `None` otherwise.
    pub(crate) fn track(&mut self, key: K, proposal: P, message: M) -> Option<(&P, &M)> {
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
    pub(crate) fn check_quorum<F>(&self, matches_prefix: F) -> Option<(&P, &M)>
    where
        F: Fn(&K) -> bool,
    {
        self.counts
            .iter()
            .find(|(k, (count, _, _))| matches_prefix(k) && *count >= self.quorum)
            .map(|(_, (_, proposal, message))| (proposal, message))
    }
}
