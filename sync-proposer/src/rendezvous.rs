//! Rendezvous hashing for acceptor selection
//!
//! This module provides rendezvous hashing (Highest Random Weight) to select
//! a subset of acceptors for message delivery.
//!
//! # Algorithm
//!
//! For each acceptor, compute a score as `hash(acceptor_id || message_id)`.
//! Sort acceptors by score (highest first) and select the top `k` acceptors.
//!
//! This ensures:
//! - Deterministic selection: any client computes the same set
//! - Minimal disruption: adding/removing acceptors only affects messages mapped to them

use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};

use universal_sync_core::{AcceptorId, MessageId};

/// Compute a hash score for an acceptor and message combination.
fn compute_score(acceptor_id: &AcceptorId, message_id: &MessageId) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    acceptor_id.as_bytes().hash(&mut hasher);
    message_id.group_id.as_bytes().hash(&mut hasher);
    message_id.epoch.0.hash(&mut hasher);
    message_id.sender.0.hash(&mut hasher);
    message_id.index.hash(&mut hasher);
    hasher.finish()
}

/// Select acceptors for a message using rendezvous hashing.
///
/// Returns up to `count` acceptors, sorted by their hash score (highest first).
/// If `count` is greater than the number of acceptors, returns all acceptors.
///
/// # Arguments
/// * `acceptors` - Iterator of acceptor IDs to select from
/// * `message_id` - The message ID to hash against
/// * `count` - Maximum number of acceptors to select
pub fn select_acceptors<'a>(
    acceptors: impl IntoIterator<Item = &'a AcceptorId>,
    message_id: &MessageId,
    count: usize,
) -> Vec<AcceptorId> {
    if count == 0 {
        return Vec::new();
    }

    // Use a min-heap to efficiently keep the top k scores
    let mut heap: BinaryHeap<std::cmp::Reverse<(u64, AcceptorId)>> = BinaryHeap::new();

    for acceptor_id in acceptors {
        let score = compute_score(acceptor_id, message_id);

        if heap.len() < count {
            heap.push(std::cmp::Reverse((score, *acceptor_id)));
        } else if let Some(&std::cmp::Reverse((min_score, _))) = heap.peek() {
            if score > min_score {
                heap.pop();
                heap.push(std::cmp::Reverse((score, *acceptor_id)));
            }
        }
    }

    // Extract acceptors, sorted by score (highest first)
    let mut result: Vec<_> = heap.into_iter().map(|r| r.0).collect();
    result.sort_by(|a, b| b.0.cmp(&a.0)); // Sort by score descending
    result.into_iter().map(|(_, id)| id).collect()
}

/// Calculate how many acceptors to select for message delivery.
///
/// Returns `ceil(sqrt(n))` where `n` is the number of acceptors.
/// Minimum is 1 (if there are any acceptors).
#[must_use]
pub fn delivery_count(num_acceptors: usize) -> usize {
    if num_acceptors == 0 {
        return 0;
    }
    let sqrt = (num_acceptors as f64).sqrt().ceil() as usize;
    sqrt.max(1)
}

#[cfg(test)]
mod tests {
    use universal_sync_core::{Epoch, GroupId, MemberId};

    use super::*;

    fn make_acceptor(id: u8) -> AcceptorId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        AcceptorId::from_bytes(bytes)
    }

    fn make_message_id(index: u32) -> MessageId {
        MessageId {
            group_id: GroupId::new([1u8; 32]),
            epoch: Epoch(0),
            sender: MemberId(0),
            index,
        }
    }

    #[test]
    fn test_delivery_count() {
        assert_eq!(delivery_count(0), 0);
        assert_eq!(delivery_count(1), 1);
        assert_eq!(delivery_count(4), 2);
        assert_eq!(delivery_count(9), 3);
        assert_eq!(delivery_count(10), 4); // ceil(sqrt(10)) = 4
        assert_eq!(delivery_count(100), 10);
    }

    #[test]
    fn test_select_acceptors_deterministic() {
        let acceptors: Vec<_> = (0..10).map(make_acceptor).collect();
        let msg_id = make_message_id(42);

        let selected1 = select_acceptors(&acceptors, &msg_id, 3);
        let selected2 = select_acceptors(&acceptors, &msg_id, 3);

        assert_eq!(selected1, selected2);
        assert_eq!(selected1.len(), 3);
    }

    #[test]
    fn test_select_acceptors_different_messages() {
        let acceptors: Vec<_> = (0..10).map(make_acceptor).collect();

        let selected1 = select_acceptors(&acceptors, &make_message_id(1), 3);
        let selected2 = select_acceptors(&acceptors, &make_message_id(2), 3);

        // Different messages should (usually) select different acceptors
        // This is probabilistic, but with 10 acceptors and 3 selected, collision is unlikely
        assert_ne!(selected1, selected2);
    }

    #[test]
    fn test_select_more_than_available() {
        let acceptors: Vec<_> = (0..3).map(make_acceptor).collect();
        let msg_id = make_message_id(0);

        let selected = select_acceptors(&acceptors, &msg_id, 10);
        assert_eq!(selected.len(), 3);
    }
}
