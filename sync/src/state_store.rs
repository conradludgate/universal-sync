//! Persistent acceptor state store using fjall
//!
//! This module provides a durable implementation of [`AcceptorStateStore`]
//! backed by the fjall embedded key-value store.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use futures::{Stream, StreamExt, stream};
use tokio::sync::broadcast;

use universal_sync_paxos::acceptor::RoundState;
use universal_sync_paxos::{AcceptorStateStore, Learner, Proposal};

use crate::message::GroupMessage;
use crate::proposal::{Epoch, GroupProposal};

/// Persistent acceptor state store backed by fjall
///
/// This implementation persists all state changes to disk before returning,
/// ensuring crash recovery is possible.
///
/// Uses two separate keyspaces:
/// - `promised`: epoch -> proposal (small values)
/// - `accepted`: epoch -> (proposal, message) (larger values)
pub struct FjallStateStore {
    /// The fjall database
    db: Database,
    /// Keyspace for promised proposals (epoch -> proposal)
    promised: Keyspace,
    /// Keyspace for accepted values (epoch -> (proposal, message))
    accepted: Keyspace,
    /// Broadcast channel for live subscriptions
    broadcast: broadcast::Sender<(GroupProposal, GroupMessage)>,
}

impl FjallStateStore {
    /// Open or create a new state store at the given path
    ///
    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_owned();

        // Use spawn_blocking for the synchronous fjall operations
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

    /// Synchronous open for use in spawn_blocking
    fn open_sync(path: &Path) -> Result<Self, fjall::Error> {
        let db = Database::builder(path).open()?;

        let promised = db.keyspace("promised", KeyspaceCreateOptions::default)?;
        let accepted = db.keyspace("accepted", KeyspaceCreateOptions::default)?;

        let (broadcast, _) = broadcast::channel(64);

        Ok(Self {
            db,
            promised,
            accepted,
            broadcast,
        })
    }

    /// Build an epoch key (8 bytes, big-endian for proper ordering)
    fn epoch_key(epoch: Epoch) -> [u8; 8] {
        epoch.0.to_be_bytes()
    }

    /// Parse an epoch from a key
    fn parse_epoch_key(key: &[u8]) -> Option<Epoch> {
        let bytes: [u8; 8] = key.try_into().ok()?;
        Some(Epoch(u64::from_be_bytes(bytes)))
    }

    /// Serialize a proposal for storage
    fn serialize_proposal(proposal: &GroupProposal) -> Vec<u8> {
        postcard::to_allocvec(proposal).expect("serialization should not fail")
    }

    /// Deserialize a proposal from storage
    fn deserialize_proposal(bytes: &[u8]) -> Option<GroupProposal> {
        postcard::from_bytes(bytes).ok()
    }

    /// Serialize a message for storage
    fn serialize_message(message: &GroupMessage) -> Vec<u8> {
        postcard::to_allocvec(message).expect("serialization should not fail")
    }

    /// Deserialize a message from storage
    fn deserialize_message(bytes: &[u8]) -> Option<GroupMessage> {
        postcard::from_bytes(bytes).ok()
    }

    /// Get promised proposal for a round (synchronous)
    fn get_promised_sync(&self, epoch: Epoch) -> Option<GroupProposal> {
        let key = Self::epoch_key(epoch);
        self.promised
            .get(key)
            .ok()
            .flatten()
            .and_then(|bytes| Self::deserialize_proposal(&bytes))
    }

    /// Get accepted (proposal, message) for a round (synchronous)
    fn get_accepted_sync(&self, epoch: Epoch) -> Option<(GroupProposal, GroupMessage)> {
        let key = Self::epoch_key(epoch);
        self.accepted.get(key).ok().flatten().and_then(|bytes| {
            // Accepted value is serialized as (proposal_len, proposal, message)
            if bytes.len() < 4 {
                return None;
            }
            let proposal_len = u32::from_be_bytes(bytes[0..4].try_into().ok()?) as usize;
            if bytes.len() < 4 + proposal_len {
                return None;
            }
            let proposal = Self::deserialize_proposal(&bytes[4..4 + proposal_len])?;
            let message = Self::deserialize_message(&bytes[4 + proposal_len..])?;
            Some((proposal, message))
        })
    }

    /// Set promised proposal for a round (synchronous)
    fn set_promised_sync(&self, proposal: &GroupProposal) -> Result<(), fjall::Error> {
        let key = Self::epoch_key(proposal.epoch);
        let value = Self::serialize_proposal(proposal);
        self.promised.insert(key, &value)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Set accepted (proposal, message) for a round (synchronous)
    fn set_accepted_sync(
        &self,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), fjall::Error> {
        let key = Self::epoch_key(proposal.epoch);

        // Serialize as (proposal_len: u32, proposal, message)
        let proposal_bytes = Self::serialize_proposal(proposal);
        let message_bytes = Self::serialize_message(message);

        let mut value = Vec::with_capacity(4 + proposal_bytes.len() + message_bytes.len());
        value.extend_from_slice(&(proposal_bytes.len() as u32).to_be_bytes());
        value.extend_from_slice(&proposal_bytes);
        value.extend_from_slice(&message_bytes);

        self.accepted.insert(key, &value)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Get all accepted values from a given round onwards (synchronous)
    fn get_accepted_from_sync(&self, from_epoch: Epoch) -> Vec<(GroupProposal, GroupMessage)> {
        let start_key = Self::epoch_key(from_epoch);

        self.accepted
            .range(start_key..)
            .filter_map(|guard| {
                let (key, value) = guard.into_inner().ok()?;
                let epoch = Self::parse_epoch_key(&key)?;
                if epoch < from_epoch {
                    return None;
                }
                // Parse the value
                if value.len() < 4 {
                    return None;
                }
                let proposal_len = u32::from_be_bytes(value[0..4].try_into().ok()?) as usize;
                if value.len() < 4 + proposal_len {
                    return None;
                }
                let proposal = Self::deserialize_proposal(&value[4..4 + proposal_len])?;
                let message = Self::deserialize_message(&value[4 + proposal_len..])?;
                Some((proposal, message))
            })
            .collect()
    }

    /// Get the highest accepted round (synchronous)
    fn highest_accepted_round_sync(&self) -> Option<Epoch> {
        // Use reverse iterator to get the last (highest) key
        self.accepted
            .range::<Vec<u8>, _>(..)
            .next_back()
            .and_then(|guard| {
                let (key, _) = guard.into_inner().ok()?;
                Self::parse_epoch_key(&key)
            })
    }
}

/// Wrapper to make FjallStateStore shareable
pub struct SharedFjallStateStore {
    inner: Arc<FjallStateStore>,
}

impl SharedFjallStateStore {
    /// Open or create a new state store at the given path
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let store = FjallStateStore::open(path).await?;
        Ok(Self {
            inner: Arc::new(store),
        })
    }
}

impl Clone for SharedFjallStateStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Learner implementation for the state store
///
/// This is a minimal implementation to satisfy the trait bounds.
/// The actual MLS processing happens in GroupAcceptor.
pub struct FjallLearner;

impl Learner for FjallLearner {
    type Proposal = GroupProposal;
    type Message = GroupMessage;
    type Error = std::io::Error;
    type AcceptorId = crate::proposal::AcceptorId;

    fn node_id(&self) -> crate::proposal::MemberId {
        crate::proposal::MemberId(u32::MAX)
    }

    fn current_round(&self) -> Epoch {
        Epoch(0)
    }

    fn validate(&self, _proposal: &GroupProposal) -> bool {
        true
    }

    async fn apply(
        &mut self,
        _proposal: GroupProposal,
        _message: GroupMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Broadcast receiver wrapper
pub struct FjallReceiver {
    inner: tokio_stream::wrappers::BroadcastStream<(GroupProposal, GroupMessage)>,
}

impl Stream for FjallReceiver {
    type Item = (GroupProposal, GroupMessage);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match std::task::ready!(Pin::new(&mut self.get_mut().inner).poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(Some(item)),
            _ => Poll::Ready(None),
        }
    }
}

/// Historical stream type for fjall
pub type FjallHistoricalStream = stream::Iter<std::vec::IntoIter<(GroupProposal, GroupMessage)>>;

/// Combined subscription stream
pub type FjallSubscription = stream::Chain<FjallHistoricalStream, FjallReceiver>;

impl AcceptorStateStore<FjallLearner> for SharedFjallStateStore {
    type Subscription = FjallSubscription;

    fn get(&self, round: Epoch) -> RoundState<FjallLearner> {
        let promised = self.inner.get_promised_sync(round);
        let accepted = self.inner.get_accepted_sync(round);

        RoundState { promised, accepted }
    }

    fn promise(&self, proposal: &GroupProposal) -> Result<(), RoundState<FjallLearner>> {
        let epoch = proposal.epoch;
        let key = proposal.key();

        // Check current state
        let current_promised = self.inner.get_promised_sync(epoch);
        let current_accepted = self.inner.get_accepted_sync(epoch);

        // Reject if a higher proposal was already promised
        if let Some(ref promised) = current_promised
            && promised.key() >= key
        {
            return Err(RoundState {
                promised: current_promised,
                accepted: current_accepted,
            });
        }

        // Reject if a higher proposal was already accepted
        if let Some((ref accepted, _)) = current_accepted
            && accepted.key() >= key
        {
            return Err(RoundState {
                promised: current_promised,
                accepted: current_accepted,
            });
        }

        // Persist the promise
        self.inner
            .set_promised_sync(proposal)
            .map_err(|_| RoundState {
                promised: current_promised,
                accepted: current_accepted,
            })?;

        Ok(())
    }

    fn accept(
        &self,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), RoundState<FjallLearner>> {
        let epoch = proposal.epoch;
        let key = proposal.key();

        // Check current state
        let current_promised = self.inner.get_promised_sync(epoch);
        let current_accepted = self.inner.get_accepted_sync(epoch);

        // Reject if a higher proposal was already promised
        if let Some(ref promised) = current_promised
            && promised.key() > key
        {
            return Err(RoundState {
                promised: current_promised,
                accepted: current_accepted,
            });
        }

        // Reject if a higher proposal was already accepted
        if let Some((ref accepted, _)) = current_accepted
            && accepted.key() >= key
        {
            return Err(RoundState {
                promised: current_promised,
                accepted: current_accepted,
            });
        }

        // Persist the accept
        self.inner
            .set_accepted_sync(proposal, message)
            .map_err(|_| RoundState {
                promised: current_promised,
                accepted: current_accepted,
            })?;

        // Also update promised to match
        let _ = self.inner.set_promised_sync(proposal);

        // Broadcast to learners
        let _ = self
            .inner
            .broadcast
            .send((proposal.clone(), message.clone()));

        Ok(())
    }

    fn subscribe_from(&self, from_round: Epoch) -> Self::Subscription {
        // Get historical values
        let historical = self.inner.get_accepted_from_sync(from_round);

        // Create live receiver
        let live = FjallReceiver {
            inner: tokio_stream::wrappers::BroadcastStream::new(self.inner.broadcast.subscribe()),
        };

        stream::iter(historical).chain(live)
    }

    fn highest_accepted_round(&self) -> Option<Epoch> {
        self.inner.highest_accepted_round_sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proposal::{Attempt, MemberId, UnsignedProposal};

    fn test_proposal(epoch: u64, attempt: u64) -> GroupProposal {
        UnsignedProposal::for_sync(MemberId(1), Epoch(epoch), Attempt(attempt))
            .with_signature(vec![1, 2, 3])
    }

    #[tokio::test]
    async fn test_open_and_close() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();
        drop(store);
    }

    #[tokio::test]
    async fn test_promise_and_get() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();

        let proposal = test_proposal(1, 1);
        assert!(store.promise(&proposal).is_ok());

        let state = store.get(Epoch(1));
        assert!(state.promised.is_some());
        assert_eq!(state.promised.unwrap().epoch, Epoch(1));
    }

    #[tokio::test]
    async fn test_promise_rejects_lower() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();

        let proposal1 = test_proposal(1, 2);
        assert!(store.promise(&proposal1).is_ok());

        let proposal2 = test_proposal(1, 1);
        let result = store.promise(&proposal2);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_highest_accepted_round() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();

        // Initially no accepted rounds
        assert_eq!(store.highest_accepted_round(), None);

        // Accept at epoch 5
        let proposal = test_proposal(5, 1);
        assert!(store.promise(&proposal).is_ok());
        // We need a message to accept - for now just use promise
        // In a real scenario we'd have a GroupMessage

        // After promise, still no accepted
        assert_eq!(store.highest_accepted_round(), None);
    }
}
