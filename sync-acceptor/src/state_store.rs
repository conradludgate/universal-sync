//! Persistent acceptor state store using fjall
//!
//! This module provides a durable implementation of [`AcceptorStateStore`]
//! backed by the fjall embedded key-value store.
//!
//! The store supports multiple groups, with keys prefixed by group ID.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use futures::{Stream, StreamExt, stream};
use tokio::sync::broadcast;
use universal_sync_core::{EncryptedAppMessage, Epoch, GroupId, GroupMessage, GroupProposal};
use universal_sync_paxos::acceptor::RoundState;
use universal_sync_paxos::{AcceptorStateStore, Learner, Proposal};

use crate::epoch_roster::EpochRoster;

/// Type alias for the broadcast sender map
type GroupBroadcasts = RwLock<HashMap<[u8; 32], broadcast::Sender<(GroupProposal, GroupMessage)>>>;

/// Persistent acceptor state store backed by fjall
///
/// This implementation persists all state changes to disk before returning,
/// ensuring crash recovery is possible.
///
/// Supports multiple groups - keys are prefixed with group ID.
///
/// Uses separate keyspaces:
/// - `promised`: (`group_id`, epoch) -> proposal
/// - `accepted`: (`group_id`, epoch) -> (proposal, message)
/// - `groups`: `group_id` -> `GroupInfo` bytes (for registry persistence)
/// - `messages`: (`group_id`, `arrival_seq`) -> `EncryptedAppMessage`
/// - `message_seq`: `group_id` -> next sequence number
/// - `epoch_rosters`: (`group_id`, epoch) -> `EpochRoster` (for signature validation)
pub struct FjallStateStore {
    /// The fjall database
    db: Database,
    /// Keyspace for promised proposals
    promised: Keyspace,
    /// Keyspace for accepted values
    accepted: Keyspace,
    /// Keyspace for registered groups (`GroupInfo` bytes)
    groups: Keyspace,
    /// Keyspace for application messages: (`group_id`, `arrival_seq`) -> message
    messages: Keyspace,
    /// Keyspace for per-group sequence counters: `group_id` -> next seq
    message_seq: Keyspace,
    /// Keyspace for epoch rosters: (`group_id`, epoch) -> `EpochRoster`
    epoch_rosters: Keyspace,
    /// Per-group broadcast channels for live subscriptions (proposals)
    broadcasts: GroupBroadcasts,
    /// Per-group broadcast channels for application messages
    message_broadcasts: RwLock<HashMap<[u8; 32], broadcast::Sender<EncryptedAppMessage>>>,
}

impl FjallStateStore {
    /// Open or create a new state store at the given path
    ///
    /// # Errors
    /// Returns an error if the database cannot be opened.
    ///
    /// # Panics
    /// Panics if the spawned blocking task panics.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_owned();

        // Use spawn_blocking for the synchronous fjall operations
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

    /// Synchronous open for use in `spawn_blocking`
    fn open_sync(path: &Path) -> Result<Self, fjall::Error> {
        let db = Database::builder(path).open()?;

        let promised = db.keyspace("promised", KeyspaceCreateOptions::default)?;
        let accepted = db.keyspace("accepted", KeyspaceCreateOptions::default)?;
        let groups = db.keyspace("groups", KeyspaceCreateOptions::default)?;
        let messages = db.keyspace("messages", KeyspaceCreateOptions::default)?;
        let message_seq = db.keyspace("message_seq", KeyspaceCreateOptions::default)?;
        let epoch_rosters = db.keyspace("epoch_rosters", KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            promised,
            accepted,
            groups,
            messages,
            message_seq,
            epoch_rosters,
            broadcasts: RwLock::new(HashMap::new()),
            message_broadcasts: RwLock::new(HashMap::new()),
        })
    }

    /// Build a key from (`group_id`, epoch)
    ///
    /// Format: `[group_id: 32 bytes][epoch: 8 bytes BE]` = 40 bytes total
    fn build_key(group_id: &GroupId, epoch: Epoch) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(group_id.as_bytes());
        key[32..].copy_from_slice(&epoch.0.to_be_bytes());
        key
    }

    /// Build a prefix for range queries on a group (just the `group_id`)
    fn build_group_prefix(group_id: &GroupId) -> [u8; 32] {
        *group_id.as_bytes()
    }

    /// Parse epoch from a key (assumes key was created by `build_key`)
    fn parse_epoch_from_key(key: &[u8]) -> Option<Epoch> {
        if key.len() < 40 {
            return None;
        }
        let epoch_bytes: [u8; 8] = key[32..40].try_into().ok()?;
        Some(Epoch(u64::from_be_bytes(epoch_bytes)))
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

    // --- Application Message Helpers ---

    /// Build a key for application messages: `group_id` + `arrival_seq`
    fn build_message_key(group_id: &GroupId, seq: u64) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(group_id.as_bytes());
        key[32..].copy_from_slice(&seq.to_be_bytes());
        key
    }

    /// Serialize an encrypted app message for storage
    fn serialize_app_message(msg: &EncryptedAppMessage) -> Vec<u8> {
        postcard::to_allocvec(msg).expect("serialization should not fail")
    }

    /// Deserialize an encrypted app message from storage
    fn deserialize_app_message(bytes: &[u8]) -> Option<EncryptedAppMessage> {
        postcard::from_bytes(bytes).ok()
    }

    /// Get the next sequence number for a group and increment it
    fn next_message_seq(&self, group_id: &GroupId) -> Result<u64, fjall::Error> {
        let key = group_id.as_bytes();

        // Get current value or default to 0
        let current = self.message_seq.get(key)?.map_or(0, |bytes| {
            if bytes.len() >= 8 {
                u64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            }
        });

        // Store incremented value
        self.message_seq.insert(key, (current + 1).to_be_bytes())?;

        Ok(current)
    }

    /// Store an application message.
    ///
    /// Returns the arrival sequence number for the message.
    /// Deduplication is handled by clients, not the acceptor.
    ///
    /// # Errors
    /// Returns an error if storage operations fail.
    pub fn store_app_message(
        &self,
        group_id: &GroupId,
        msg: &EncryptedAppMessage,
    ) -> Result<u64, fjall::Error> {
        // Get next sequence number
        let seq = self.next_message_seq(group_id)?;

        // Store the message
        let msg_key = Self::build_message_key(group_id, seq);
        let msg_bytes = Self::serialize_app_message(msg);
        self.messages.insert(msg_key, &msg_bytes)?;

        // Persist
        self.db.persist(PersistMode::SyncAll)?;

        // Broadcast to subscribers
        if let Ok(broadcasts) = self.message_broadcasts.read()
            && let Some(tx) = broadcasts.get(group_id.as_bytes())
        {
            let _ = tx.send(msg.clone());
        }

        Ok(seq)
    }

    /// Get messages for a group starting from a sequence number
    pub fn get_messages_since(
        &self,
        group_id: &GroupId,
        since_seq: u64,
    ) -> Vec<(u64, EncryptedAppMessage)> {
        let start_key = Self::build_message_key(group_id, since_seq);
        let prefix = Self::build_group_prefix(group_id);

        let mut messages = Vec::new();

        for guard in self.messages.range(start_key..) {
            let Ok((key, value)) = guard.into_inner() else {
                continue;
            };

            // Check if still in the same group
            if key.len() < 32 || &key[..32] != prefix.as_slice() {
                break;
            }

            // Parse sequence from key
            if key.len() >= 40 {
                let seq = u64::from_be_bytes(key[32..40].try_into().unwrap_or([0; 8]));
                if let Some(msg) = Self::deserialize_app_message(&value) {
                    messages.push((seq, msg));
                }
            }
        }

        messages
    }

    /// Subscribe to new application messages for a group
    ///
    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Receiver<EncryptedAppMessage> {
        let mut broadcasts = self.message_broadcasts.write().unwrap();
        let key = *group_id.as_bytes();

        broadcasts
            .entry(key)
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    /// Get promised proposal for a (group, round) (synchronous)
    fn get_promised_sync(&self, group_id: &GroupId, epoch: Epoch) -> Option<GroupProposal> {
        let key = Self::build_key(group_id, epoch);
        self.promised
            .get(key)
            .ok()
            .flatten()
            .and_then(|bytes| Self::deserialize_proposal(&bytes))
    }

    /// Get accepted (proposal, message) for a (group, round) (synchronous)
    fn get_accepted_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<(GroupProposal, GroupMessage)> {
        let key = Self::build_key(group_id, epoch);
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

    /// Set promised proposal for a (group, round) (synchronous)
    fn set_promised_sync(
        &self,
        group_id: &GroupId,
        proposal: &GroupProposal,
    ) -> Result<(), fjall::Error> {
        let key = Self::build_key(group_id, proposal.epoch);
        let value = Self::serialize_proposal(proposal);
        self.promised.insert(key, &value)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Set accepted (proposal, message) for a (group, round) (synchronous)
    #[expect(clippy::cast_possible_truncation)]
    fn set_accepted_sync(
        &self,
        group_id: &GroupId,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), fjall::Error> {
        let key = Self::build_key(group_id, proposal.epoch);

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

    /// Get all accepted values for a group from a given round onwards (synchronous)
    fn get_accepted_from_sync(
        &self,
        group_id: &GroupId,
        from_epoch: Epoch,
    ) -> Vec<(GroupProposal, GroupMessage)> {
        let start_key = Self::build_key(group_id, from_epoch);
        let prefix = Self::build_group_prefix(group_id);

        self.accepted
            .range(start_key..)
            .filter_map(|guard| {
                let (key, value) = guard.into_inner().ok()?;
                // Stop if we've left this group's prefix
                if !key.starts_with(&prefix) {
                    return None;
                }
                let epoch = Self::parse_epoch_from_key(&key)?;
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

    /// Get the highest accepted round for a group (synchronous)
    fn highest_accepted_round_sync(&self, group_id: &GroupId) -> Option<Epoch> {
        let prefix = Self::build_group_prefix(group_id);

        // Build end key: group_id with last byte incremented
        let mut end_key = prefix;
        // Find the first non-0xFF byte from the end and increment it
        for byte in end_key.iter_mut().rev() {
            if *byte < 0xFF {
                *byte += 1;
                break;
            }
            *byte = 0;
        }

        // Iterate through all entries for this group, tracking the highest epoch
        let mut highest: Option<Epoch> = None;
        for guard in self.accepted.range(prefix..end_key) {
            if let Ok((key, _)) = guard.into_inner()
                && let Some(epoch) = Self::parse_epoch_from_key(&key)
            {
                highest = Some(epoch);
            }
        }
        highest
    }

    /// Get or create the broadcast channel for a group
    fn get_broadcast(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Sender<(GroupProposal, GroupMessage)> {
        // Try read lock first
        if let Some(sender) = self.broadcasts.read().unwrap().get(&group_id.0) {
            return sender.clone();
        }

        // Need to create - use write lock
        let mut broadcasts = self.broadcasts.write().unwrap();
        broadcasts
            .entry(group_id.0)
            .or_insert_with(|| broadcast::channel(64).0)
            .clone()
    }

    // ========== Group Registry Methods ==========

    /// Store a group's `GroupInfo` bytes (synchronous)
    fn store_group_sync(&self, group_id: &GroupId, group_info: &[u8]) -> Result<(), fjall::Error> {
        self.groups.insert(group_id.as_bytes(), group_info)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Get a group's `GroupInfo` bytes (synchronous)
    fn get_group_sync(&self, group_id: &GroupId) -> Option<Vec<u8>> {
        self.groups
            .get(group_id.as_bytes())
            .ok()
            .flatten()
            .map(|slice| slice.to_vec())
    }

    /// List all registered group IDs (synchronous)
    fn list_groups_sync(&self) -> Vec<GroupId> {
        self.groups
            .iter()
            .filter_map(|guard| {
                let (key, _) = guard.into_inner().ok()?;
                if key.len() == 32 {
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(&key);
                    Some(GroupId::new(bytes))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove a group (synchronous)
    fn remove_group_sync(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.groups.remove(group_id.as_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    // ========== Epoch Roster Methods ==========

    /// Store an epoch roster snapshot
    fn store_epoch_roster_sync(
        &self,
        group_id: &GroupId,
        roster: &EpochRoster,
    ) -> Result<(), fjall::Error> {
        let key = Self::build_key(group_id, roster.epoch);
        self.epoch_rosters.insert(key, roster.to_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Get an epoch roster snapshot
    fn get_epoch_roster_sync(&self, group_id: &GroupId, epoch: Epoch) -> Option<EpochRoster> {
        let key = Self::build_key(group_id, epoch);
        self.epoch_rosters
            .get(key)
            .ok()
            .flatten()
            .and_then(|bytes| EpochRoster::from_bytes(&bytes))
    }

    /// Get the closest epoch roster at or before the given epoch
    ///
    /// This is useful when we don't have a snapshot for the exact epoch
    /// but need to validate against the closest prior state.
    fn get_epoch_roster_at_or_before_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<EpochRoster> {
        let prefix = Self::build_group_prefix(group_id);
        let target_key = Self::build_key(group_id, epoch);

        // Scan backwards from the target epoch
        for guard in self
            .epoch_rosters
            .range(prefix.as_slice()..=target_key.as_slice())
            .rev()
        {
            if let Ok((_, value)) = guard.into_inner()
                && let Some(roster) = EpochRoster::from_bytes(&value)
            {
                return Some(roster);
            }
        }
        None
    }
}

/// Wrapper to make `FjallStateStore` shareable across groups
#[derive(Clone)]
pub struct SharedFjallStateStore {
    inner: Arc<FjallStateStore>,
}

impl SharedFjallStateStore {
    /// Open or create a new state store at the given path
    ///
    /// # Errors
    /// Returns an error if the database cannot be opened.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let store = FjallStateStore::open(path).await?;
        Ok(Self {
            inner: Arc::new(store),
        })
    }

    /// Get a per-group state store view
    #[must_use]
    pub fn for_group(&self, group_id: GroupId) -> GroupStateStore {
        GroupStateStore {
            inner: self.inner.clone(),
            group_id,
        }
    }

    // ========== Group Registry Methods ==========

    /// Store a group's `GroupInfo` bytes
    ///
    /// This persists the `GroupInfo` so the group can be restored after restart.
    ///
    /// # Errors
    /// Returns an error if persisting to the database fails.
    pub fn store_group(&self, group_id: &GroupId, group_info: &[u8]) -> Result<(), fjall::Error> {
        self.inner.store_group_sync(group_id, group_info)
    }

    /// Get a group's `GroupInfo` bytes
    ///
    /// Returns `None` if the group is not registered.
    #[must_use]
    pub fn get_group_info(&self, group_id: &GroupId) -> Option<Vec<u8>> {
        self.inner.get_group_sync(group_id)
    }

    /// List all registered group IDs
    #[must_use]
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.inner.list_groups_sync()
    }

    /// Remove a group registration
    ///
    /// # Errors
    /// Returns an error if removing from the database fails.
    pub fn remove_group(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.inner.remove_group_sync(group_id)
    }

    // ========== Application Message Methods ==========

    /// Store an application message.
    ///
    /// # Errors
    /// Returns an error if persisting to the database fails.
    pub fn store_app_message(
        &self,
        group_id: &GroupId,
        msg: &EncryptedAppMessage,
    ) -> Result<u64, fjall::Error> {
        self.inner.store_app_message(group_id, msg)
    }

    /// Get messages since a sequence number.
    #[must_use]
    pub fn get_messages_since(
        &self,
        group_id: &GroupId,
        since_seq: u64,
    ) -> Vec<(u64, EncryptedAppMessage)> {
        self.inner.get_messages_since(group_id, since_seq)
    }

    /// Subscribe to new messages for a group.
    #[must_use]
    pub fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Receiver<EncryptedAppMessage> {
        self.inner.subscribe_messages(group_id)
    }

    // ========== Epoch Roster Methods ==========

    /// Store an epoch roster snapshot
    ///
    /// # Errors
    /// Returns an error if persisting to the database fails.
    pub fn store_epoch_roster(
        &self,
        group_id: &GroupId,
        roster: &EpochRoster,
    ) -> Result<(), fjall::Error> {
        self.inner.store_epoch_roster_sync(group_id, roster)
    }

    /// Get an epoch roster snapshot for a specific epoch
    #[must_use]
    pub fn get_epoch_roster(&self, group_id: &GroupId, epoch: Epoch) -> Option<EpochRoster> {
        self.inner.get_epoch_roster_sync(group_id, epoch)
    }

    /// Get the closest epoch roster at or before the given epoch
    #[must_use]
    pub fn get_epoch_roster_at_or_before(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<EpochRoster> {
        self.inner
            .get_epoch_roster_at_or_before_sync(group_id, epoch)
    }
}

/// Per-group view of the state store
///
/// This wraps a [`SharedFjallStateStore`] with a specific group ID,
/// implementing [`AcceptorStateStore`] for that group.
#[derive(Clone)]
pub struct GroupStateStore {
    inner: Arc<FjallStateStore>,
    group_id: GroupId,
}

impl GroupStateStore {
    /// Get the group ID
    #[must_use]
    pub fn group_id(&self) -> &GroupId {
        &self.group_id
    }

    /// Get all accepted messages from the given epoch onwards
    ///
    /// This is useful for replaying messages to catch up a newly created acceptor.
    #[must_use]
    pub fn get_accepted_from(&self, from_epoch: Epoch) -> Vec<(GroupProposal, GroupMessage)> {
        self.inner
            .get_accepted_from_sync(&self.group_id, from_epoch)
    }

    /// Store an epoch roster snapshot for this group
    ///
    /// # Errors
    /// Returns an error if persisting to the database fails.
    pub fn store_epoch_roster(&self, roster: &EpochRoster) -> Result<(), fjall::Error> {
        self.inner.store_epoch_roster_sync(&self.group_id, roster)
    }

    /// Get an epoch roster snapshot for a specific epoch
    #[must_use]
    pub fn get_epoch_roster(&self, epoch: Epoch) -> Option<EpochRoster> {
        self.inner.get_epoch_roster_sync(&self.group_id, epoch)
    }

    /// Get the closest epoch roster at or before the given epoch
    #[must_use]
    pub fn get_epoch_roster_at_or_before(&self, epoch: Epoch) -> Option<EpochRoster> {
        self.inner
            .get_epoch_roster_at_or_before_sync(&self.group_id, epoch)
    }
}

/// Learner implementation for the state store
///
/// This is a minimal implementation to satisfy the trait bounds.
/// The actual MLS processing happens in `GroupAcceptor`.
pub struct FjallLearner;

impl Learner for FjallLearner {
    type Proposal = GroupProposal;
    type Message = GroupMessage;
    type Error = std::io::Error;
    type AcceptorId = universal_sync_core::AcceptorId;

    fn node_id(&self) -> universal_sync_core::MemberId {
        universal_sync_core::MemberId(u32::MAX)
    }

    fn current_round(&self) -> Epoch {
        Epoch(0)
    }

    fn acceptors(&self) -> impl IntoIterator<Item = Self::AcceptorId, IntoIter: ExactSizeIterator> {
        std::iter::empty()
    }

    fn propose(&self, attempt: universal_sync_core::Attempt) -> GroupProposal {
        // FjallLearner is just a marker type for the state store
        // It doesn't actually propose anything
        universal_sync_core::UnsignedProposal::new(
            universal_sync_core::MemberId(u32::MAX),
            Epoch(0),
            attempt,
            [0u8; 32],
        )
        .with_signature(vec![])
    }

    fn validate(
        &self,
        _proposal: &GroupProposal,
    ) -> Result<
        universal_sync_paxos::Validated,
        error_stack::Report<universal_sync_paxos::ValidationError>,
    > {
        // Internal learner for state store - always accepts
        Ok(universal_sync_paxos::Validated::assert_valid())
    }

    async fn apply(
        &mut self,
        _proposal: GroupProposal,
        _message: GroupMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Broadcast receiver wrapper that filters by group
pub struct GroupReceiver {
    inner: tokio_stream::wrappers::BroadcastStream<(GroupProposal, GroupMessage)>,
}

impl Stream for GroupReceiver {
    type Item = (GroupProposal, GroupMessage);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match std::task::ready!(Pin::new(&mut self.get_mut().inner).poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(Some(item)),
            _ => Poll::Ready(None),
        }
    }
}

/// Historical stream type
pub type HistoricalStream = stream::Iter<std::vec::IntoIter<(GroupProposal, GroupMessage)>>;

/// Combined subscription stream
pub type GroupSubscription = stream::Chain<HistoricalStream, GroupReceiver>;

impl<L> AcceptorStateStore<L> for GroupStateStore
where
    L: Learner<Proposal = GroupProposal, Message = GroupMessage>,
{
    type Subscription = GroupSubscription;

    async fn get(&self, round: Epoch) -> RoundState<L> {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        tokio::task::spawn_blocking(move || {
            let promised = inner.get_promised_sync(&group_id, round);
            let accepted = inner.get_accepted_sync(&group_id, round);
            RoundState { promised, accepted }
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn promise(&self, proposal: &GroupProposal) -> Result<(), RoundState<L>> {
        let inner = self.inner.clone();
        let group_id = self.group_id;
        let proposal = proposal.clone();

        tokio::task::spawn_blocking(move || {
            let epoch = proposal.epoch;
            let key = proposal.key();

            // Check current state
            let current_promised = inner.get_promised_sync(&group_id, epoch);
            let current_accepted = inner.get_accepted_sync(&group_id, epoch);

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
            inner
                .set_promised_sync(&group_id, &proposal)
                .map_err(|_| RoundState {
                    promised: current_promised,
                    accepted: current_accepted,
                })?;

            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn accept(
        &self,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), RoundState<L>> {
        let inner = self.inner.clone();
        let group_id = self.group_id;
        let proposal = proposal.clone();
        let message = message.clone();

        tokio::task::spawn_blocking(move || {
            let epoch = proposal.epoch;
            let key = proposal.key();

            // Check current state
            let current_promised = inner.get_promised_sync(&group_id, epoch);
            let current_accepted = inner.get_accepted_sync(&group_id, epoch);

            // Require exact promise match - no leader optimization
            // Accept only succeeds if this exact proposal was promised
            let not_promised = current_promised.as_ref().is_none_or(|p| p.key() != key);
            if not_promised {
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
            inner
                .set_accepted_sync(&group_id, &proposal, &message)
                .map_err(|_| RoundState {
                    promised: current_promised.clone(),
                    accepted: current_accepted.clone(),
                })?;

            // Broadcast to learners for this group
            let _ = inner.get_broadcast(&group_id).send((proposal, message));

            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn subscribe_from(&self, from_round: Epoch) -> Self::Subscription {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        // Get historical values in a blocking task
        let historical = tokio::task::spawn_blocking(move || {
            inner.get_accepted_from_sync(&group_id, from_round)
        })
        .await
        .expect("spawn_blocking panicked");

        // Create live receiver (this is just channel subscription, no blocking IO)
        let live = GroupReceiver {
            inner: tokio_stream::wrappers::BroadcastStream::new(
                self.inner.get_broadcast(&self.group_id).subscribe(),
            ),
        };

        stream::iter(historical).chain(live)
    }

    async fn highest_accepted_round(&self) -> Option<Epoch> {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        tokio::task::spawn_blocking(move || inner.highest_accepted_round_sync(&group_id))
            .await
            .expect("spawn_blocking panicked")
    }
}

#[cfg(test)]
mod tests {
    use universal_sync_core::{Attempt, MemberId, UnsignedProposal};

    use super::*;

    fn test_proposal(epoch: u64, attempt: u64) -> GroupProposal {
        UnsignedProposal::for_sync(MemberId(1), Epoch(epoch), Attempt(attempt))
            .with_signature(vec![1, 2, 3])
    }

    fn test_group_id(id: u8) -> GroupId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        GroupId::new(bytes)
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
        let shared = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();
        let store = shared.for_group(test_group_id(1));

        let proposal = test_proposal(1, 1);
        assert!(
            AcceptorStateStore::<FjallLearner>::promise(&store, &proposal)
                .await
                .is_ok()
        );

        let state = AcceptorStateStore::<FjallLearner>::get(&store, Epoch(1)).await;
        assert!(state.promised.is_some());
        assert_eq!(state.promised.unwrap().epoch, Epoch(1));
    }

    #[tokio::test]
    async fn test_promise_rejects_lower() {
        let temp_dir = tempfile::tempdir().unwrap();
        let shared = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();
        let store = shared.for_group(test_group_id(1));

        let proposal1 = test_proposal(1, 2);
        assert!(
            AcceptorStateStore::<FjallLearner>::promise(&store, &proposal1)
                .await
                .is_ok()
        );

        let proposal2 = test_proposal(1, 1);
        let result = AcceptorStateStore::<FjallLearner>::promise(&store, &proposal2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_groups_are_isolated() {
        let temp_dir = tempfile::tempdir().unwrap();
        let shared = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();

        let store1 = shared.for_group(test_group_id(1));
        let store2 = shared.for_group(test_group_id(2));

        // Promise in group 1
        let proposal = test_proposal(1, 1);
        assert!(
            AcceptorStateStore::<FjallLearner>::promise(&store1, &proposal)
                .await
                .is_ok()
        );

        // Group 2 should not see it
        let state = AcceptorStateStore::<FjallLearner>::get(&store2, Epoch(1)).await;
        assert!(state.promised.is_none());

        // Group 1 should see it
        let state = AcceptorStateStore::<FjallLearner>::get(&store1, Epoch(1)).await;
        assert!(state.promised.is_some());
    }

    #[tokio::test]
    async fn test_highest_accepted_round() {
        let temp_dir = tempfile::tempdir().unwrap();
        let shared = SharedFjallStateStore::open(temp_dir.path()).await.unwrap();
        let store = shared.for_group(test_group_id(1));

        // Initially no accepted rounds
        assert_eq!(
            AcceptorStateStore::<FjallLearner>::highest_accepted_round(&store).await,
            None
        );

        // Accept at epoch 5 (need a proper message)
        // For now just test with promise
        let proposal = test_proposal(5, 1);
        assert!(
            AcceptorStateStore::<FjallLearner>::promise(&store, &proposal)
                .await
                .is_ok()
        );

        // After promise, still no accepted
        assert_eq!(
            AcceptorStateStore::<FjallLearner>::highest_accepted_round(&store).await,
            None
        );
    }
}
