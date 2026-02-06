//! Persistent acceptor state store using fjall.

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
use universal_sync_paxos::core::decision;
use universal_sync_paxos::{AcceptorStateStore, Learner, Proposal};

use crate::epoch_roster::EpochRoster;

type GroupBroadcasts = RwLock<HashMap<[u8; 32], broadcast::Sender<(GroupProposal, GroupMessage)>>>;

pub(crate) struct FjallStateStore {
    db: Database,
    promised: Keyspace,
    accepted: Keyspace,
    groups: Keyspace,
    messages: Keyspace,
    message_seq: Keyspace,
    epoch_rosters: Keyspace,
    broadcasts: GroupBroadcasts,
    message_broadcasts: RwLock<HashMap<[u8; 32], broadcast::Sender<EncryptedAppMessage>>>,
}

impl FjallStateStore {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

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

    fn build_key(group_id: &GroupId, epoch: Epoch) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(group_id.as_bytes());
        key[32..].copy_from_slice(&epoch.0.to_be_bytes());
        key
    }

    fn build_group_prefix(group_id: &GroupId) -> [u8; 32] {
        *group_id.as_bytes()
    }

    fn parse_epoch_from_key(key: &[u8]) -> Option<Epoch> {
        if key.len() < 40 {
            return None;
        }
        let epoch_bytes: [u8; 8] = key[32..40].try_into().ok()?;
        Some(Epoch(u64::from_be_bytes(epoch_bytes)))
    }

    fn serialize_proposal(proposal: &GroupProposal) -> Vec<u8> {
        postcard::to_allocvec(proposal).expect("serialization should not fail")
    }

    fn deserialize_proposal(bytes: &[u8]) -> Option<GroupProposal> {
        postcard::from_bytes(bytes).ok()
    }

    fn serialize_message(message: &GroupMessage) -> Vec<u8> {
        postcard::to_allocvec(message).expect("serialization should not fail")
    }

    fn deserialize_message(bytes: &[u8]) -> Option<GroupMessage> {
        postcard::from_bytes(bytes).ok()
    }

    fn build_message_key(group_id: &GroupId, seq: u64) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(group_id.as_bytes());
        key[32..].copy_from_slice(&seq.to_be_bytes());
        key
    }

    fn serialize_app_message(msg: &EncryptedAppMessage) -> Vec<u8> {
        postcard::to_allocvec(msg).expect("serialization should not fail")
    }

    fn deserialize_app_message(bytes: &[u8]) -> Option<EncryptedAppMessage> {
        postcard::from_bytes(bytes).ok()
    }

    fn next_message_seq(&self, group_id: &GroupId) -> Result<u64, fjall::Error> {
        let key = group_id.as_bytes();
        let current = self.message_seq.get(key)?.map_or(0, |bytes| {
            if bytes.len() >= 8 {
                u64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            }
        });

        self.message_seq.insert(key, (current + 1).to_be_bytes())?;

        Ok(current)
    }

    pub(crate) fn store_app_message(
        &self,
        group_id: &GroupId,
        msg: &EncryptedAppMessage,
    ) -> Result<u64, fjall::Error> {
        let seq = self.next_message_seq(group_id)?;
        let msg_key = Self::build_message_key(group_id, seq);
        let msg_bytes = Self::serialize_app_message(msg);
        self.messages.insert(msg_key, &msg_bytes)?;

        self.db.persist(PersistMode::SyncAll)?;

        if let Ok(broadcasts) = self.message_broadcasts.read()
            && let Some(tx) = broadcasts.get(group_id.as_bytes())
        {
            let _ = tx.send(msg.clone());
        }

        Ok(seq)
    }

    pub(crate) fn get_messages_since(
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

            if key.len() < 32 || &key[..32] != prefix.as_slice() {
                break;
            }

            if key.len() >= 40 {
                let seq = u64::from_be_bytes(key[32..40].try_into().unwrap_or([0; 8]));
                if let Some(msg) = Self::deserialize_app_message(&value) {
                    messages.push((seq, msg));
                }
            }
        }

        messages
    }

    pub(crate) fn subscribe_messages(
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

    fn get_promised_sync(&self, group_id: &GroupId, epoch: Epoch) -> Option<GroupProposal> {
        let key = Self::build_key(group_id, epoch);
        self.promised
            .get(key)
            .ok()
            .flatten()
            .and_then(|bytes| Self::deserialize_proposal(&bytes))
    }

    fn get_accepted_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<(GroupProposal, GroupMessage)> {
        let key = Self::build_key(group_id, epoch);
        self.accepted.get(key).ok().flatten().and_then(|bytes| {
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

    #[expect(clippy::cast_possible_truncation)]
    fn set_accepted_sync(
        &self,
        group_id: &GroupId,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), fjall::Error> {
        let key = Self::build_key(group_id, proposal.epoch);

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
                if !key.starts_with(&prefix) {
                    return None;
                }
                let epoch = Self::parse_epoch_from_key(&key)?;
                if epoch < from_epoch {
                    return None;
                }
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

    fn highest_accepted_round_sync(&self, group_id: &GroupId) -> Option<Epoch> {
        let prefix = Self::build_group_prefix(group_id);

        let mut end_key = prefix;
        for byte in end_key.iter_mut().rev() {
            if *byte < 0xFF {
                *byte += 1;
                break;
            }
            *byte = 0;
        }

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

    fn get_broadcast(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Sender<(GroupProposal, GroupMessage)> {
        if let Some(sender) = self.broadcasts.read().unwrap().get(&group_id.0) {
            return sender.clone();
        }

        let mut broadcasts = self.broadcasts.write().unwrap();
        broadcasts
            .entry(group_id.0)
            .or_insert_with(|| broadcast::channel(64).0)
            .clone()
    }

    fn store_group_sync(&self, group_id: &GroupId, group_info: &[u8]) -> Result<(), fjall::Error> {
        self.groups.insert(group_id.as_bytes(), group_info)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    fn get_group_sync(&self, group_id: &GroupId) -> Option<Vec<u8>> {
        self.groups
            .get(group_id.as_bytes())
            .ok()
            .flatten()
            .map(|slice| slice.to_vec())
    }

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

    fn remove_group_sync(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.groups.remove(group_id.as_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

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

    fn get_epoch_roster_at_or_before_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<EpochRoster> {
        let prefix = Self::build_group_prefix(group_id);
        let target_key = Self::build_key(group_id, epoch);

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

#[derive(Clone)]
pub struct SharedFjallStateStore {
    inner: Arc<FjallStateStore>,
}

impl SharedFjallStateStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let store = FjallStateStore::open(path).await?;
        Ok(Self {
            inner: Arc::new(store),
        })
    }

    #[must_use]
    pub(crate) fn for_group(&self, group_id: GroupId) -> GroupStateStore {
        GroupStateStore {
            inner: self.inner.clone(),
            group_id,
        }
    }

    pub fn store_group(&self, group_id: &GroupId, group_info: &[u8]) -> Result<(), fjall::Error> {
        self.inner.store_group_sync(group_id, group_info)
    }

    #[must_use]
    pub fn get_group_info(&self, group_id: &GroupId) -> Option<Vec<u8>> {
        self.inner.get_group_sync(group_id)
    }

    #[must_use]
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.inner.list_groups_sync()
    }

    pub fn remove_group(&self, group_id: &GroupId) -> Result<(), fjall::Error> {
        self.inner.remove_group_sync(group_id)
    }

    pub(crate) fn store_app_message(
        &self,
        group_id: &GroupId,
        msg: &EncryptedAppMessage,
    ) -> Result<u64, fjall::Error> {
        self.inner.store_app_message(group_id, msg)
    }

    #[must_use]
    pub(crate) fn get_messages_since(
        &self,
        group_id: &GroupId,
        since_seq: u64,
    ) -> Vec<(u64, EncryptedAppMessage)> {
        self.inner.get_messages_since(group_id, since_seq)
    }

    #[must_use]
    pub(crate) fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Receiver<EncryptedAppMessage> {
        self.inner.subscribe_messages(group_id)
    }

}

#[derive(Clone)]
pub struct GroupStateStore {
    inner: Arc<FjallStateStore>,
    group_id: GroupId,
}

impl GroupStateStore {
    #[must_use]
    pub(crate) fn get_accepted_from(
        &self,
        from_epoch: Epoch,
    ) -> Vec<(GroupProposal, GroupMessage)> {
        self.inner
            .get_accepted_from_sync(&self.group_id, from_epoch)
    }

    pub(crate) fn store_epoch_roster(&self, roster: &EpochRoster) -> Result<(), fjall::Error> {
        self.inner.store_epoch_roster_sync(&self.group_id, roster)
    }

    #[must_use]
    pub(crate) fn get_epoch_roster_at_or_before(&self, epoch: Epoch) -> Option<EpochRoster> {
        self.inner
            .get_epoch_roster_at_or_before_sync(&self.group_id, epoch)
    }
}

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

pub type HistoricalStream = stream::Iter<std::vec::IntoIter<(GroupProposal, GroupMessage)>>;
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

            let current_promised = inner.get_promised_sync(&group_id, epoch);
            let current_accepted = inner.get_accepted_sync(&group_id, epoch);

            let promised_key = current_promised.as_ref().map(Proposal::key);
            let accepted_key = current_accepted.as_ref().map(|(p, _)| p.key());

            if !decision::should_promise(&key, promised_key.as_ref(), accepted_key.as_ref()) {
                return Err(RoundState {
                    promised: current_promised,
                    accepted: current_accepted,
                });
            }

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

            let current_promised = inner.get_promised_sync(&group_id, epoch);
            let current_accepted = inner.get_accepted_sync(&group_id, epoch);

            let promised_key = current_promised.as_ref().map(Proposal::key);
            let accepted_key = current_accepted.as_ref().map(|(p, _)| p.key());

            if !decision::should_accept(&key, promised_key.as_ref(), accepted_key.as_ref()) {
                return Err(RoundState {
                    promised: current_promised,
                    accepted: current_accepted,
                });
            }

            inner
                .set_accepted_sync(&group_id, &proposal, &message)
                .map_err(|_| RoundState {
                    promised: current_promised.clone(),
                    accepted: current_accepted.clone(),
                })?;

            let _ = inner.get_broadcast(&group_id).send((proposal, message));

            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn subscribe_from(&self, from_round: Epoch) -> Self::Subscription {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        let historical = tokio::task::spawn_blocking(move || {
            inner.get_accepted_from_sync(&group_id, from_round)
        })
        .await
        .expect("spawn_blocking panicked");

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

    async fn get_accepted_from(&self, from_round: Epoch) -> Vec<(GroupProposal, GroupMessage)> {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        tokio::task::spawn_blocking(move || inner.get_accepted_from_sync(&group_id, from_round))
            .await
            .expect("spawn_blocking panicked")
    }
}
