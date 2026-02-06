//! Persistent acceptor state store using fjall.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use universal_sync_core::{
    EncryptedAppMessage, Epoch, GroupId, GroupMessage, GroupProposal, MemberFingerprint, MessageId,
    StateVector,
};
use universal_sync_paxos::acceptor::RoundState;
use universal_sync_paxos::core::decision;
use universal_sync_paxos::{AcceptorStateStore, Learner, Proposal};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EpochRoster {
    pub(crate) epoch: Epoch,
    pub(crate) members: Vec<(universal_sync_core::MemberId, Vec<u8>)>,
}

impl EpochRoster {
    pub(crate) fn new(
        epoch: Epoch,
        members: impl IntoIterator<Item = (universal_sync_core::MemberId, Vec<u8>)>,
    ) -> Self {
        Self {
            epoch,
            members: members.into_iter().collect(),
        }
    }

    #[must_use]
    pub(crate) fn get_member_key(&self, member_id: universal_sync_core::MemberId) -> Option<&[u8]> {
        self.members
            .iter()
            .find(|(id, _)| *id == member_id)
            .map(|(_, key)| key.as_slice())
    }

    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialization should not fail")
    }

    #[must_use]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

type GroupBroadcasts = RwLock<HashMap<[u8; 32], broadcast::Sender<(GroupProposal, GroupMessage)>>>;

pub(crate) struct FjallStateStore {
    db: Database,
    promised: Keyspace,
    accepted: Keyspace,
    groups: Keyspace,
    messages: Keyspace,
    epoch_rosters: Keyspace,
    broadcasts: GroupBroadcasts,
    message_broadcasts: RwLock<HashMap<[u8; 32], broadcast::Sender<(MessageId, EncryptedAppMessage)>>>,
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
        let epoch_rosters = db.keyspace("epoch_rosters", KeyspaceCreateOptions::default)?;

        Ok(Self {
            db,
            promised,
            accepted,
            groups,
            messages,
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

    /// Message key: group_id (32) || sender_fingerprint (32) || seq (8) = 72 bytes.
    fn build_message_key(group_id: &GroupId, sender: &MemberFingerprint, seq: u64) -> [u8; 72] {
        let mut key = [0u8; 72];
        key[..32].copy_from_slice(group_id.as_bytes());
        key[32..64].copy_from_slice(sender.as_bytes());
        key[64..72].copy_from_slice(&seq.to_be_bytes());
        key
    }

    fn message_id_from_key(group_id: &GroupId, key: &[u8]) -> Option<MessageId> {
        if key.len() < 72 {
            return None;
        }
        let mut fp = [0u8; 32];
        fp.copy_from_slice(&key[32..64]);
        let seq = u64::from_be_bytes(key[64..72].try_into().ok()?);
        Some(MessageId {
            group_id: *group_id,
            sender: MemberFingerprint(fp),
            seq,
        })
    }

    fn serialize_app_message(msg: &EncryptedAppMessage) -> Vec<u8> {
        postcard::to_allocvec(msg).expect("serialization should not fail")
    }

    fn deserialize_app_message(bytes: &[u8]) -> Option<EncryptedAppMessage> {
        postcard::from_bytes(bytes).ok()
    }

    pub(crate) fn store_app_message(
        &self,
        group_id: &GroupId,
        id: &MessageId,
        msg: &EncryptedAppMessage,
    ) -> Result<(), fjall::Error> {
        let msg_key = Self::build_message_key(group_id, &id.sender, id.seq);
        let msg_bytes = Self::serialize_app_message(msg);
        self.messages.insert(msg_key, &msg_bytes)?;

        self.db.persist(PersistMode::SyncAll)?;

        if let Ok(broadcasts) = self.message_broadcasts.read()
            && let Some(tx) = broadcasts.get(group_id.as_bytes())
        {
            let _ = tx.send((*id, msg.clone()));
        }

        Ok(())
    }

    pub(crate) fn get_messages_after(
        &self,
        group_id: &GroupId,
        state_vector: &StateVector,
    ) -> Vec<(MessageId, EncryptedAppMessage)> {
        let prefix = Self::build_group_prefix(group_id);
        let start_key = prefix;

        let mut messages = Vec::new();

        for guard in self.messages.range(start_key..) {
            let Ok((key, value)) = guard.into_inner() else {
                continue;
            };

            if key.len() < 32 || &key[..32] != prefix.as_slice() {
                break;
            }

            if let Some(msg_id) = Self::message_id_from_key(group_id, &key) {
                let covered = state_vector
                    .get(&msg_id.sender)
                    .is_some_and(|&hw| msg_id.seq <= hw);
                if !covered {
                    if let Some(msg) = Self::deserialize_app_message(&value) {
                        messages.push((msg_id, msg));
                    }
                }
            }
        }

        messages
    }

    pub(crate) fn delete_before_watermark(
        &self,
        group_id: &GroupId,
        watermark: &StateVector,
    ) -> Result<usize, fjall::Error> {
        let prefix = Self::build_group_prefix(group_id);
        let mut deleted = 0;

        for guard in self.messages.range(prefix..) {
            let Ok((key, _)) = guard.into_inner() else {
                continue;
            };

            if key.len() < 32 || &key[..32] != prefix.as_slice() {
                break;
            }

            if let Some(msg_id) = Self::message_id_from_key(group_id, &key) {
                if watermark
                    .get(&msg_id.sender)
                    .is_some_and(|&hw| msg_id.seq <= hw)
                {
                    self.messages.remove(&*key)?;
                    deleted += 1;
                }
            }
        }

        if deleted > 0 {
            self.db.persist(PersistMode::SyncAll)?;
        }

        Ok(deleted)
    }

    pub(crate) fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Receiver<(MessageId, EncryptedAppMessage)> {
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
        id: &MessageId,
        msg: &EncryptedAppMessage,
    ) -> Result<(), fjall::Error> {
        self.inner.store_app_message(group_id, id, msg)
    }

    #[must_use]
    pub(crate) fn get_messages_after(
        &self,
        group_id: &GroupId,
        state_vector: &StateVector,
    ) -> Vec<(MessageId, EncryptedAppMessage)> {
        self.inner.get_messages_after(group_id, state_vector)
    }

    pub(crate) fn delete_before_watermark(
        &self,
        group_id: &GroupId,
        watermark: &StateVector,
    ) -> Result<usize, fjall::Error> {
        self.inner.delete_before_watermark(group_id, watermark)
    }

    #[must_use]
    pub(crate) fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> broadcast::Receiver<(MessageId, EncryptedAppMessage)> {
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

    /// Delete all messages for this group whose (sender, seq) is covered by the watermark.
    pub(crate) fn delete_before_watermark(
        &self,
        watermark: &StateVector,
    ) -> Result<usize, fjall::Error> {
        self.inner
            .delete_before_watermark(&self.group_id, watermark)
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

#[cfg(test)]
mod tests {
    use universal_sync_core::MemberId;

    use super::*;

    #[test]
    fn test_epoch_roster_roundtrip() {
        let roster = EpochRoster::new(
            Epoch(5),
            vec![
                (MemberId(0), vec![1, 2, 3, 4]),
                (MemberId(1), vec![5, 6, 7, 8]),
            ],
        );

        let bytes = roster.to_bytes();
        let decoded = EpochRoster::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.epoch, Epoch(5));
        assert_eq!(decoded.get_member_key(MemberId(0)), Some(&[1, 2, 3, 4][..]));
        assert_eq!(decoded.get_member_key(MemberId(1)), Some(&[5, 6, 7, 8][..]));
        assert_eq!(decoded.get_member_key(MemberId(2)), None);
    }
}
