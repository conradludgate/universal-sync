//! Persistent acceptor state store using fjall.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use filament_core::{
    Attempt, EncryptedAppMessage, Epoch, GroupId, GroupMessage, GroupProposal, MemberFingerprint,
    MemberId, MessageId, StateVector,
};
use filament_warp::acceptor::RoundState;
use filament_warp::core::decision;
use filament_warp::{AcceptorStateStore, Learner, Proposal};
use fjall::{AbstractTree, Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use futures::Stream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

const STORAGE_MAGIC: [u8; 2] = [0xFF, 0xFE];
const STORAGE_VERSION: u8 = 1;

fn storage_versioned_encode(version: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + payload.len());
    out.extend_from_slice(&STORAGE_MAGIC);
    out.push(version);
    out.extend_from_slice(payload);
    out
}

fn storage_versioned_decode(bytes: &[u8]) -> (u8, &[u8]) {
    if bytes.len() >= 3 && bytes[..2] == STORAGE_MAGIC {
        (bytes[2], &bytes[3..])
    } else {
        (1, bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlimAccepted {
    member_id: MemberId,
    attempt: Attempt,
    signature: Vec<u8>,
    message: GroupMessage,
}

const PROMISED_SENTINEL_EPOCH: u64 = u64::MAX;

fn group_keyspace_prefix(group_id: &GroupId) -> String {
    bs58::encode(group_id.as_bytes()).into_string()
}

fn keyspace_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(fjall::config::CompressionPolicy::disabled())
}

#[derive(Clone)]
struct GroupKeyspaces {
    accepted: Keyspace,
    messages: Keyspace,
    snapshots: Keyspace,
}

/// Returns `disk_space` + active memtable size for a keyspace.
/// `Keyspace::disk_space()` only counts flushed `SSTable` segments; unflushed
/// memtable data (which can be the majority for small/new keyspaces) is excluded.
fn keyspace_data_size(ks: &Keyspace) -> u64 {
    ks.disk_space() + ks.tree.active_memtable().size()
}

impl GroupKeyspaces {
    fn open(db: &Database, group_id: &GroupId) -> Result<Self, fjall::Error> {
        let prefix = group_keyspace_prefix(group_id);
        let accepted = db.keyspace(&format!("{prefix}.accepted"), keyspace_opts)?;
        let messages = db.keyspace(&format!("{prefix}.messages"), keyspace_opts)?;
        let snapshots = db.keyspace(&format!("{prefix}.snapshots"), keyspace_opts)?;
        Ok(Self {
            accepted,
            messages,
            snapshots,
        })
    }

    pub fn disk_space(&self) -> GroupStorageSizes {
        GroupStorageSizes {
            accepted_bytes: keyspace_data_size(&self.accepted),
            messages_bytes: keyspace_data_size(&self.messages),
            snapshots_bytes: keyspace_data_size(&self.snapshots),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GroupStorageSizes {
    pub accepted_bytes: u64,
    pub messages_bytes: u64,
    pub snapshots_bytes: u64,
}

type GroupBroadcasts = RwLock<HashMap<[u8; 32], watch::Sender<Option<GroupProposal>>>>;
type MessageBroadcasts = RwLock<HashMap<[u8; 32], watch::Sender<StateVector>>>;

const SNAPSHOT_CACHE_CAPACITY: usize = 512;
const ACCEPTED_CACHE_CAPACITY: usize = 4096;
const MESSAGE_CACHE_CAPACITY: usize = 16384;

pub(crate) struct FjallStateStore {
    db: Database,
    keyspaces_cache: RwLock<HashMap<GroupId, GroupKeyspaces>>,
    broadcasts: GroupBroadcasts,
    message_broadcasts: MessageBroadcasts,
    snapshot_cache: quick_cache::sync::Cache<(GroupId, Epoch), bytes::Bytes>,
    latest_epoch: RwLock<HashMap<GroupId, Epoch>>,
    accepted_cache: quick_cache::sync::Cache<(GroupId, Epoch), Arc<(GroupProposal, GroupMessage)>>,
    message_cache: quick_cache::sync::Cache<MessageId, Arc<EncryptedAppMessage>>,
    encrypted_group_info: RwLock<HashMap<GroupId, bytes::Bytes>>,
}

impl FjallStateStore {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

    pub(crate) fn open_sync(path: &Path) -> Result<Self, fjall::Error> {
        let db = Database::builder(path).open()?;

        Ok(Self {
            db,
            keyspaces_cache: RwLock::new(HashMap::new()),
            broadcasts: RwLock::new(HashMap::new()),
            message_broadcasts: RwLock::new(HashMap::new()),
            snapshot_cache: quick_cache::sync::Cache::new(SNAPSHOT_CACHE_CAPACITY),
            latest_epoch: RwLock::new(HashMap::new()),
            accepted_cache: quick_cache::sync::Cache::new(ACCEPTED_CACHE_CAPACITY),
            message_cache: quick_cache::sync::Cache::new(MESSAGE_CACHE_CAPACITY),
            encrypted_group_info: RwLock::new(HashMap::new()),
        })
    }

    fn get_keyspaces(&self, group_id: &GroupId) -> GroupKeyspaces {
        if let Some(ks) = self.keyspaces_cache.read().unwrap().get(group_id) {
            return ks.clone();
        }

        let ks = GroupKeyspaces::open(&self.db, group_id).expect("failed to open group keyspaces");
        self.keyspaces_cache
            .write()
            .unwrap()
            .insert(*group_id, ks.clone());
        ks
    }

    fn build_epoch_key(epoch: Epoch) -> [u8; 8] {
        epoch.0.to_be_bytes()
    }

    fn parse_epoch_from_key(key: &[u8]) -> Option<Epoch> {
        let epoch_bytes: [u8; 8] = key.try_into().ok()?;
        Some(Epoch(u64::from_be_bytes(epoch_bytes)))
    }

    fn serialize_proposal(proposal: &GroupProposal) -> Vec<u8> {
        let data = postcard::to_allocvec(proposal).expect("serialization should not fail");
        storage_versioned_encode(STORAGE_VERSION, &data)
    }

    fn deserialize_proposal(bytes: &[u8]) -> Option<GroupProposal> {
        let (_, payload) = storage_versioned_decode(bytes);
        postcard::from_bytes(payload).ok()
    }

    /// Message key: `sender_fingerprint` (8) || `seq` (8) = 16 bytes.
    fn build_message_key(sender: MemberFingerprint, seq: u64) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(sender.as_bytes());
        key[8..16].copy_from_slice(&seq.to_be_bytes());
        key
    }

    fn message_id_from_key(group_id: &GroupId, key: &[u8]) -> Option<MessageId> {
        if key.len() < 16 {
            return None;
        }
        let mut fp = [0u8; 8];
        fp.copy_from_slice(&key[..8]);
        let seq = u64::from_be_bytes(key[8..16].try_into().ok()?);
        Some(MessageId {
            group_id: *group_id,
            sender: MemberFingerprint(fp),
            seq,
        })
    }

    fn serialize_app_message(msg: &EncryptedAppMessage) -> Vec<u8> {
        let data = postcard::to_allocvec(msg).expect("serialization should not fail");
        storage_versioned_encode(STORAGE_VERSION, &data)
    }

    fn deserialize_app_message(bytes: &[u8]) -> Option<EncryptedAppMessage> {
        let (_, payload) = storage_versioned_decode(bytes);
        postcard::from_bytes(payload).ok()
    }

    pub(crate) fn store_app_message(
        &self,
        group_id: &GroupId,
        id: &MessageId,
        msg: &EncryptedAppMessage,
    ) -> Result<(), fjall::Error> {
        let ks = self.get_keyspaces(group_id);
        let msg_key = Self::build_message_key(id.sender, id.seq);
        let msg_bytes = Self::serialize_app_message(msg);
        ks.messages.insert(msg_key, &msg_bytes)?;

        self.db.persist(PersistMode::SyncAll)?;

        self.message_cache.insert(*id, Arc::new(msg.clone()));

        if let Ok(broadcasts) = self.message_broadcasts.read()
            && let Some(tx) = broadcasts.get(group_id.as_bytes())
        {
            tx.send_modify(|sv| {
                let hw = sv.entry(id.sender).or_insert(0);
                *hw = (*hw).max(id.seq);
            });
        }

        Ok(())
    }

    pub(crate) fn get_messages_after(
        &self,
        group_id: &GroupId,
        state_vector: &StateVector,
    ) -> Vec<(MessageId, EncryptedAppMessage)> {
        let ks = self.get_keyspaces(group_id);
        let mut messages = Vec::new();

        for guard in ks.messages.iter() {
            let Ok((key, value)) = guard.into_inner() else {
                continue;
            };

            if let Some(msg_id) = Self::message_id_from_key(group_id, &key) {
                let covered = state_vector
                    .get(&msg_id.sender)
                    .is_some_and(|&hw| msg_id.seq <= hw);
                if !covered {
                    if let Some(cached) = self.message_cache.get(&msg_id) {
                        messages.push((msg_id, (*cached).clone()));
                    } else if let Some(msg) = Self::deserialize_app_message(&value) {
                        self.message_cache.insert(msg_id, Arc::new(msg.clone()));
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
        let ks = self.get_keyspaces(group_id);
        let mut deleted = 0;

        for guard in ks.messages.iter() {
            let Ok((key, _)) = guard.into_inner() else {
                continue;
            };

            if let Some(msg_id) = Self::message_id_from_key(group_id, &key)
                && watermark
                    .get(&msg_id.sender)
                    .is_some_and(|&hw| msg_id.seq <= hw)
            {
                ks.messages.remove(&*key)?;
                self.message_cache.remove(&msg_id);
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.db.persist(PersistMode::SyncAll)?;
        }

        Ok(deleted)
    }

    pub(crate) fn subscribe_messages(&self, group_id: &GroupId) -> watch::Receiver<StateVector> {
        let mut broadcasts = self.message_broadcasts.write().unwrap();
        let key = *group_id.as_bytes();

        broadcasts
            .entry(key)
            .or_insert_with(|| watch::channel(StateVector::default()).0)
            .subscribe()
    }

    fn get_promised_sync(&self, group_id: &GroupId, epoch: Epoch) -> Option<GroupProposal> {
        let ks = self.get_keyspaces(group_id);
        let key = Self::build_epoch_key(Epoch(PROMISED_SENTINEL_EPOCH));
        ks.accepted
            .get(key)
            .ok()
            .flatten()
            .and_then(|bytes| Self::deserialize_proposal(&bytes))
            .filter(|p| p.epoch == epoch)
    }

    fn get_accepted_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<(GroupProposal, GroupMessage)> {
        if let Some(cached) = self.accepted_cache.get(&(*group_id, epoch)) {
            return Some((*cached).clone());
        }

        let ks = self.get_keyspaces(group_id);
        let key = Self::build_epoch_key(epoch);
        let bytes = ks.accepted.get(key).ok()??;
        let result = Self::decode_slim_accepted(&bytes, epoch)?;
        self.accepted_cache
            .insert((*group_id, epoch), Arc::new(result.clone()));
        Some(result)
    }

    fn decode_slim_accepted(bytes: &[u8], epoch: Epoch) -> Option<(GroupProposal, GroupMessage)> {
        let (_, payload) = storage_versioned_decode(bytes);
        let slim: SlimAccepted = postcard::from_bytes(payload).ok()?;

        let message_bytes =
            postcard::to_allocvec(&slim.message).expect("serialization should not fail");
        let message_hash: [u8; 32] = Sha256::digest(&message_bytes).into();

        let proposal = GroupProposal {
            member_id: slim.member_id,
            epoch,
            attempt: slim.attempt,
            message_hash,
            signature: bytes::Bytes::from(slim.signature),
        };
        Some((proposal, slim.message))
    }

    fn set_promised_sync(
        &self,
        group_id: &GroupId,
        proposal: &GroupProposal,
    ) -> Result<(), fjall::Error> {
        let ks = self.get_keyspaces(group_id);
        let key = Self::build_epoch_key(Epoch(PROMISED_SENTINEL_EPOCH));
        let value = Self::serialize_proposal(proposal);
        ks.accepted.insert(key, &value)?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    fn set_accepted_sync(
        &self,
        group_id: &GroupId,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), fjall::Error> {
        let ks = self.get_keyspaces(group_id);
        let key = Self::build_epoch_key(proposal.epoch);

        let slim = SlimAccepted {
            member_id: proposal.member_id,
            attempt: proposal.attempt,
            signature: proposal.signature.to_vec(),
            message: message.clone(),
        };
        let data = postcard::to_allocvec(&slim).expect("serialization should not fail");
        let value = storage_versioned_encode(STORAGE_VERSION, &data);

        ks.accepted.insert(key, &value)?;
        self.db.persist(PersistMode::SyncAll)?;

        self.accepted_cache.insert(
            (*group_id, proposal.epoch),
            Arc::new((proposal.clone(), message.clone())),
        );

        Ok(())
    }

    fn get_accepted_from_sync(
        &self,
        group_id: &GroupId,
        from_epoch: Epoch,
    ) -> Vec<(GroupProposal, GroupMessage)> {
        let ks = self.get_keyspaces(group_id);
        let start_key = Self::build_epoch_key(from_epoch);

        ks.accepted
            .range(start_key..)
            .filter_map(|guard| {
                let (key, value) = guard.into_inner().ok()?;
                let epoch = Self::parse_epoch_from_key(&key)?;
                if epoch < from_epoch || epoch.0 == PROMISED_SENTINEL_EPOCH {
                    return None;
                }
                if let Some(cached) = self.accepted_cache.get(&(*group_id, epoch)) {
                    return Some((*cached).clone());
                }
                let result = Self::decode_slim_accepted(&value, epoch)?;
                self.accepted_cache
                    .insert((*group_id, epoch), Arc::new(result.clone()));
                Some(result)
            })
            .collect()
    }

    fn highest_accepted_round_sync(&self, group_id: &GroupId) -> Option<Epoch> {
        let ks = self.get_keyspaces(group_id);

        let mut highest: Option<Epoch> = None;
        for guard in ks.accepted.iter() {
            if let Ok((key, _)) = guard.into_inner()
                && let Some(epoch) = Self::parse_epoch_from_key(&key)
                && epoch.0 != PROMISED_SENTINEL_EPOCH
            {
                highest = Some(epoch);
            }
        }
        highest
    }

    fn get_broadcast(&self, group_id: &GroupId) -> watch::Sender<Option<GroupProposal>> {
        if let Some(sender) = self.broadcasts.read().unwrap().get(&group_id.0) {
            return sender.clone();
        }

        let mut broadcasts = self.broadcasts.write().unwrap();
        broadcasts
            .entry(group_id.0)
            .or_insert_with(|| watch::channel(None).0)
            .clone()
    }

    fn store_snapshot_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
        snapshot_bytes: &[u8],
    ) -> Result<(), fjall::Error> {
        let ks = self.get_keyspaces(group_id);
        let key = Self::build_epoch_key(epoch);
        ks.snapshots.insert(key, snapshot_bytes)?;

        self.snapshot_cache.insert(
            (*group_id, epoch),
            bytes::Bytes::copy_from_slice(snapshot_bytes),
        );
        let mut latest = self.latest_epoch.write().unwrap();
        if latest.get(group_id).is_none_or(|&e| epoch > e) {
            latest.insert(*group_id, epoch);
        }

        Ok(())
    }

    fn get_latest_snapshot_sync(&self, group_id: &GroupId) -> Option<(Epoch, bytes::Bytes)> {
        if let Some(&epoch) = self.latest_epoch.read().unwrap().get(group_id)
            && let Some(cached) = self.snapshot_cache.get(&(*group_id, epoch))
        {
            return Some((epoch, cached));
        }

        let ks = self.get_keyspaces(group_id);
        for guard in ks.snapshots.iter().rev() {
            if let Ok((key, value)) = guard.into_inner()
                && let Some(epoch) = Self::parse_epoch_from_key(&key)
            {
                let cached = bytes::Bytes::from(value);
                self.snapshot_cache
                    .insert((*group_id, epoch), cached.clone());
                self.latest_epoch.write().unwrap().insert(*group_id, epoch);
                return Some((epoch, cached));
            }
        }
        None
    }

    fn get_snapshot_at_or_before_sync(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
    ) -> Option<(Epoch, bytes::Bytes)> {
        if let Some(cached) = self.snapshot_cache.get(&(*group_id, epoch)) {
            return Some((epoch, cached));
        }

        let ks = self.get_keyspaces(group_id);
        let target_key = Self::build_epoch_key(epoch);

        for guard in ks.snapshots.range(..=target_key.as_slice()).rev() {
            if let Ok((key, value)) = guard.into_inner()
                && let Some(found_epoch) = Self::parse_epoch_from_key(&key)
            {
                let cached = bytes::Bytes::from(value);
                self.snapshot_cache
                    .insert((*group_id, found_epoch), cached.clone());
                return Some((found_epoch, cached));
            }
        }
        None
    }

    fn prune_snapshots_sync(
        &self,
        group_id: &GroupId,
        current_epoch: Epoch,
    ) -> Result<(), fjall::Error> {
        let ks = self.get_keyspaces(group_id);

        let mut kept_epochs = std::collections::HashSet::new();
        kept_epochs.insert(current_epoch.0);
        if current_epoch.0 >= 1 {
            kept_epochs.insert(current_epoch.0 - 1);
        }
        if current_epoch.0 >= 2 {
            kept_epochs.insert(current_epoch.0 - 2);
        }
        let mut gap = 4u64;
        while gap <= current_epoch.0 {
            kept_epochs.insert(current_epoch.0 - gap);
            gap = gap.saturating_mul(2);
        }

        let mut oldest_epoch: Option<u64> = None;
        let mut to_delete = Vec::new();

        for guard in ks.snapshots.iter() {
            if let Ok((key, _)) = guard.into_inner()
                && let Some(epoch) = Self::parse_epoch_from_key(&key)
            {
                if oldest_epoch.is_none() {
                    oldest_epoch = Some(epoch.0);
                }
                if Some(epoch.0) != oldest_epoch && !kept_epochs.contains(&epoch.0) {
                    to_delete.push(key.to_vec());
                }
            }
        }

        for key in &to_delete {
            ks.snapshots.remove(key)?;
            if let Some(epoch) = Self::parse_epoch_from_key(key) {
                self.snapshot_cache.remove(&(*group_id, epoch));
            }
        }

        Ok(())
    }

    fn list_groups_sync(&self) -> Vec<GroupId> {
        let names = self.db.list_keyspace_names();
        let mut groups = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for name in &names {
            let name = name.as_ref();
            if let Some(prefix) = name.strip_suffix(".snapshots")
                && let Ok(bytes) = bs58::decode(prefix).into_vec()
                && bytes.len() == 32
            {
                let mut group_bytes = [0u8; 32];
                group_bytes.copy_from_slice(&bytes);
                let gid = GroupId::new(group_bytes);
                if seen.insert(gid) {
                    groups.push(gid);
                }
            }
        }
        groups
    }

    pub(crate) fn database(&self) -> &Database {
        &self.db
    }
}

#[derive(Clone)]
pub struct SharedFjallStateStore {
    inner: Arc<FjallStateStore>,
}

impl SharedFjallStateStore {
    /// # Errors
    ///
    /// Returns [`fjall::Error`] if opening the database fails.
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

    #[must_use]
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.inner.list_groups_sync()
    }

    /// # Errors
    ///
    /// Returns [`fjall::Error`] if storing the snapshot fails.
    pub fn store_snapshot(
        &self,
        group_id: &GroupId,
        epoch: Epoch,
        snapshot_bytes: &[u8],
    ) -> Result<(), fjall::Error> {
        self.inner
            .store_snapshot_sync(group_id, epoch, snapshot_bytes)
    }

    #[must_use]
    pub fn get_latest_snapshot(&self, group_id: &GroupId) -> Option<(Epoch, bytes::Bytes)> {
        self.inner.get_latest_snapshot_sync(group_id)
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

    #[allow(dead_code)]
    pub(crate) fn delete_before_watermark(
        &self,
        group_id: &GroupId,
        watermark: &StateVector,
    ) -> Result<usize, fjall::Error> {
        self.inner.delete_before_watermark(group_id, watermark)
    }

    #[must_use]
    pub(crate) fn subscribe_messages(&self, group_id: &GroupId) -> watch::Receiver<StateVector> {
        self.inner.subscribe_messages(group_id)
    }

    #[must_use]
    pub fn group_storage_sizes(&self, group_id: &GroupId) -> GroupStorageSizes {
        self.inner.get_keyspaces(group_id).disk_space()
    }

    #[must_use]
    pub fn database(&self) -> &Database {
        self.inner.database()
    }

    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn store_encrypted_group_info(&self, group_id: &GroupId, ciphertext: bytes::Bytes) {
        self.inner
            .encrypted_group_info
            .write()
            .expect("lock poisoned")
            .insert(*group_id, ciphertext);
    }

    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_encrypted_group_info(&self, group_id: &GroupId) -> Option<bytes::Bytes> {
        self.inner
            .encrypted_group_info
            .read()
            .expect("lock poisoned")
            .get(group_id)
            .cloned()
    }
}

#[derive(Clone)]
pub struct GroupStateStore {
    pub(crate) inner: Arc<FjallStateStore>,
    pub(crate) group_id: GroupId,
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

    /// # Errors
    ///
    /// Returns [`fjall::Error`] if storing the snapshot fails.
    pub(crate) fn store_snapshot(
        &self,
        epoch: Epoch,
        snapshot_bytes: &[u8],
    ) -> Result<(), fjall::Error> {
        self.inner
            .store_snapshot_sync(&self.group_id, epoch, snapshot_bytes)?;
        self.inner.prune_snapshots_sync(&self.group_id, epoch)?;
        Ok(())
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn get_latest_snapshot(&self) -> Option<(Epoch, bytes::Bytes)> {
        self.inner.get_latest_snapshot_sync(&self.group_id)
    }

    #[must_use]
    pub(crate) fn get_snapshot_at_or_before(&self, epoch: Epoch) -> Option<(Epoch, bytes::Bytes)> {
        self.inner
            .get_snapshot_at_or_before_sync(&self.group_id, epoch)
    }

    pub(crate) fn delete_before_watermark(
        &self,
        watermark: &StateVector,
    ) -> Result<usize, fjall::Error> {
        self.inner
            .delete_before_watermark(&self.group_id, watermark)
    }

    /// Store an accepted proposal and broadcast it to subscribers.
    ///
    /// Unlike `AcceptorStateStore::accept`, this bypasses the Paxos promise
    /// check. Used for external commits where the spool validates the MLS
    /// message directly and needs to distribute the result.
    ///
    /// # Errors
    ///
    /// Returns [`fjall::Error`] if the storage write fails.
    pub(crate) fn store_and_broadcast(
        &self,
        proposal: &GroupProposal,
        message: &GroupMessage,
    ) -> Result<(), fjall::Error> {
        self.inner
            .set_accepted_sync(&self.group_id, proposal, message)?;
        self.inner
            .get_broadcast(&self.group_id)
            .send_replace(Some(proposal.clone()));
        Ok(())
    }
}

type WatchChangedFut = Pin<
    Box<
        dyn Future<
                Output = (
                    Result<(), watch::error::RecvError>,
                    watch::Receiver<Option<GroupProposal>>,
                ),
            > + Send,
    >,
>;

fn make_watch_changed_fut(mut rx: watch::Receiver<Option<GroupProposal>>) -> WatchChangedFut {
    Box::pin(async move {
        let result = rx.changed().await;
        (result, rx)
    })
}

pub struct GroupSubscription {
    inner: Arc<FjallStateStore>,
    group_id: GroupId,
    state: GroupSubscriptionState,
    next_epoch: Epoch,
    buffer: std::collections::VecDeque<(GroupProposal, GroupMessage)>,
}

enum GroupSubscriptionState {
    Idle(watch::Receiver<Option<GroupProposal>>),
    Waiting(WatchChangedFut),
    Done,
}

impl Stream for GroupSubscription {
    type Item = (GroupProposal, GroupMessage);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(item) = this.buffer.pop_front() {
            this.next_epoch = Epoch(item.0.epoch.0 + 1);
            return Poll::Ready(Some(item));
        }

        loop {
            match &mut this.state {
                GroupSubscriptionState::Done => return Poll::Ready(None),
                GroupSubscriptionState::Idle(_) => {
                    let GroupSubscriptionState::Idle(rx) =
                        std::mem::replace(&mut this.state, GroupSubscriptionState::Done)
                    else {
                        unreachable!()
                    };
                    this.state = GroupSubscriptionState::Waiting(make_watch_changed_fut(rx));
                }
                GroupSubscriptionState::Waiting(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready((Ok(()), rx)) => {
                        this.state = GroupSubscriptionState::Idle(rx);
                        let from = this.next_epoch;
                        let entries = this.inner.get_accepted_from_sync(&this.group_id, from);
                        this.buffer.extend(entries);
                        if let Some(item) = this.buffer.pop_front() {
                            this.next_epoch = Epoch(item.0.epoch.0 + 1);
                            return Poll::Ready(Some(item));
                        }
                    }
                    Poll::Ready((Err(_), _)) => {
                        this.state = GroupSubscriptionState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

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

            inner.get_broadcast(&group_id).send_replace(Some(proposal));

            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    async fn subscribe_from(&self, from_round: Epoch) -> Self::Subscription {
        let inner = self.inner.clone();
        let group_id = self.group_id;

        let historical = tokio::task::spawn_blocking({
            let inner = inner.clone();
            move || inner.get_accepted_from_sync(&group_id, from_round)
        })
        .await
        .expect("spawn_blocking panicked");

        let watch_rx = self.inner.get_broadcast(&self.group_id).subscribe();

        let next_epoch = historical
            .last()
            .map_or(from_round, |(p, _)| Epoch(p.epoch.0 + 1));

        GroupSubscription {
            inner,
            group_id: self.group_id,
            state: GroupSubscriptionState::Idle(watch_rx),
            next_epoch,
            buffer: historical.into(),
        }
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
    use mls_rs::ExtensionList;

    use super::*;

    fn make_test_message() -> GroupMessage {
        use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
        use mls_rs::{CipherSuite, CipherSuiteProvider, CryptoProvider};
        use mls_rs_crypto_rustcrypto::RustCryptoProvider;

        let cipher_suite = CipherSuite::CURVE25519_AES128;
        let crypto = RustCryptoProvider::default();
        let cs = crypto.cipher_suite_provider(cipher_suite).unwrap();

        let (secret_key, public_key) = cs.signature_key_generate().unwrap();
        let credential = BasicCredential::new(b"test".to_vec());
        let signing_identity =
            mls_rs::identity::SigningIdentity::new(credential.into_credential(), public_key);

        let client = mls_rs::Client::builder()
            .crypto_provider(crypto)
            .identity_provider(BasicIdentityProvider::new())
            .signing_identity(signing_identity, secret_key, cipher_suite)
            .build();

        let mut group = client
            .create_group(ExtensionList::default(), ExtensionList::default(), None)
            .unwrap();
        let commit = group.commit_builder().build().unwrap();
        GroupMessage::new(commit.commit_message)
    }

    fn make_test_proposal(epoch: Epoch) -> GroupProposal {
        GroupProposal {
            member_id: MemberId(1),
            epoch,
            attempt: Attempt(0),
            message_hash: [0u8; 32],
            signature: bytes::Bytes::from_static(&[1, 2, 3, 4]),
        }
    }

    fn open_test_store(path: &Path) -> FjallStateStore {
        FjallStateStore::open_sync(path).unwrap()
    }

    #[test]
    fn slim_accepted_roundtrip() {
        let message = make_test_message();
        let epoch = Epoch(42);

        let slim = SlimAccepted {
            member_id: MemberId(5),
            attempt: Attempt(3),
            signature: vec![10, 20, 30],
            message: message.clone(),
        };

        let data = postcard::to_allocvec(&slim).expect("serialize");
        let encoded = storage_versioned_encode(STORAGE_VERSION, &data);
        let decoded = FjallStateStore::decode_slim_accepted(&encoded, epoch).unwrap();

        assert_eq!(decoded.0.member_id, MemberId(5));
        assert_eq!(decoded.0.epoch, epoch);
        assert_eq!(decoded.0.attempt, Attempt(3));
        assert_eq!(decoded.0.signature, &[10, 20, 30][..]);

        let message_bytes = postcard::to_allocvec(&message).expect("serialization should not fail");
        let expected_hash: [u8; 32] = Sha256::digest(&message_bytes).into();
        assert_eq!(decoded.0.message_hash, expected_hash);
    }

    #[test]
    fn accepted_set_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([1u8; 32]);
        let message = make_test_message();
        let proposal = make_test_proposal(Epoch(10));

        store.set_accepted_sync(&gid, &proposal, &message).unwrap();
        let (loaded_proposal, _loaded_message) = store.get_accepted_sync(&gid, Epoch(10)).unwrap();

        assert_eq!(loaded_proposal.member_id, proposal.member_id);
        assert_eq!(loaded_proposal.epoch, Epoch(10));
        assert_eq!(loaded_proposal.attempt, proposal.attempt);
        assert_eq!(loaded_proposal.signature, proposal.signature);
    }

    #[test]
    fn accepted_from_range_query() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([2u8; 32]);
        let message = make_test_message();

        for e in 0..5 {
            let proposal = make_test_proposal(Epoch(e));
            store.set_accepted_sync(&gid, &proposal, &message).unwrap();
        }

        let from_2 = store.get_accepted_from_sync(&gid, Epoch(2));
        assert_eq!(from_2.len(), 3);
        assert_eq!(from_2[0].0.epoch, Epoch(2));
        assert_eq!(from_2[2].0.epoch, Epoch(4));
    }

    #[test]
    fn accepted_from_excludes_promised_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([3u8; 32]);
        let message = make_test_message();

        let proposal = make_test_proposal(Epoch(0));
        store.set_accepted_sync(&gid, &proposal, &message).unwrap();

        let promised = make_test_proposal(Epoch(1));
        store.set_promised_sync(&gid, &promised).unwrap();

        let all = store.get_accepted_from_sync(&gid, Epoch(0));
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0.epoch, Epoch(0));
    }

    #[test]
    fn promised_sentinel_matching_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([4u8; 32]);

        let proposal = make_test_proposal(Epoch(5));
        store.set_promised_sync(&gid, &proposal).unwrap();

        let loaded = store.get_promised_sync(&gid, Epoch(5));
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().epoch, Epoch(5));

        let wrong_epoch = store.get_promised_sync(&gid, Epoch(6));
        assert!(wrong_epoch.is_none());
    }

    #[test]
    fn snapshot_crud() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([5u8; 32]);

        store.store_snapshot_sync(&gid, Epoch(0), b"snap0").unwrap();
        store.store_snapshot_sync(&gid, Epoch(5), b"snap5").unwrap();
        store
            .store_snapshot_sync(&gid, Epoch(10), b"snap10")
            .unwrap();

        let (epoch, bytes) = store.get_latest_snapshot_sync(&gid).unwrap();
        assert_eq!(epoch, Epoch(10));
        assert_eq!(bytes, &b"snap10"[..]);

        let (epoch, bytes) = store
            .get_snapshot_at_or_before_sync(&gid, Epoch(7))
            .unwrap();
        assert_eq!(epoch, Epoch(5));
        assert_eq!(bytes, &b"snap5"[..]);

        let (epoch, bytes) = store
            .get_snapshot_at_or_before_sync(&gid, Epoch(5))
            .unwrap();
        assert_eq!(epoch, Epoch(5));
        assert_eq!(bytes, &b"snap5"[..]);

        assert!(
            store
                .get_snapshot_at_or_before_sync(&gid, Epoch(0))
                .is_some()
        );
    }

    #[test]
    fn logarithmic_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([6u8; 32]);

        for e in 0..=20 {
            store
                .store_snapshot_sync(&gid, Epoch(e), format!("snap{e}").as_bytes())
                .unwrap();
        }

        store.prune_snapshots_sync(&gid, Epoch(20)).unwrap();

        let remaining: Vec<u64> = (0..=20)
            .filter(|&e| {
                store
                    .get_snapshot_at_or_before_sync(&gid, Epoch(e))
                    .is_some()
                    && store
                        .get_snapshot_at_or_before_sync(&gid, Epoch(e))
                        .unwrap()
                        .0
                        == Epoch(e)
            })
            .collect();

        assert!(remaining.contains(&0), "oldest must be kept");
        assert!(remaining.contains(&20), "current must be kept");
        assert!(remaining.contains(&19), "E-1 must be kept");
        assert!(remaining.contains(&18), "E-2 must be kept");
        assert!(remaining.contains(&16), "E-4 must be kept");
        assert!(remaining.contains(&12), "E-8 must be kept");
        assert!(remaining.contains(&4), "E-16 must be kept");
    }

    #[test]
    fn list_groups_from_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());

        let gid1 = GroupId::new([1u8; 32]);
        let gid2 = GroupId::new([2u8; 32]);

        store.store_snapshot_sync(&gid1, Epoch(0), b"snap").unwrap();
        store.store_snapshot_sync(&gid1, Epoch(1), b"snap").unwrap();
        store.store_snapshot_sync(&gid2, Epoch(0), b"snap").unwrap();

        let groups = store.list_groups_sync();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&gid1));
        assert!(groups.contains(&gid2));
    }

    #[test]
    fn highest_accepted_excludes_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([7u8; 32]);
        let message = make_test_message();

        let proposal = make_test_proposal(Epoch(5));
        store.set_accepted_sync(&gid, &proposal, &message).unwrap();

        let promised = make_test_proposal(Epoch(10));
        store.set_promised_sync(&gid, &promised).unwrap();

        let highest = store.highest_accepted_round_sync(&gid).unwrap();
        assert_eq!(highest, Epoch(5));
    }

    #[test]
    fn test_store_and_get_messages() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([1u8; 32]);
        let sender = MemberFingerprint([2u8; 8]);

        let id1 = MessageId {
            group_id: gid,
            sender,
            seq: 1,
        };
        let id2 = MessageId {
            group_id: gid,
            sender,
            seq: 2,
        };
        let msg = EncryptedAppMessage {
            ciphertext: bytes::Bytes::from_static(&[10, 20]),
        };

        store.store_app_message(&gid, &id1, &msg).unwrap();
        store.store_app_message(&gid, &id2, &msg).unwrap();

        let all = store.get_messages_after(&gid, &StateVector::default());
        assert_eq!(all.len(), 2);

        let mut sv = StateVector::default();
        sv.insert(sender, 1);
        let after = store.get_messages_after(&gid, &sv);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].0.seq, 2);
    }

    #[test]
    fn test_delete_before_watermark_partial() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([3u8; 32]);

        let sender_a = MemberFingerprint([10u8; 8]);
        let sender_b = MemberFingerprint([20u8; 8]);
        let msg = EncryptedAppMessage {
            ciphertext: bytes::Bytes::from_static(&[1]),
        };

        for seq in 1..=5 {
            let id = MessageId {
                group_id: gid,
                sender: sender_a,
                seq,
            };
            store.store_app_message(&gid, &id, &msg).unwrap();
        }
        for seq in 1..=3 {
            let id = MessageId {
                group_id: gid,
                sender: sender_b,
                seq,
            };
            store.store_app_message(&gid, &id, &msg).unwrap();
        }

        let mut watermark = StateVector::default();
        watermark.insert(sender_a, 3);
        watermark.insert(sender_b, 1);

        let deleted = store.delete_before_watermark(&gid, &watermark).unwrap();
        assert_eq!(deleted, 4);

        let remaining = store.get_messages_after(&gid, &StateVector::default());
        assert_eq!(remaining.len(), 4);
    }

    #[test]
    fn test_delete_before_watermark_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([4u8; 32]);

        let deleted = store
            .delete_before_watermark(&gid, &StateVector::default())
            .unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_message_key_ordering() {
        let sender = MemberFingerprint([2u8; 8]);

        let key1 = FjallStateStore::build_message_key(sender, 1);
        let key2 = FjallStateStore::build_message_key(sender, 2);
        let key3 = FjallStateStore::build_message_key(sender, 100);

        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_message_id_from_key_roundtrip() {
        let gid = GroupId::new([5u8; 32]);
        let sender = MemberFingerprint([6u8; 8]);

        let key = FjallStateStore::build_message_key(sender, 42);
        let id = FjallStateStore::message_id_from_key(&gid, &key).unwrap();

        assert_eq!(id.group_id, gid);
        assert_eq!(id.sender, sender);
        assert_eq!(id.seq, 42);
    }

    #[test]
    fn storage_versioned_encode_decode_roundtrip() {
        let payload = b"test data";
        let encoded = super::storage_versioned_encode(1, payload);
        let (version, decoded) = super::storage_versioned_decode(&encoded);
        assert_eq!(version, 1);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn storage_versioned_decode_legacy_data() {
        let legacy = b"raw postcard bytes";
        let (version, decoded) = super::storage_versioned_decode(legacy);
        assert_eq!(version, 1);
        assert_eq!(decoded, legacy.as_slice());
    }

    #[test]
    fn shared_store_group_storage_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let shared = SharedFjallStateStore {
            inner: Arc::new(store),
        };
        let gid = GroupId::new([0xAA; 32]);
        let sizes = shared.group_storage_sizes(&gid);
        assert_eq!(sizes.accepted_bytes, 0);
        assert_eq!(sizes.messages_bytes, 0);
        assert_eq!(sizes.snapshots_bytes, 0);
    }

    #[test]
    fn shared_store_database_accessible() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let shared = SharedFjallStateStore {
            inner: Arc::new(store),
        };
        let _db = shared.database();
    }

    #[test]
    fn shared_store_wrappers() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let shared = SharedFjallStateStore {
            inner: Arc::new(store),
        };
        let gid = GroupId::new([0xBB; 32]);

        shared.store_snapshot(&gid, Epoch(0), b"snap").unwrap();
        let (epoch, data) = shared.get_latest_snapshot(&gid).unwrap();
        assert_eq!(epoch, Epoch(0));
        assert_eq!(data, &b"snap"[..]);

        let sender = MemberFingerprint([1u8; 8]);
        let id = MessageId {
            group_id: gid,
            sender,
            seq: 1,
        };
        let msg = EncryptedAppMessage {
            ciphertext: bytes::Bytes::from_static(&[42]),
        };
        shared.store_app_message(&gid, &id, &msg).unwrap();

        let msgs = shared.get_messages_after(&gid, &StateVector::default());
        assert_eq!(msgs.len(), 1);

        let groups = shared.list_groups();
        assert!(groups.contains(&gid));

        let deleted = shared
            .delete_before_watermark(&gid, &StateVector::default())
            .unwrap();
        assert_eq!(deleted, 0);

        let _rx = shared.subscribe_messages(&gid);
    }

    #[test]
    fn decode_slim_accepted_invalid_bytes() {
        assert!(FjallStateStore::decode_slim_accepted(&[], Epoch(0)).is_none());
        assert!(FjallStateStore::decode_slim_accepted(b"garbage", Epoch(0)).is_none());

        let versioned_garbage = storage_versioned_encode(1, b"not valid postcard");
        assert!(FjallStateStore::decode_slim_accepted(&versioned_garbage, Epoch(0)).is_none());
    }

    #[test]
    fn message_id_from_key_too_short() {
        let gid = GroupId::new([0u8; 32]);
        assert!(FjallStateStore::message_id_from_key(&gid, &[]).is_none());
        assert!(FjallStateStore::message_id_from_key(&gid, &[1; 15]).is_none());
    }

    #[test]
    fn disk_space_populated() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xDD; 32]);

        store
            .store_snapshot_sync(&gid, Epoch(0), b"some snapshot data")
            .unwrap();
        let ks = store.get_keyspaces(&gid);
        let _sizes = ks.disk_space();
    }

    #[test]
    fn parse_epoch_from_key_wrong_length() {
        assert!(FjallStateStore::parse_epoch_from_key(&[]).is_none());
        assert!(FjallStateStore::parse_epoch_from_key(&[1, 2, 3]).is_none());
    }

    #[test]
    fn deserialize_proposal_invalid() {
        assert!(FjallStateStore::deserialize_proposal(&[]).is_none());
        assert!(FjallStateStore::deserialize_proposal(b"not valid").is_none());
        let versioned_bad = storage_versioned_encode(1, b"bad postcard");
        assert!(FjallStateStore::deserialize_proposal(&versioned_bad).is_none());
    }

    #[test]
    fn deserialize_app_message_invalid() {
        assert!(FjallStateStore::deserialize_app_message(&[]).is_none());
        assert!(FjallStateStore::deserialize_app_message(b"nope").is_none());
    }

    #[test]
    fn group_state_store_wrappers() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let shared = SharedFjallStateStore {
            inner: Arc::new(store),
        };
        let gid = GroupId::new([0xCC; 32]);
        let gs = shared.for_group(gid);

        let message = make_test_message();
        let proposal = make_test_proposal(Epoch(0));

        shared
            .inner
            .set_accepted_sync(&gid, &proposal, &message)
            .unwrap();

        let accepted = gs.get_accepted_from(Epoch(0));
        assert_eq!(accepted.len(), 1);

        gs.store_snapshot(Epoch(0), b"snap").unwrap();
        let (epoch, data) = gs.get_latest_snapshot().unwrap();
        assert_eq!(epoch, Epoch(0));
        assert_eq!(data, &b"snap"[..]);

        let snap = gs.get_snapshot_at_or_before(Epoch(10));
        assert!(snap.is_some());

        let deleted = gs.delete_before_watermark(&StateVector::default()).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn snapshot_cache_hit_avoids_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE0; 32]);

        store
            .store_snapshot_sync(&gid, Epoch(5), b"cached_snap")
            .unwrap();

        let (epoch, data) = store.get_latest_snapshot_sync(&gid).unwrap();
        assert_eq!(epoch, Epoch(5));
        assert_eq!(data, &b"cached_snap"[..]);

        assert!(store.snapshot_cache.get(&(gid, Epoch(5))).is_some());
        assert_eq!(
            *store.latest_epoch.read().unwrap().get(&gid).unwrap(),
            Epoch(5)
        );
    }

    #[test]
    fn snapshot_cache_updates_on_newer_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE1; 32]);

        store.store_snapshot_sync(&gid, Epoch(1), b"snap1").unwrap();
        store.store_snapshot_sync(&gid, Epoch(5), b"snap5").unwrap();

        assert_eq!(
            *store.latest_epoch.read().unwrap().get(&gid).unwrap(),
            Epoch(5)
        );

        let (epoch, data) = store.get_latest_snapshot_sync(&gid).unwrap();
        assert_eq!(epoch, Epoch(5));
        assert_eq!(data, &b"snap5"[..]);

        assert!(store.snapshot_cache.get(&(gid, Epoch(1))).is_some());
        assert!(store.snapshot_cache.get(&(gid, Epoch(5))).is_some());
    }

    #[test]
    fn snapshot_cache_at_or_before_exact_hit() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE2; 32]);

        store.store_snapshot_sync(&gid, Epoch(3), b"snap3").unwrap();
        store.store_snapshot_sync(&gid, Epoch(7), b"snap7").unwrap();

        let (epoch, data) = store
            .get_snapshot_at_or_before_sync(&gid, Epoch(7))
            .unwrap();
        assert_eq!(epoch, Epoch(7));
        assert_eq!(data, &b"snap7"[..]);

        let (epoch, data) = store
            .get_snapshot_at_or_before_sync(&gid, Epoch(5))
            .unwrap();
        assert_eq!(epoch, Epoch(3));
        assert_eq!(data, &b"snap3"[..]);
    }

    #[test]
    fn snapshot_prune_removes_from_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE3; 32]);

        for e in 0..=20 {
            store
                .store_snapshot_sync(&gid, Epoch(e), format!("snap{e}").as_bytes())
                .unwrap();
        }

        for e in 0..=20 {
            assert!(store.snapshot_cache.get(&(gid, Epoch(e))).is_some());
        }

        store.prune_snapshots_sync(&gid, Epoch(20)).unwrap();

        assert!(store.snapshot_cache.get(&(gid, Epoch(20))).is_some());
        assert!(store.snapshot_cache.get(&(gid, Epoch(0))).is_some());
        assert!(store.snapshot_cache.get(&(gid, Epoch(3))).is_none());
    }

    #[test]
    fn accepted_cache_set_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE4; 32]);
        let message = make_test_message();
        let proposal = make_test_proposal(Epoch(10));

        store.set_accepted_sync(&gid, &proposal, &message).unwrap();

        assert!(store.accepted_cache.get(&(gid, Epoch(10))).is_some());

        let (loaded, _) = store.get_accepted_sync(&gid, Epoch(10)).unwrap();
        assert_eq!(loaded.epoch, Epoch(10));
        assert_eq!(loaded.member_id, proposal.member_id);
    }

    #[test]
    fn accepted_cache_populated_on_range_query() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE5; 32]);
        let message = make_test_message();

        for e in 0..5 {
            let proposal = make_test_proposal(Epoch(e));
            store.set_accepted_sync(&gid, &proposal, &message).unwrap();
        }

        for e in 0..5 {
            assert!(store.accepted_cache.get(&(gid, Epoch(e))).is_some());
        }

        let from_2 = store.get_accepted_from_sync(&gid, Epoch(2));
        assert_eq!(from_2.len(), 3);
    }

    #[test]
    fn message_cache_store_and_retrieve() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE6; 32]);
        let sender = MemberFingerprint([0xAA; 8]);
        let id = MessageId {
            group_id: gid,
            sender,
            seq: 1,
        };
        let msg = EncryptedAppMessage {
            ciphertext: bytes::Bytes::from_static(&[1, 2, 3]),
        };

        store.store_app_message(&gid, &id, &msg).unwrap();

        assert!(store.message_cache.get(&id).is_some());
        assert_eq!(
            store.message_cache.get(&id).unwrap().ciphertext,
            &[1, 2, 3][..]
        );

        let msgs = store.get_messages_after(&gid, &StateVector::default());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].1.ciphertext, &[1, 2, 3][..]);
    }

    #[test]
    fn message_cache_delete_removes_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE7; 32]);
        let sender = MemberFingerprint([0xBB; 8]);
        let msg = EncryptedAppMessage {
            ciphertext: bytes::Bytes::from_static(&[1]),
        };

        for seq in 1..=3 {
            let id = MessageId {
                group_id: gid,
                sender,
                seq,
            };
            store.store_app_message(&gid, &id, &msg).unwrap();
        }

        for seq in 1..=3 {
            let id = MessageId {
                group_id: gid,
                sender,
                seq,
            };
            assert!(store.message_cache.get(&id).is_some());
        }

        let mut watermark = StateVector::default();
        watermark.insert(sender, 2);
        let deleted = store.delete_before_watermark(&gid, &watermark).unwrap();
        assert_eq!(deleted, 2);

        assert!(
            store
                .message_cache
                .get(&MessageId {
                    group_id: gid,
                    sender,
                    seq: 1
                })
                .is_none()
        );
        assert!(
            store
                .message_cache
                .get(&MessageId {
                    group_id: gid,
                    sender,
                    seq: 2
                })
                .is_none()
        );
        assert!(
            store
                .message_cache
                .get(&MessageId {
                    group_id: gid,
                    sender,
                    seq: 3
                })
                .is_some()
        );
    }

    #[test]
    fn snapshot_cache_cold_read_populates() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_test_store(dir.path());
        let gid = GroupId::new([0xE8; 32]);

        {
            let ks = store.get_keyspaces(&gid);
            let key = FjallStateStore::build_epoch_key(Epoch(3));
            ks.snapshots.insert(key, b"direct_snap").unwrap();
        }

        assert!(store.snapshot_cache.get(&(gid, Epoch(3))).is_none());
        assert!(store.latest_epoch.read().unwrap().get(&gid).is_none());

        let (epoch, data) = store.get_latest_snapshot_sync(&gid).unwrap();
        assert_eq!(epoch, Epoch(3));
        assert_eq!(data, &b"direct_snap"[..]);

        assert!(store.snapshot_cache.get(&(gid, Epoch(3))).is_some());
        assert_eq!(
            *store.latest_epoch.read().unwrap().get(&gid).unwrap(),
            Epoch(3)
        );
    }
}
