//! `GroupStateStorage` implementation backed by fjall.

use std::path::Path;
use std::sync::Arc;

use error_stack::{Report, ResultExt};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use mls_rs_core::group::{EpochRecord, GroupState, GroupStateStorage};
use zeroize::Zeroizing;

#[derive(Debug)]
pub(crate) struct GroupStateError;

impl std::fmt::Display for GroupStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("group state storage error")
    }
}

impl std::error::Error for GroupStateError {}

impl mls_rs_core::error::IntoAnyError for GroupStateError {
    fn into_dyn_error(self) -> Result<Box<dyn std::error::Error + Send + Sync>, Self> {
        Ok(self.into())
    }
}

/// Keys: group state = raw `group_id`, epoch records = `group_id || epoch_be_bytes`.
#[derive(Clone)]
pub(crate) struct FjallGroupStateStorage {
    inner: Arc<FjallGroupStateStorageInner>,
}

struct FjallGroupStateStorageInner {
    #[allow(dead_code)]
    db: Database,
    keyspace: Keyspace,
}

impl FjallGroupStateStorage {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self, Report<GroupStateError>> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || Self::open_sync(&path))
            .await
            .expect("spawn_blocking panicked")
    }

    fn open_sync(path: &Path) -> Result<Self, Report<GroupStateError>> {
        let db = Database::builder(path)
            .open()
            .change_context(GroupStateError)
            .attach_opaque_with(|| format!("failed to open database at {}", path.display()))?;

        let keyspace = db
            .keyspace("mls_groups", KeyspaceCreateOptions::default)
            .change_context(GroupStateError)
            .attach("failed to open keyspace")?;

        Ok(Self {
            inner: Arc::new(FjallGroupStateStorageInner { db, keyspace }),
        })
    }

    fn state_key(group_id: &[u8]) -> &[u8] {
        group_id
    }

    fn epoch_key(group_id: &[u8], epoch_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(group_id.len() + 8);
        key.extend_from_slice(group_id);
        key.extend_from_slice(&epoch_id.to_be_bytes());
        key
    }

    fn parse_epoch(key: &[u8], group_id_len: usize) -> Option<u64> {
        if key.len() != group_id_len + 8 {
            return None;
        }
        let epoch_bytes: [u8; 8] = key[group_id_len..].try_into().ok()?;
        Some(u64::from_be_bytes(epoch_bytes))
    }
}

impl GroupStateStorage for FjallGroupStateStorage {
    type Error = GroupStateError;

    fn state(&self, group_id: &[u8]) -> Result<Option<Zeroizing<Vec<u8>>>, Self::Error> {
        self.inner
            .keyspace
            .get(Self::state_key(group_id))
            .map_err(|_| GroupStateError)
            .map(|opt| opt.map(|slice| Zeroizing::new(slice.to_vec())))
    }

    fn epoch(
        &self,
        group_id: &[u8],
        epoch_id: u64,
    ) -> Result<Option<Zeroizing<Vec<u8>>>, Self::Error> {
        let key = Self::epoch_key(group_id, epoch_id);
        self.inner
            .keyspace
            .get(&key)
            .map_err(|_| GroupStateError)
            .map(|opt| opt.map(|slice| Zeroizing::new(slice.to_vec())))
    }

    fn write(
        &mut self,
        state: GroupState,
        epoch_inserts: Vec<EpochRecord>,
        epoch_updates: Vec<EpochRecord>,
    ) -> Result<(), Self::Error> {
        let group_id = &state.id;

        self.inner
            .keyspace
            .insert(Self::state_key(group_id), &*state.data)
            .map_err(|_| GroupStateError)?;

        for record in epoch_inserts {
            let epoch_key = Self::epoch_key(group_id, record.id);
            self.inner
                .keyspace
                .insert(&epoch_key, &*record.data)
                .map_err(|_| GroupStateError)?;
        }

        for record in epoch_updates {
            let epoch_key = Self::epoch_key(group_id, record.id);
            self.inner
                .keyspace
                .insert(&epoch_key, &*record.data)
                .map_err(|_| GroupStateError)?;
        }

        Ok(())
    }

    fn max_epoch_id(&self, group_id: &[u8]) -> Result<Option<u64>, Self::Error> {
        let group_id_len = group_id.len();

        for guard in self.inner.keyspace.prefix(group_id).rev() {
            let (key, _) = guard.into_inner().map_err(|_| GroupStateError)?;

            if key.len() == group_id_len {
                continue;
            }

            if let Some(epoch_id) = Self::parse_epoch(&key, group_id_len) {
                return Ok(Some(epoch_id));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_group_state_storage_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = FjallGroupStateStorage::open(temp_dir.path()).await.unwrap();

        let group_id = b"test-group-id";
        let state_data = b"group state data".to_vec();
        let epoch_data = b"epoch data".to_vec();

        // Write state and epoch
        let state = GroupState {
            id: group_id.to_vec(),
            data: Zeroizing::new(state_data.clone()),
        };
        let epochs = vec![EpochRecord::new(1, Zeroizing::new(epoch_data.clone()))];

        storage.write(state, epochs, vec![]).unwrap();

        // Read back
        let loaded_state = storage.state(group_id).unwrap();
        assert_eq!(loaded_state, Some(Zeroizing::new(state_data)));

        let loaded_epoch = storage.epoch(group_id, 1).unwrap();
        assert_eq!(loaded_epoch, Some(Zeroizing::new(epoch_data)));

        let max_epoch = storage.max_epoch_id(group_id).unwrap();
        assert_eq!(max_epoch, Some(1));
    }

    #[tokio::test]
    async fn test_multiple_epochs() {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = FjallGroupStateStorage::open(temp_dir.path()).await.unwrap();

        let group_id = b"test-group";

        // Write multiple epochs
        let state = GroupState {
            id: group_id.to_vec(),
            data: Zeroizing::new(vec![]),
        };
        let epochs = vec![
            EpochRecord::new(1, Zeroizing::new(b"epoch1".to_vec())),
            EpochRecord::new(5, Zeroizing::new(b"epoch5".to_vec())),
            EpochRecord::new(3, Zeroizing::new(b"epoch3".to_vec())),
        ];

        storage.write(state, epochs, vec![]).unwrap();

        // Max should be 5
        let max_epoch = storage.max_epoch_id(group_id).unwrap();
        assert_eq!(max_epoch, Some(5));

        // Each epoch should be retrievable
        assert!(storage.epoch(group_id, 1).unwrap().is_some());
        assert!(storage.epoch(group_id, 3).unwrap().is_some());
        assert!(storage.epoch(group_id, 5).unwrap().is_some());
        assert!(storage.epoch(group_id, 2).unwrap().is_none());
    }
}
