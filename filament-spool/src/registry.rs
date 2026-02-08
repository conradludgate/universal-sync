//! Multi-group registry for an acceptor server.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use error_stack::{Report, ResultExt};
use filament_core::{AcceptorId, Epoch, GroupId, GroupInfoExt};
use filament_warp::Learner;
use iroh::{Endpoint, EndpointAddr};
use mls_rs::external_client::ExternalClient;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::{CipherSuiteProvider, ExtensionList, MlsMessage};
use tokio::sync::watch;

#[derive(Debug)]
pub struct RegistryError;

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("registry operation failed")
    }
}

impl std::error::Error for RegistryError {}

use crate::acceptor::GroupAcceptor;
use crate::learner::GroupLearningActor;
use crate::state_store::{GroupStateStore, SharedFjallStateStore};

pub type EpochWatcher = (watch::Receiver<Epoch>, Box<dyn Fn() -> Epoch + Send>);

struct GroupEpochWatcher {
    tx: watch::Sender<Epoch>,
    rx: watch::Receiver<Epoch>,
}

impl GroupEpochWatcher {
    fn new(initial_epoch: Epoch) -> Self {
        let (tx, rx) = watch::channel(initial_epoch);
        Self { tx, rx }
    }

    fn notify(&self, epoch: Epoch) {
        let _ = self.tx.send(epoch);
    }
}

/// Manages multiple MLS groups for an acceptor server.
///
/// Groups are persisted and reconstructed on each access.
#[derive(Clone)]
pub struct AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    external_client: Arc<ExternalClient<C>>,
    cipher_suite: CS,
    state_store: SharedFjallStateStore,
    endpoint: Endpoint,
    epoch_watchers: Arc<RwLock<HashMap<GroupId, Arc<GroupEpochWatcher>>>>,
}

impl<C, CS> AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    pub fn new(
        external_client: ExternalClient<C>,
        cipher_suite: CS,
        state_store: SharedFjallStateStore,
        endpoint: Endpoint,
    ) -> Self {
        Self {
            external_client: Arc::new(external_client),
            cipher_suite,
            state_store,
            endpoint,
            epoch_watchers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn own_id(&self) -> AcceptorId {
        AcceptorId::from_bytes(*self.endpoint.id().as_bytes())
    }

    fn extract_acceptors_from_extensions(extensions: &ExtensionList) -> Vec<EndpointAddr> {
        extensions
            .get_as::<GroupInfoExt>()
            .ok()
            .flatten()
            .map_or_else(Vec::new, |info| info.acceptors)
    }

    fn create_acceptor_from_snapshot(
        &self,
        snapshot_bytes: &[u8],
    ) -> Result<GroupAcceptor<C, CS>, Report<RegistryError>> {
        let snapshot = mls_rs::external_client::ExternalSnapshot::from_bytes(snapshot_bytes)
            .change_context(RegistryError)
            .attach("failed to parse ExternalSnapshot")?;

        let external_group = self
            .external_client
            .load_group(snapshot)
            .change_context(RegistryError)
            .attach("failed to load group from snapshot")?;

        let acceptors =
            Self::extract_acceptors_from_extensions(&external_group.group_context().extensions);

        Ok(GroupAcceptor::new(
            external_group,
            self.cipher_suite.clone(),
            self.endpoint.secret_key().clone(),
            acceptors,
        ))
    }
}

impl<C, CS> AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    pub fn get_group(&self, group_id: &GroupId) -> Option<(GroupAcceptor<C, CS>, GroupStateStore)> {
        let (snapshot_epoch, snapshot_bytes) = self.state_store.get_latest_snapshot(group_id)?;
        let acceptor = self.create_acceptor_from_snapshot(&snapshot_bytes).ok()?;
        let state = self.state_store.for_group(*group_id);

        let mut acceptor = acceptor.with_state_store(state.clone());

        let historical = state.get_accepted_from(snapshot_epoch);

        for (proposal, message) in historical {
            tracing::debug!(epoch = ?proposal.epoch, "replaying commit for acceptor");
            if let Err(e) =
                futures::executor::block_on(Learner::apply(&mut acceptor, proposal, message))
            {
                tracing::warn!(?e, "failed to replay message");
            }
        }

        self.ensure_epoch_watcher_and_learning(
            *group_id,
            acceptor.current_round(),
            &acceptor,
            state.clone(),
        );

        Some((acceptor, state))
    }

    /// Creates a new group from serialized `GroupInfo`.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] if parsing, observing, or persisting the group fails.
    pub fn create_group(
        &self,
        group_info_bytes: &[u8],
    ) -> Result<(GroupId, GroupAcceptor<C, CS>, GroupStateStore), Report<RegistryError>> {
        let mls_message = MlsMessage::from_bytes(group_info_bytes)
            .change_context(RegistryError)
            .attach("failed to parse MLS message")?;

        let acceptors = mls_message
            .as_group_info()
            .map(|gi| Self::extract_acceptors_from_extensions(gi.extensions()))
            .unwrap_or_default();

        let external_group = self
            .external_client
            .observe_group(mls_message, None, None)
            .change_context(RegistryError)
            .attach("failed to observe group")?;

        let mls_group_id = external_group.group_context().group_id.clone();
        let group_id = GroupId::from_slice(&mls_group_id);

        let acceptor = GroupAcceptor::new(
            external_group,
            self.cipher_suite.clone(),
            self.endpoint.secret_key().clone(),
            acceptors,
        );

        let state = self.state_store.for_group(group_id);

        let acceptor = acceptor.with_state_store(state.clone());
        acceptor.store_initial_snapshot();

        self.ensure_epoch_watcher_and_learning(
            group_id,
            acceptor.current_round(),
            &acceptor,
            state.clone(),
        );

        Ok((group_id, acceptor, state))
    }

    /// # Errors
    ///
    /// Returns [`RegistryError`] if writing to the state store fails.
    pub fn store_message(
        &self,
        group_id: &GroupId,
        id: &filament_core::MessageId,
        msg: &filament_core::EncryptedAppMessage,
    ) -> Result<(), Report<RegistryError>> {
        self.state_store
            .store_app_message(group_id, id, msg)
            .change_context(RegistryError)
            .attach("failed to store message")
    }

    pub fn get_messages_after(
        &self,
        group_id: &GroupId,
        state_vector: &filament_core::StateVector,
    ) -> Vec<(filament_core::MessageId, filament_core::EncryptedAppMessage)> {
        self.state_store.get_messages_after(group_id, state_vector)
    }

    pub fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> tokio::sync::broadcast::Receiver<(
        filament_core::MessageId,
        filament_core::EncryptedAppMessage,
    )> {
        self.state_store.subscribe_messages(group_id)
    }

    pub fn get_epoch_watcher(&self, group_id: &GroupId) -> Option<EpochWatcher> {
        let watchers = self.epoch_watchers.read().ok()?;
        let watcher = watchers.get(group_id)?.clone();
        let rx = watcher.rx.clone();
        Some((rx.clone(), Box::new(move || *rx.borrow())))
    }

    pub fn notify_epoch_learned(&self, group_id: &GroupId, epoch: Epoch) {
        if let Ok(watchers) = self.epoch_watchers.read()
            && let Some(watcher) = watchers.get(group_id)
        {
            watcher.notify(epoch);
        }
    }

    fn ensure_epoch_watcher_and_learning(
        &self,
        group_id: GroupId,
        current_epoch: Epoch,
        acceptor: &GroupAcceptor<C, CS>,
        state: GroupStateStore,
    ) {
        let mut watchers = self.epoch_watchers.write().expect("lock poisoned");

        if watchers.contains_key(&group_id) {
            return;
        }

        let watcher = Arc::new(GroupEpochWatcher::new(current_epoch));
        watchers.insert(group_id, watcher.clone());

        let initial_acceptors: Vec<_> = acceptor
            .acceptor_addrs()
            .map(|(id, addr)| (*id, addr.clone()))
            .collect();

        let learning_actor: GroupLearningActor<C, CS> = GroupLearningActor::new(
            self.own_id(),
            group_id,
            self.endpoint.clone(),
            initial_acceptors,
            watcher.tx.clone(),
        );

        tokio::spawn(async move {
            learning_actor.run(state, current_epoch).await;
        });

        tracing::debug!(?group_id, "spawned learning actor");
    }
}
