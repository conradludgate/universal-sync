//! Multi-group registry for an acceptor server.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use error_stack::{Report, ResultExt};
use iroh::{Endpoint, EndpointAddr};
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalClient, ExternalGroup};
use mls_rs::mls_rs_codec::MlsDecode;
use mls_rs::{CipherSuiteProvider, ExtensionList, MlsMessage};
use tokio::sync::watch;
use universal_sync_core::{ACCEPTORS_EXTENSION_TYPE, AcceptorId, AcceptorsExt, Epoch, GroupId};
use universal_sync_paxos::Learner;

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
        for ext in extensions.iter() {
            if ext.extension_type == ACCEPTORS_EXTENSION_TYPE
                && let Ok(acceptors_ext) =
                    AcceptorsExt::mls_decode(&mut ext.extension_data.as_slice())
            {
                return acceptors_ext.acceptors().to_vec();
            }
        }
        vec![]
    }

    fn extract_acceptors_from_group(external_group: &ExternalGroup<C>) -> Vec<EndpointAddr> {
        let ctx_acceptors =
            Self::extract_acceptors_from_extensions(&external_group.group_context().extensions);
        if !ctx_acceptors.is_empty() {
            return ctx_acceptors;
        }

        vec![]
    }

    fn create_acceptor_from_bytes(
        &self,
        group_info_bytes: &[u8],
    ) -> Result<GroupAcceptor<C, CS>, Report<RegistryError>> {
        let mls_message = MlsMessage::from_bytes(group_info_bytes)
            .change_context(RegistryError)
            .attach("failed to parse MLS message")?;

        let external_group = self
            .external_client
            .observe_group(mls_message, None, None)
            .change_context(RegistryError)
            .attach("failed to observe group")?;

        let acceptors = Self::extract_acceptors_from_group(&external_group);

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
        let group_info_bytes = self.state_store.get_group_info(group_id)?;
        let acceptor = self.create_acceptor_from_bytes(&group_info_bytes).ok()?;
        let state = self.state_store.for_group(*group_id);

        let mut acceptor = acceptor.with_state_store(state.clone());
        acceptor.store_initial_epoch_roster();

        // Replay accepted messages to catch up from epoch 0
        let historical = state.get_accepted_from(acceptor.current_round());

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
    pub fn create_group(
        &self,
        group_info_bytes: &[u8],
    ) -> Result<(GroupId, GroupAcceptor<C, CS>, GroupStateStore), Report<RegistryError>> {
        let mls_message = MlsMessage::from_bytes(group_info_bytes)
            .change_context(RegistryError)
            .attach("failed to parse MLS message")?;

        let external_group = self
            .external_client
            .observe_group(mls_message, None, None)
            .change_context(RegistryError)
            .attach("failed to observe group")?;

        let mls_group_id = external_group.group_context().group_id.clone();
        let group_id = GroupId::from_slice(&mls_group_id);
        let acceptors = Self::extract_acceptors_from_group(&external_group);

        let acceptor = GroupAcceptor::new(
            external_group,
            self.cipher_suite.clone(),
            self.endpoint.secret_key().clone(),
            acceptors,
        );

        self.state_store
            .store_group(&group_id, group_info_bytes)
            .change_context(RegistryError)
            .attach("failed to persist group")?;

        let state = self.state_store.for_group(group_id);

        let acceptor = acceptor.with_state_store(state.clone());
        acceptor.store_initial_epoch_roster();

        self.ensure_epoch_watcher_and_learning(
            group_id,
            acceptor.current_round(),
            &acceptor,
            state.clone(),
        );

        Ok((group_id, acceptor, state))
    }

    pub fn store_message(
        &self,
        group_id: &GroupId,
        id: &universal_sync_core::MessageId,
        msg: &universal_sync_core::EncryptedAppMessage,
    ) -> Result<(), Report<RegistryError>> {
        self.state_store
            .store_app_message(group_id, id, msg)
            .change_context(RegistryError)
            .attach("failed to store message")
    }

    pub fn get_messages_after(
        &self,
        group_id: &GroupId,
        state_vector: &universal_sync_core::StateVector,
    ) -> Vec<(
        universal_sync_core::MessageId,
        universal_sync_core::EncryptedAppMessage,
    )> {
        self.state_store.get_messages_after(group_id, state_vector)
    }

    pub fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> tokio::sync::broadcast::Receiver<(
        universal_sync_core::MessageId,
        universal_sync_core::EncryptedAppMessage,
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
