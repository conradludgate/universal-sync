//! Group registry implementation
//!
//! Provides a concrete implementation of [`GroupRegistry`] for managing
//! multiple groups on an acceptor server.

use std::sync::Arc;

use iroh::SecretKey;
use mls_rs::external_client::builder::MlsConfig as ExternalMlsConfig;
use mls_rs::external_client::{ExternalClient, ExternalGroup};
use mls_rs::mls_rs_codec::MlsDecode;
use mls_rs::{CipherSuiteProvider, ExtensionList, MlsMessage};
use universal_sync_core::{ACCEPTORS_EXTENSION_TYPE, AcceptorId, AcceptorsExt, GroupId};
use universal_sync_paxos::Learner;

use crate::acceptor::GroupAcceptor;
use crate::server::GroupRegistry;
use crate::state_store::{GroupStateStore, SharedFjallStateStore};

/// Concrete implementation of [`GroupRegistry`]
///
/// Manages multiple MLS groups for an acceptor server. Each group is
/// identified by its 32-byte [`GroupId`].
///
/// Groups are persisted to the state store and reconstructed on each
/// access. This ensures groups survive server restarts.
///
/// # Type Parameters
/// * `C` - The MLS external client config type
/// * `CS` - The cipher suite provider type
#[derive(Clone)]
pub struct AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    /// The external client for creating new groups (shared)
    external_client: Arc<ExternalClient<C>>,

    /// Cipher suite provider (cloned for each group)
    cipher_suite: CS,

    /// Persistent state store (shared across all groups)
    state_store: SharedFjallStateStore,

    /// This acceptor's iroh secret key
    secret_key: SecretKey,
}

impl<C, CS> AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    /// Create a new registry
    ///
    /// # Arguments
    /// * `external_client` - MLS external client for joining groups
    /// * `cipher_suite` - Cipher suite provider for signature verification
    /// * `state_store` - Persistent state store for all groups
    /// * `secret_key` - This acceptor's iroh secret key
    pub fn new(
        external_client: ExternalClient<C>,
        cipher_suite: CS,
        state_store: SharedFjallStateStore,
        secret_key: SecretKey,
    ) -> Self {
        Self {
            external_client: Arc::new(external_client),
            cipher_suite,
            state_store,
            secret_key,
        }
    }

    /// Get this acceptor's own ID (derived from secret key)
    pub fn own_id(&self) -> AcceptorId {
        AcceptorId::from_bytes(*self.secret_key.public().as_bytes())
    }

    /// Get the state store
    pub fn state_store(&self) -> &SharedFjallStateStore {
        &self.state_store
    }

    /// List all registered groups
    pub fn list_groups(&self) -> Vec<GroupId> {
        self.state_store.list_groups()
    }

    /// Extract acceptors from an extension list
    fn extract_acceptors_from_extensions(extensions: &ExtensionList) -> Vec<AcceptorId> {
        for ext in extensions.iter() {
            if ext.extension_type == ACCEPTORS_EXTENSION_TYPE
                && let Ok(acceptors_ext) =
                    AcceptorsExt::mls_decode(&mut ext.extension_data.as_slice())
            {
                return acceptors_ext.acceptor_ids();
            }
        }
        vec![]
    }

    /// Extract acceptors from an external group's context extensions
    fn extract_acceptors_from_group<C2: ExternalMlsConfig + Clone>(
        external_group: &ExternalGroup<C2>,
    ) -> Vec<AcceptorId> {
        // Try group context extensions first
        let ctx_acceptors =
            Self::extract_acceptors_from_extensions(&external_group.group_context().extensions);
        if !ctx_acceptors.is_empty() {
            return ctx_acceptors;
        }

        // Fall back to group info extensions if available
        // Note: For newly created groups, acceptors are typically in GroupInfo extensions
        // set via set_group_info_ext(). When the acceptor observes the group, these
        // may not be directly accessible, so we rely on the creator setting them
        // in the group context extensions as well, or storing them separately.
        vec![]
    }

    /// Create an acceptor from stored `GroupInfo` bytes
    fn create_acceptor_from_bytes(
        &self,
        group_info_bytes: &[u8],
    ) -> Result<GroupAcceptor<C, CS>, String> {
        let mls_message = MlsMessage::from_bytes(group_info_bytes)
            .map_err(|e| format!("failed to parse MLS message: {e}"))?;

        let external_group = self
            .external_client
            .observe_group(mls_message, None)
            .map_err(|e| format!("failed to observe group: {e}"))?;

        // Extract acceptors from the group's extensions
        let acceptors = Self::extract_acceptors_from_group(&external_group);

        Ok(GroupAcceptor::new(
            external_group,
            self.cipher_suite.clone(),
            self.secret_key.clone(),
            acceptors,
        ))
    }
}

impl<C, CS> GroupRegistry for AcceptorRegistry<C, CS>
where
    C: ExternalMlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    type Acceptor = GroupAcceptor<C, CS>;
    type StateStore = GroupStateStore;

    fn get_group(&self, group_id: &GroupId) -> Option<(Self::Acceptor, Self::StateStore)> {
        // Load GroupInfo from persistent storage
        let group_info_bytes = self.state_store.get_group_info(group_id)?;

        // Recreate the acceptor from stored GroupInfo
        let mut acceptor = self.create_acceptor_from_bytes(&group_info_bytes).ok()?;

        let state = self.state_store.for_group(*group_id);

        // Replay all accepted messages to bring the acceptor up to date
        // This is necessary because the GroupInfo we stored is from epoch 0,
        // but the acceptor may have processed commits since then.
        let historical = state.get_accepted_from(acceptor.current_round());

        for (proposal, message) in historical {
            tracing::debug!(epoch = ?proposal.epoch, "replaying commit for acceptor");
            // Apply the message to catch up the MLS state
            // Note: We ignore errors here since the message was already accepted
            if let Err(e) =
                futures::executor::block_on(Learner::apply(&mut acceptor, proposal, message))
            {
                tracing::warn!(?e, "failed to replay message");
            }
        }

        Some((acceptor, state))
    }

    fn create_group(
        &self,
        group_info_bytes: &[u8],
    ) -> Result<(GroupId, Self::Acceptor, Self::StateStore), String> {
        // Parse and observe the group
        let mls_message = MlsMessage::from_bytes(group_info_bytes)
            .map_err(|e| format!("failed to parse MLS message: {e}"))?;

        let external_group = self
            .external_client
            .observe_group(mls_message, None)
            .map_err(|e| format!("failed to observe group: {e}"))?;

        // Extract the group ID
        let mls_group_id = external_group.group_context().group_id.clone();
        let group_id = GroupId::from_slice(&mls_group_id);

        // Extract acceptors from the group's extensions
        let acceptors = Self::extract_acceptors_from_group(&external_group);

        // Create the acceptor
        let acceptor = GroupAcceptor::new(
            external_group,
            self.cipher_suite.clone(),
            self.secret_key.clone(),
            acceptors,
        );

        // Persist the GroupInfo bytes
        self.state_store
            .store_group(&group_id, group_info_bytes)
            .map_err(|e| format!("failed to persist group: {e}"))?;

        let state = self.state_store.for_group(group_id);

        Ok((group_id, acceptor, state))
    }

    fn store_message(
        &self,
        group_id: &GroupId,
        msg: &universal_sync_core::EncryptedAppMessage,
    ) -> Result<u64, String> {
        self.state_store
            .store_app_message(group_id, msg)
            .map_err(|e| format!("failed to store message: {e}"))
    }

    fn get_messages_since(
        &self,
        group_id: &GroupId,
        since_seq: u64,
    ) -> Result<Vec<(u64, universal_sync_core::EncryptedAppMessage)>, String> {
        Ok(self.state_store.get_messages_since(group_id, since_seq))
    }

    fn subscribe_messages(
        &self,
        group_id: &GroupId,
    ) -> tokio::sync::broadcast::Receiver<universal_sync_core::EncryptedAppMessage> {
        self.state_store.subscribe_messages(group_id)
    }
}
