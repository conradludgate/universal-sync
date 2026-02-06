//! High-level client abstraction for creating and joining groups.

use std::collections::HashMap;
use std::sync::Arc;

use error_stack::Report;
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use tokio::sync::mpsc;
use universal_sync_core::{CrdtFactory, MemberAddrExt, SupportedCrdtsExt};

use crate::connection::ConnectionManager;
use crate::group::{Group, GroupError};

/// Combines an MLS client, iroh endpoint, connection manager, and CRDT factory
/// registry. When joining a group, looks up the factory for the group's CRDT type
/// to construct the CRDT from the welcome snapshot.
pub struct GroupClient<C, CS> {
    client: Arc<Client<C>>,
    signer: SignatureSecretKey,
    cipher_suite: CS,
    connection_manager: ConnectionManager,
    crdt_factories: HashMap<String, Arc<dyn CrdtFactory>>,
    welcome_rx: mpsc::Receiver<Vec<u8>>,
}

impl<C, CS> GroupClient<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Spawns a background task to listen for incoming welcome messages.
    pub fn new(
        client: Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: Endpoint,
    ) -> Self {
        let (welcome_tx, welcome_rx) = mpsc::channel(16);
        let connection_manager = ConnectionManager::new(endpoint);

        let endpoint_clone = connection_manager.endpoint().clone();
        tokio::spawn(async move {
            Self::welcome_acceptor_loop(endpoint_clone, welcome_tx).await;
        });

        Self {
            client: Arc::new(client),
            signer,
            cipher_suite,
            connection_manager,
            crdt_factories: HashMap::new(),
            welcome_rx,
        }
    }

    async fn welcome_acceptor_loop(endpoint: Endpoint, tx: mpsc::Sender<Vec<u8>>) {
        loop {
            match crate::group::wait_for_welcome(&endpoint).await {
                Ok(welcome_bytes) => {
                    if tx.send(welcome_bytes).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(?e, "welcome acceptor error (may be normal on shutdown)");
                }
            }
        }
    }

    /// Register a CRDT factory. The factory's `type_id()` is used as the key.
    pub fn register_crdt_factory(&mut self, factory: impl CrdtFactory + 'static) {
        let type_id = factory.type_id().to_owned();
        self.crdt_factories.insert(type_id, Arc::new(factory));
    }

    /// Generate a key package containing this client's endpoint address and
    /// supported CRDT types.
    ///
    /// # Errors
    /// Returns an error if key package generation fails.
    pub fn generate_key_package(&self) -> Result<MlsMessage, Report<GroupError>> {
        let member_addr_ext = MemberAddrExt::new(self.connection_manager.endpoint().addr());
        let mut kp_extensions = ExtensionList::default();
        kp_extensions.set_from(member_addr_ext).map_err(|e| {
            Report::new(GroupError).attach(format!("failed to set member address extension: {e:?}"))
        })?;

        let supported_crdts = SupportedCrdtsExt::new(self.crdt_factories.keys().cloned());
        kp_extensions.set_from(supported_crdts).map_err(|e| {
            Report::new(GroupError)
                .attach(format!("failed to set supported CRDTs extension: {e:?}"))
        })?;

        self.client
            .generate_key_package_message(kp_extensions, ExtensionList::default(), None)
            .map_err(|e| {
                Report::new(GroupError).attach(format!("failed to generate key package: {e:?}"))
            })
    }

    /// Create a new group, optionally registering with acceptors.
    ///
    /// # Errors
    /// Returns an error if `crdt_type_id` is not registered, or if group creation fails.
    pub async fn create_group(
        &self,
        acceptors: &[EndpointAddr],
        crdt_type_id: &str,
    ) -> Result<Group<C, CS>, Report<GroupError>> {
        let crdt_factory = self.crdt_factories.get(crdt_type_id).ok_or_else(|| {
            Report::new(GroupError).attach(format!(
                "CRDT type '{crdt_type_id}' not registered. Register a factory with register_crdt_factory()"
            ))
        })?;

        Group::create(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            acceptors,
            crdt_factory.clone(),
        )
        .await
    }

    /// Join an existing group using a welcome message. The CRDT factory is looked
    /// up from the registry based on the group's CRDT registration extension.
    ///
    /// # Errors
    /// Returns an error if the CRDT type is not registered or if joining fails.
    pub async fn join_group(
        &self,
        welcome_bytes: &[u8],
    ) -> Result<Group<C, CS>, Report<GroupError>> {
        Group::join(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            welcome_bytes,
            &self.crdt_factories,
        )
        .await
    }

    /// Receive the next welcome message, or wait for one. Returns `None` on shutdown.
    pub async fn recv_welcome(&mut self) -> Option<Vec<u8>> {
        self.welcome_rx.recv().await
    }

    /// Non-blocking variant of [`recv_welcome`](Self::recv_welcome).
    pub fn try_recv_welcome(&mut self) -> Option<Vec<u8>> {
        self.welcome_rx.try_recv().ok()
    }

    /// Take the welcome receiver for use in a `select!` loop.
    /// After this, `recv_welcome`/`try_recv_welcome` will always return `None`.
    pub fn take_welcome_rx(&mut self) -> mpsc::Receiver<Vec<u8>> {
        let (_, empty_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.welcome_rx, empty_rx)
    }
}

// Not Clone: welcome_rx can only have one receiver.
