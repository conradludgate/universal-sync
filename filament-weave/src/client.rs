//! High-level client abstraction for creating and joining weavers.

use std::sync::Arc;

use error_stack::{Report, ResultExt};
use filament_core::{CompactionConfig, KeyPackageExt, LeafNodeExt, default_compaction_config};
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use tokio::sync::mpsc;

use crate::connection::ConnectionManager;
use crate::group::{JoinInfo, Weaver, WeaverError};

/// Entry point for creating and joining synchronized groups.
///
/// Wraps an MLS client, iroh endpoint, and connection manager.
pub struct WeaverClient<C, CS> {
    client: Arc<Client<C>>,
    signer: SignatureSecretKey,
    cipher_suite: CS,
    connection_manager: ConnectionManager,
    welcome_rx: mpsc::Receiver<Vec<u8>>,
}

impl<C, CS> WeaverClient<C, CS>
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

    /// Generate a key package containing this client's endpoint address.
    ///
    /// # Errors
    /// Returns an error if key package generation fails.
    pub fn generate_key_package(&self) -> Result<MlsMessage, Report<WeaverError>> {
        let kp_ext = KeyPackageExt::new(
            self.connection_manager.endpoint().addr(),
            std::iter::empty::<String>(),
        );
        let mut kp_extensions = ExtensionList::default();
        kp_extensions.set_from(kp_ext).change_context(WeaverError)?;

        let ln_ext = LeafNodeExt::random();
        let mut ln_extensions = ExtensionList::default();
        ln_extensions.set_from(ln_ext).change_context(WeaverError)?;

        self.client
            .generate_key_package_message(kp_extensions, ln_extensions, None)
            .change_context(WeaverError)
    }

    /// Create a new weaver with the default compaction config.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    pub async fn create(
        &self,
        acceptors: &[EndpointAddr],
        protocol_name: &str,
    ) -> Result<Weaver<C, CS>, Report<WeaverError>> {
        self.create_with_config(acceptors, protocol_name, default_compaction_config())
            .await
    }

    /// Create a weaver with a custom compaction config.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    pub async fn create_with_config(
        &self,
        acceptors: &[EndpointAddr],
        protocol_name: &str,
        compaction_config: CompactionConfig,
    ) -> Result<Weaver<C, CS>, Report<WeaverError>> {
        Weaver::create(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            acceptors,
            protocol_name,
            compaction_config,
        )
        .await
    }

    /// Join an existing group using a welcome message. Returns the group
    /// handle along with the protocol name and optional CRDT snapshot
    /// so the caller can construct the appropriate CRDT.
    ///
    /// # Errors
    /// Returns an error if joining fails.
    pub async fn join(&self, welcome_bytes: &[u8]) -> Result<JoinInfo<C, CS>, Report<WeaverError>> {
        Weaver::join(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            welcome_bytes,
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
