//! `GroupClient` - high-level client abstraction for creating and joining groups.
//!
//! This module provides [`GroupClient`], which combines an MLS client, iroh endpoint,
//! and connection management into a single convenient type.

use std::collections::HashMap;
use std::sync::Arc;

use error_stack::Report;
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use universal_sync_core::{CrdtFactory, SupportedCrdtsExt};

use crate::MemberAddrExt;
use crate::connection::ConnectionManager;
use crate::error::GroupError;
use crate::group::Group;

/// A high-level client for creating and joining synchronized groups.
///
/// `GroupClient` combines:
/// - An MLS [`Client`] for cryptographic group operations
/// - A [`ConnectionManager`] for peer-to-peer networking
/// - A signing key for authentication
/// - A cipher suite for cryptographic operations
/// - A registry of CRDT factories for different CRDT types
///
/// # CRDT Registry
///
/// The client maintains a registry of CRDT factories keyed by type ID.
/// When joining a group, the client looks up the factory for the group's
/// CRDT type and uses it to construct the CRDT from the welcome snapshot.
///
/// # Example
///
/// ```ignore
/// use universal_sync_proposer::GroupClient;
/// use universal_sync_core::NoCrdtFactory;
///
/// // Create a client
/// let mut group_client = GroupClient::new(mls_client, signer, cipher_suite, endpoint);
///
/// // Register CRDT factories
/// group_client.register_crdt_factory(Box::new(NoCrdtFactory));
///
/// // Create a new group with a specific CRDT
/// let group = group_client.create_group(&[acceptor_addr], "none").await?;
///
/// // Generate a key package to share with others
/// let key_package = group_client.generate_key_package()?;
/// ```
pub struct GroupClient<C, CS> {
    /// The MLS client
    client: Arc<Client<C>>,
    /// The signing secret key
    signer: SignatureSecretKey,
    /// The cipher suite provider
    cipher_suite: CS,
    /// The connection manager for p2p connections
    connection_manager: ConnectionManager,
    /// Registry of CRDT factories by type ID
    crdt_factories: HashMap<String, Arc<dyn CrdtFactory>>,
}

impl<C, CS> GroupClient<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new `GroupClient`.
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key for this client
    /// * `cipher_suite` - The cipher suite provider
    /// * `endpoint` - The iroh endpoint for networking
    pub fn new(
        client: Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: Endpoint,
    ) -> Self {
        Self {
            client: Arc::new(client),
            signer,
            cipher_suite,
            connection_manager: ConnectionManager::new(endpoint),
            crdt_factories: HashMap::new(),
        }
    }

    /// Register a CRDT factory for a specific type ID.
    ///
    /// The factory's `type_id()` is used as the key. If a factory with the
    /// same type ID already exists, it will be replaced.
    pub fn register_crdt_factory(&mut self, factory: impl CrdtFactory + 'static) {
        let type_id = factory.type_id().to_owned();
        self.crdt_factories.insert(type_id, Arc::new(factory));
    }

    /// Get the list of supported CRDT type IDs.
    #[must_use]
    pub fn supported_crdt_types(&self) -> Vec<&str> {
        self.crdt_factories.keys().map(String::as_str).collect()
    }

    /// Get a CRDT factory by type ID.
    #[must_use]
    pub fn get_crdt_factory(&self, type_id: &str) -> Option<&dyn CrdtFactory> {
        self.crdt_factories.get(type_id).map(Arc::as_ref)
    }

    /// Get this client's endpoint address.
    ///
    /// This is the address other peers use to connect to this client.
    #[must_use]
    pub fn addr(&self) -> EndpointAddr {
        self.connection_manager.endpoint().addr()
    }

    /// Get a reference to the iroh endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        self.connection_manager.endpoint()
    }

    /// Get a reference to the connection manager.
    #[must_use]
    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    /// Generate a key package for joining a group.
    ///
    /// The key package includes:
    /// - This client's endpoint address (for the inviter to send the welcome)
    /// - List of supported CRDT types (from registered factories)
    ///
    /// # Errors
    /// Returns an error if key package generation fails.
    pub fn generate_key_package(&self) -> Result<MlsMessage, Report<GroupError>> {
        // Include our endpoint address in the key package
        let member_addr_ext = MemberAddrExt::new(self.connection_manager.endpoint().addr());
        let mut kp_extensions = ExtensionList::default();
        kp_extensions.set_from(member_addr_ext).map_err(|e| {
            Report::new(GroupError).attach(format!("failed to set member address extension: {e:?}"))
        })?;

        // Include supported CRDT types
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
    /// # Arguments
    /// * `acceptors` - Endpoint addresses of acceptors to register with (can be empty)
    /// * `crdt_type_id` - The CRDT type ID to use for this group (must be registered)
    ///
    /// # Errors
    /// Returns an error if the CRDT type is not registered, or if group creation
    /// or acceptor registration fails.
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
            crdt_factory.as_ref(),
        )
        .await
    }

    /// Join an existing group using a welcome message.
    ///
    /// The CRDT factory is automatically looked up from the registry based on
    /// the group's CRDT registration extension in the group context. If the
    /// group uses a CRDT type that is not registered, this will fail.
    ///
    /// # Arguments
    /// * `welcome_bytes` - The serialized welcome message received from the inviter
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

    /// Wait for a welcome message from an inviter.
    ///
    /// This listens for an incoming connection with a `SendWelcome` handshake
    /// and returns the welcome bytes.
    ///
    /// # Errors
    /// Returns an error if receiving the welcome fails.
    pub async fn wait_for_welcome(&self) -> Result<Vec<u8>, Report<GroupError>> {
        crate::group::wait_for_welcome(self.connection_manager.endpoint()).await
    }
}

impl<C, CS> Clone for GroupClient<C, CS>
where
    CS: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            signer: self.signer.clone(),
            cipher_suite: self.cipher_suite.clone(),
            connection_manager: self.connection_manager.clone(),
            crdt_factories: self.crdt_factories.clone(),
        }
    }
}
