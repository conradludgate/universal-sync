//! High-level client abstraction for creating and joining weavers.

use std::sync::Arc;

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, KeyPackageExt, LeafNodeExt, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE,
};
use iroh::Endpoint;
use mls_rs::client_builder::{BaseConfig, WithCryptoProvider, WithIdentityProvider};
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider, ExtensionList, MlsMessage};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tokio::sync::mpsc;

use crate::connection::ConnectionManager;
use crate::group::{JoinInfo, Weaver, WeaverError};

type WeaverMlsConfig =
    WithIdentityProvider<BasicIdentityProvider, WithCryptoProvider<RustCryptoProvider, BaseConfig>>;
type WeaverCipherSuite = <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider;

const CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

/// Entry point for creating and joining synchronized groups.
pub struct WeaverClient {
    client: Arc<Client<WeaverMlsConfig>>,
    signer: SignatureSecretKey,
    cipher_suite: WeaverCipherSuite,
    connection_manager: ConnectionManager,
    welcome_rx: mpsc::Receiver<bytes::Bytes>,
    key_package_rx: mpsc::Receiver<(filament_core::GroupId, Vec<u8>)>,
}

impl WeaverClient {
    /// Create a new `WeaverClient`.
    ///
    /// Internally constructs the MLS client with `RustCryptoProvider` and
    /// `BasicIdentityProvider`. Spawns a background task to listen for
    /// incoming welcome messages on the iroh endpoint.
    ///
    /// # Panics
    ///
    /// Panics if key generation or client building fails (should not happen
    /// with a correctly compiled `RustCryptoProvider`).
    pub fn new(identity: impl Into<Vec<u8>>, endpoint: Endpoint) -> Self {
        let crypto = RustCryptoProvider::default();
        let cipher_suite = crypto
            .cipher_suite_provider(CIPHER_SUITE)
            .expect("cipher suite should be available");

        let (secret_key, public_key) = cipher_suite
            .signature_key_generate()
            .expect("key generation should succeed");

        let credential = BasicCredential::new(identity.into());
        let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

        let client: Client<WeaverMlsConfig> = Client::builder()
            .crypto_provider(crypto)
            .identity_provider(BasicIdentityProvider::new())
            .signing_identity(signing_identity, secret_key.clone(), CIPHER_SUITE)
            .extension_type(SYNC_EXTENSION_TYPE)
            .custom_proposal_type(SYNC_PROPOSAL_TYPE)
            .build();

        let (welcome_tx, welcome_rx) = mpsc::channel(16);
        let (key_package_tx, key_package_rx) = mpsc::channel(16);
        let connection_manager = ConnectionManager::new(endpoint);

        let endpoint_clone = connection_manager.endpoint().clone();
        tokio::spawn(async move {
            Self::incoming_loop(endpoint_clone, welcome_tx, key_package_tx).await;
        });

        Self {
            client: Arc::new(client),
            signer: secret_key,
            cipher_suite,
            connection_manager,
            welcome_rx,
            key_package_rx,
        }
    }

    async fn incoming_loop(
        endpoint: Endpoint,
        welcome_tx: mpsc::Sender<bytes::Bytes>,
        key_package_tx: mpsc::Sender<(filament_core::GroupId, Vec<u8>)>,
    ) {
        use crate::group::IncomingHandshake;
        loop {
            match crate::group::wait_for_incoming(&endpoint).await {
                Ok(IncomingHandshake::Welcome(bytes)) => {
                    if welcome_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                Ok(IncomingHandshake::KeyPackage(group_id, bytes)) => {
                    if key_package_tx.send((group_id, bytes)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(?e, "incoming handshake error (may be normal on shutdown)");
                }
            }
        }
    }

    /// Returns this client's iroh endpoint public key (32 bytes).
    #[must_use]
    pub fn endpoint_id(&self) -> [u8; 32] {
        *self.connection_manager.endpoint().id().as_bytes()
    }

    /// Generate a serialised key package containing this client's endpoint identity.
    ///
    /// # Errors
    /// Returns an error if key package generation or serialisation fails.
    pub fn generate_key_package(&self) -> Result<Vec<u8>, Report<WeaverError>> {
        let kp_ext = KeyPackageExt::new(
            *self.connection_manager.endpoint().id().as_bytes(),
            std::iter::empty::<String>(),
        );
        let mut kp_extensions = ExtensionList::default();
        kp_extensions.set_from(kp_ext).change_context(WeaverError)?;

        let ln_ext = LeafNodeExt::random();
        let mut ln_extensions = ExtensionList::default();
        ln_extensions.set_from(ln_ext).change_context(WeaverError)?;

        self.client
            .generate_key_package_message(kp_extensions, ln_extensions, None)
            .change_context(WeaverError)?
            .to_bytes()
            .change_context(WeaverError)
    }

    /// Create a new weaver.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    pub async fn create(
        &self,
        acceptors: &[AcceptorId],
        protocol_name: &str,
    ) -> Result<Weaver, Report<WeaverError>> {
        Weaver::create(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            acceptors,
            protocol_name,
        )
        .await
    }

    /// Join an existing group using a welcome message. Returns the group
    /// handle along with the protocol name and optional CRDT snapshot
    /// so the caller can construct the appropriate CRDT.
    ///
    /// # Errors
    /// Returns an error if joining fails.
    pub async fn join(&self, welcome_bytes: &[u8]) -> Result<JoinInfo, Report<WeaverError>> {
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
    pub async fn recv_welcome(&mut self) -> Option<bytes::Bytes> {
        self.welcome_rx.recv().await
    }

    /// Non-blocking variant of [`recv_welcome`](Self::recv_welcome).
    pub fn try_recv_welcome(&mut self) -> Option<bytes::Bytes> {
        self.welcome_rx.try_recv().ok()
    }

    /// Take the welcome receiver for use in a `select!` loop.
    /// After this, `recv_welcome`/`try_recv_welcome` will always return `None`.
    pub fn take_welcome_rx(&mut self) -> mpsc::Receiver<bytes::Bytes> {
        let (_, empty_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.welcome_rx, empty_rx)
    }

    /// Receive the next key package sent by a remote peer, or wait for one.
    /// Returns `None` on shutdown.
    pub async fn recv_key_package(&mut self) -> Option<(filament_core::GroupId, Vec<u8>)> {
        self.key_package_rx.recv().await
    }

    /// Take the key-package receiver for use in a `select!` loop.
    /// After this, `recv_key_package` will always return `None`.
    pub fn take_key_package_rx(&mut self) -> mpsc::Receiver<(filament_core::GroupId, Vec<u8>)> {
        let (_, empty_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.key_package_rx, empty_rx)
    }

    /// Send this client's key package to a remote peer, requesting to join
    /// the given group. The remote peer will receive a `KeyPackage` event
    /// and can call `add_member` with it.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if key package generation, connecting, or
    /// sending fails.
    pub async fn send_key_package(
        &self,
        target: [u8; 32],
        group_id: filament_core::GroupId,
    ) -> Result<(), Report<WeaverError>> {
        use filament_core::{Handshake, PAXOS_ALPN};
        use futures::SinkExt;
        use iroh::PublicKey;

        let key_package_bytes = self.generate_key_package()?;
        let key_package = MlsMessage::from_bytes(&key_package_bytes).change_context(WeaverError)?;

        let public_key = PublicKey::from_bytes(&target)
            .map_err(|_| Report::new(WeaverError).attach("invalid target public key"))?;

        let endpoint = self.connection_manager.endpoint();
        let conn = endpoint
            .connect(public_key, PAXOS_ALPN)
            .await
            .change_context(WeaverError)
            .attach("failed to connect to target")?;

        let (send, _recv) = conn.open_bi().await.change_context(WeaverError)?;
        let mut framed = tokio_util::codec::FramedWrite::new(
            send,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );

        let handshake = Handshake::SendKeyPackage {
            group_id,
            key_package,
        };
        let hs_bytes = postcard::to_allocvec(&handshake).change_context(WeaverError)?;
        framed
            .send(hs_bytes.into())
            .await
            .change_context(WeaverError)?;

        let mut send = framed.into_inner();
        send.finish().change_context(WeaverError)?;
        // stopped() may return an error if the peer closes the connection
        // before explicitly stopping the stream — that's fine, finish()
        // already ensures the data is flushed.
        let _ = send.stopped().await;

        Ok(())
    }
}

// Not Clone: welcome_rx can only have one receiver.
