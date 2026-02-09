//! High-level client abstraction for creating and joining weavers.

use std::sync::Arc;

use error_stack::{Report, ResultExt};
use filament_core::{
    AcceptorId, KeyPackageExt, LeafNodeExt, QrPayload, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE,
};
use iroh::Endpoint;
use mls_rs::client_builder::{BaseConfig, WithCryptoProvider, WithIdentityProvider};
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider, ExtensionList};
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
        let connection_manager = ConnectionManager::new(endpoint);

        let endpoint_clone = connection_manager.endpoint().clone();
        tokio::spawn(async move {
            Self::welcome_acceptor_loop(endpoint_clone, welcome_tx).await;
        });

        Self {
            client: Arc::new(client),
            signer: secret_key,
            cipher_suite,
            connection_manager,
            welcome_rx,
        }
    }

    async fn welcome_acceptor_loop(endpoint: Endpoint, tx: mpsc::Sender<bytes::Bytes>) {
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

    /// Join a group via an external commit using a [`QrPayload`].
    ///
    /// 1. Connects to the spool specified in the payload.
    /// 2. Fetches the encrypted `GroupInfo`.
    /// 3. Decrypts it using the key/nonce from the payload.
    /// 4. Creates an external commit.
    /// 5. Submits the commit back to the spool.
    ///
    /// # Errors
    ///
    /// Returns [`WeaverError`] if any step fails.
    pub async fn join_external(&self, qr: &QrPayload) -> Result<JoinInfo, Report<WeaverError>> {
        use filament_core::{Handshake, HandshakeResponse, PAXOS_ALPN, decrypt_group_info};
        use futures::{SinkExt, StreamExt};
        use iroh::PublicKey;

        let endpoint = self.connection_manager.endpoint();
        let spool_public_key = PublicKey::from_bytes(qr.spool_id.as_bytes())
            .map_err(|_| Report::new(WeaverError).attach("invalid spool public key"))?;

        let send_handshake = |handshake: Handshake| {
            let endpoint = endpoint.clone();
            async move {
                let conn = endpoint
                    .connect(spool_public_key, PAXOS_ALPN)
                    .await
                    .change_context(WeaverError)
                    .attach("failed to connect to spool")?;

                let (send, recv) = conn.open_bi().await.change_context(WeaverError)?;
                let codec = tokio_util::codec::LengthDelimitedCodec::builder()
                    .max_frame_length(16 * 1024 * 1024)
                    .new_codec();
                let mut writer = tokio_util::codec::FramedWrite::new(send, codec.clone());
                let mut reader = tokio_util::codec::FramedRead::new(recv, codec);

                let hs_bytes = postcard::to_allocvec(&handshake).change_context(WeaverError)?;

                writer
                    .send(hs_bytes.into())
                    .await
                    .change_context(WeaverError)?;

                let resp_bytes = reader
                    .next()
                    .await
                    .ok_or_else(|| Report::new(WeaverError).attach("spool closed before response"))?
                    .change_context(WeaverError)?;

                let resp: HandshakeResponse =
                    postcard::from_bytes(&resp_bytes).change_context(WeaverError)?;

                Ok::<_, Report<WeaverError>>(resp)
            }
        };

        // Fetch encrypted `GroupInfo` from spool.
        let resp = send_handshake(Handshake::FetchGroupInfo {
            group_id: qr.group_id,
        })
        .await?;

        let ciphertext = match resp {
            HandshakeResponse::Data(data) => data,
            HandshakeResponse::GroupNotFound => {
                return Err(
                    Report::new(WeaverError).attach("encrypted GroupInfo not found on spool")
                );
            }
            other => {
                return Err(Report::new(WeaverError)
                    .attach(format!("unexpected spool response: {other:?}")));
            }
        };

        // Decrypt GroupInfo.
        let group_info_bytes = decrypt_group_info(&qr.key, &qr.nonce, &ciphertext)
            .map_err(|_| Report::new(WeaverError).attach("GroupInfo decryption failed"))?;

        // Create external commit.
        let (join_info, commit_bytes) = Weaver::join_external(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.connection_manager,
            &group_info_bytes,
        )
        .await?;

        // Submit external commit to spool.
        let resp = send_handshake(Handshake::ExternalCommit {
            group_id: qr.group_id,
            commit: commit_bytes.into(),
        })
        .await?;

        match resp {
            HandshakeResponse::Ok => {}
            HandshakeResponse::Error(e) => {
                return Err(
                    Report::new(WeaverError).attach(format!("external commit rejected: {e}"))
                );
            }
            other => {
                return Err(Report::new(WeaverError)
                    .attach(format!("unexpected commit response: {other:?}")));
            }
        }

        Ok(join_info)
    }
}

// Not Clone: welcome_rx can only have one receiver.
