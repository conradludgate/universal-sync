//! Test utilities for Universal Sync integration tests.

pub mod yrs_crdt;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use iroh::{Endpoint, EndpointAddr, RelayMode};
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use universal_sync_acceptor::{AcceptorRegistry, SharedFjallStateStore, accept_connection};
use universal_sync_core::{PAXOS_ALPN, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE};
use universal_sync_proposer::{GroupClient, ReplContext};
pub use yrs_crdt::{AWARENESS_TIMEOUT, YrsCrdt};

pub const TEST_CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

#[must_use]
pub fn test_crypto_provider() -> RustCryptoProvider {
    RustCryptoProvider::default()
}

/// # Panics
/// Panics if the cipher suite is not available.
#[must_use]
pub fn test_cipher_suite(
    crypto: &RustCryptoProvider,
) -> <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider {
    crypto
        .cipher_suite_provider(TEST_CIPHER_SUITE)
        .expect("cipher suite should be available")
}

#[must_use]
pub fn test_identity_provider() -> BasicIdentityProvider {
    BasicIdentityProvider::new()
}

pub type TestCipherSuiteProvider = <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider;

pub struct TestClientResult<C> {
    pub client: Client<C>,
    pub signer: SignatureSecretKey,
    pub cipher_suite: TestCipherSuiteProvider,
}

/// # Panics
/// Panics if key generation or client building fails.
#[must_use]
pub fn test_client(name: &str) -> TestClientResult<impl mls_rs::client_builder::MlsConfig> {
    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    let credential = BasicCredential::new(name.as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    let client = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(test_identity_provider())
        .signing_identity(signing_identity, secret_key.clone(), TEST_CIPHER_SUITE)
        .extension_type(SYNC_EXTENSION_TYPE)
        .custom_proposal_type(SYNC_PROPOSAL_TYPE)
        .build();

    TestClientResult {
        client,
        signer: secret_key,
        cipher_suite,
    }
}

/// # Panics
/// Panics if key generation or client building fails.
#[must_use]
pub fn test_group_client(
    name: &'static str,
    endpoint: iroh::Endpoint,
) -> GroupClient<impl mls_rs::client_builder::MlsConfig, TestCipherSuiteProvider> {
    let result = test_client(name);
    GroupClient::new(result.client, result.signer, result.cipher_suite, endpoint)
}

/// # Panics
/// Panics if key generation or client building fails.
#[must_use]
pub fn test_repl_context(
    name: &'static str,
    endpoint: iroh::Endpoint,
) -> ReplContext<impl mls_rs::client_builder::MlsConfig, TestCipherSuiteProvider> {
    let client = test_group_client(name, endpoint);
    ReplContext::new(client)
}

/// Alias for [`test_group_client`] (factory registration is no longer needed).
#[must_use]
pub fn test_yrs_group_client(
    name: &'static str,
    endpoint: Endpoint,
) -> GroupClient<impl mls_rs::client_builder::MlsConfig, TestCipherSuiteProvider> {
    test_group_client(name, endpoint)
}

/// Safe to call multiple times.
pub fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("universal_sync=debug")),
        )
        .with_test_writer()
        .try_init();
}

/// Localhost-only endpoint with relays disabled for fast local testing.
///
/// # Panics
/// Panics if the endpoint fails to bind.
pub async fn test_endpoint() -> Endpoint {
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()))
        .build();

    Endpoint::empty_builder(RelayMode::Disabled)
        .transport_config(transport_config)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind_addr(bind_addr)
        .expect("valid bind address")
        .bind()
        .await
        .expect("failed to create endpoint")
}

/// Returns (task handle, acceptor address, temp dir).
/// Keep the `TempDir` alive for the acceptor's state store.
///
/// # Panics
/// Panics if the acceptor fails to start.
pub async fn spawn_acceptor() -> (tokio::task::JoinHandle<()>, EndpointAddr, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let endpoint = test_endpoint().await;
    let addr = endpoint.addr();

    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    let state_store = SharedFjallStateStore::open(temp_dir.path())
        .await
        .expect("open state store");

    let external_client = ExternalClient::builder()
        .crypto_provider(crypto)
        .identity_provider(test_identity_provider())
        .build();

    let registry =
        AcceptorRegistry::new(external_client, cipher_suite, state_store, endpoint.clone());

    let task = tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            loop {
                if let Some(incoming) = endpoint.accept().await {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        let _ = accept_connection(incoming, registry).await;
                    });
                }
            }
        }
    });

    (task, addr, temp_dir)
}
