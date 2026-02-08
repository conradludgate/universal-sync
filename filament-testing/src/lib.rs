//! Test utilities for Universal Sync integration tests.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use filament_core::{PAXOS_ALPN, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE};
pub use filament_editor::{PeerAwareness, YrsCrdt};
use filament_spool::{
    AcceptorMetrics, AcceptorRegistry, MetricsEncoder, SharedFjallStateStore, accept_connection,
};
use filament_weave::WeaverClient;
use iroh::{Endpoint, EndpointAddr, RelayMode};
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::basic::BasicIdentityProvider;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};

const TEST_CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

#[must_use]
pub fn test_weaver_client(name: &str, endpoint: Endpoint) -> WeaverClient {
    WeaverClient::new(name.as_bytes().to_vec(), endpoint)
}

/// Alias for [`test_weaver_client`].
#[must_use]
pub fn test_yrs_weaver_client(name: &str, endpoint: Endpoint) -> WeaverClient {
    test_weaver_client(name, endpoint)
}

/// Safe to call multiple times.
pub fn init_tracing() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("filament=debug")),
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

    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(TEST_CIPHER_SUITE)
        .expect("cipher suite should be available");

    let state_store = SharedFjallStateStore::open(temp_dir.path())
        .await
        .expect("open state store");

    let external_client = ExternalClient::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .extension_type(SYNC_EXTENSION_TYPE)
        .custom_proposal_types(Some(SYNC_PROPOSAL_TYPE))
        .build();

    let metrics = AcceptorMetrics::new(state_store.clone());
    let metrics_encoder = std::sync::Arc::new(MetricsEncoder::new(metrics));

    let registry = AcceptorRegistry::new(
        external_client,
        cipher_suite,
        state_store,
        endpoint.clone(),
        metrics_encoder,
    );

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
