//! Test utilities for Universal Sync integration tests.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};

use filament_core::{ALPN, AcceptorId, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE};
pub use filament_editor::{PeerAwareness, YrsCrdt};
use filament_spool::{
    AcceptorMetrics, AcceptorRegistry, MetricsEncoder, SharedFjallStateStore, accept_connection,
};
use filament_weave::{FjallGroupStateStorage, WeaverClient};
use iroh::address_lookup::{AddressLookup, EndpointData, Item as AddressLookupItem};
use iroh::{Endpoint, EndpointId, RelayMode};
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::basic::BasicIdentityProvider;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};

const TEST_CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

/// Shared in-memory address lookup for tests.
///
/// Each test endpoint publishes its addressing info here and resolves other
/// endpoints from the same store, avoiding the need for real DNS/pkarr/mDNS.
#[derive(Debug, Clone, Default)]
pub struct TestAddressLookup {
    endpoints: Arc<Mutex<HashMap<EndpointId, EndpointData>>>,
}

impl TestAddressLookup {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

type LookupStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<AddressLookupItem, iroh::address_lookup::Error>> + Send>,
>;

impl AddressLookup for TestAddressLookup {
    fn resolve(&self, endpoint_id: EndpointId) -> Option<LookupStream> {
        let data = self.endpoints.lock().unwrap().get(&endpoint_id).cloned();
        match data {
            Some(data) => {
                let item = AddressLookupItem::new(
                    iroh::address_lookup::EndpointInfo::from_parts(endpoint_id, data),
                    "test",
                    None,
                );
                Some(Box::pin(futures::stream::once(async { Ok(item) })))
            }
            None => Some(Box::pin(futures::stream::empty())),
        }
    }
}

/// Per-endpoint wrapper that knows the endpoint's ID for publishing.
#[derive(Debug)]
struct TestAddressLookupEndpoint {
    id: EndpointId,
    shared: TestAddressLookup,
}

impl AddressLookup for TestAddressLookupEndpoint {
    fn publish(&self, data: &EndpointData) {
        self.shared
            .endpoints
            .lock()
            .unwrap()
            .insert(self.id, data.clone());
    }

    fn resolve(&self, endpoint_id: EndpointId) -> Option<LookupStream> {
        self.shared.resolve(endpoint_id)
    }
}

pub async fn test_weaver_client(name: &str, endpoint: Endpoint) -> WeaverClient {
    let dir = TempDir::new().expect("create temp dir for weaver client storage");
    let storage = FjallGroupStateStorage::open(dir.path())
        .await
        .expect("open weaver client storage");
    // Leak the TempDir so it lives as long as the process (tests manage
    // cleanup via the test harness / process exit).
    std::mem::forget(dir);
    WeaverClient::new(name.as_bytes().to_vec(), endpoint, storage)
}

/// Alias for [`test_weaver_client`].
pub async fn test_yrs_weaver_client(name: &str, endpoint: Endpoint) -> WeaverClient {
    test_weaver_client(name, endpoint).await
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

/// Localhost-only endpoint with a shared in-memory address lookup for testing.
///
/// # Panics
/// Panics if the endpoint fails to bind.
pub async fn test_endpoint(discovery: &TestAddressLookup) -> Endpoint {
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()))
        .build();

    let endpoint = Endpoint::empty_builder(RelayMode::Disabled)
        .transport_config(transport_config)
        .alpns(vec![ALPN.to_vec()])
        .bind_addr(bind_addr)
        .expect("valid bind address")
        .bind()
        .await
        .expect("failed to create endpoint");

    let per_endpoint = TestAddressLookupEndpoint {
        id: endpoint.id(),
        shared: discovery.clone(),
    };
    endpoint.address_lookup().add(per_endpoint);

    endpoint
}

/// Returns (task handle, acceptor id, temp dir).
/// Keep the `TempDir` alive for the acceptor's state store.
///
/// # Panics
/// Panics if the acceptor fails to start.
pub async fn spawn_acceptor(
    discovery: &TestAddressLookup,
) -> (tokio::task::JoinHandle<()>, AcceptorId, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let endpoint = test_endpoint(discovery).await;
    let acceptor_id = AcceptorId::from_bytes(*endpoint.id().as_bytes());

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

    (task, acceptor_id, temp_dir)
}
