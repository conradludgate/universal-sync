//! Paxos acceptor server for Universal Sync groups.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use filament_core::{PAXOS_ALPN, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE, load_secret_key};
use filament_spool::api::ApiState;
use filament_spool::{
    AcceptorMetrics, AcceptorRegistry, MetricsEncoder, SharedFjallStateStore, accept_connection,
};
use iroh::address_lookup::{DnsAddressLookup, MdnsAddressLookup, PkarrPublisher};
use iroh::{Endpoint, SecretKey};
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::basic::BasicIdentityProvider;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "acceptor")]
#[command(about = "Run a Universal Sync acceptor server")]
struct Args {
    #[arg(short, long, default_value = "./acceptor-db")]
    database: PathBuf,

    #[arg(short, long)]
    key_file: Option<PathBuf>,

    #[arg(short = 'B', long, default_value = "0.0.0.0:0")]
    bind: String,

    #[arg(short, long, default_value = "0.0.0.0:9090")]
    api_bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let secret_key = if let Some(ref key_path) = args.key_file {
        info!(?key_path, "Loading secret key from file");
        let bytes = load_secret_key(key_path)?;
        SecretKey::from_bytes(&bytes)
    } else {
        let key = SecretKey::generate(&mut rand::rng());
        info!(
            public_key = %key.public(),
            "Generated ephemeral secret key (use --key-file to persist)"
        );
        key
    };

    let public_key = secret_key.public();
    info!(%public_key, "Acceptor public key");

    info!(path = ?args.database, "Opening database");
    let state_store = SharedFjallStateStore::open(&args.database).await?;

    let groups = state_store.list_groups();
    info!(count = groups.len(), "Loaded existing weavers");

    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available");

    let external_client = ExternalClient::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .extension_type(SYNC_EXTENSION_TYPE)
        .custom_proposal_types(Some(SYNC_PROPOSAL_TYPE))
        .build();

    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()))
        .build();

    let mut endpoint_builder = Endpoint::builder()
        .transport_config(transport_config)
        .secret_key(secret_key.clone())
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .address_lookup(PkarrPublisher::n0_dns())
        .address_lookup(DnsAddressLookup::n0_dns())
        .address_lookup(MdnsAddressLookup::builder());

    for addr in tokio::net::lookup_host(&args.bind).await? {
        endpoint_builder = endpoint_builder.bind_addr(addr)?;
    }

    let endpoint = endpoint_builder.bind().await?;

    let metrics = AcceptorMetrics::new(state_store.clone());
    let metrics_encoder = Arc::new(MetricsEncoder::new(metrics));

    let registry = AcceptorRegistry::new(
        external_client,
        cipher_suite,
        state_store.clone(),
        endpoint.clone(),
        metrics_encoder.clone(),
    );

    let api_state = ApiState {
        metrics: metrics_encoder,
        endpoint: endpoint.clone(),
        state_store,
    };
    let api_router = filament_spool::api::router(api_state);

    let api_listener = tokio::net::TcpListener::bind(args.api_bind).await?;
    info!(addr = %args.api_bind, "API server listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(api_listener, api_router).await {
            error!(error = %e, "API server error");
        }
    });

    let spool_address = bs58::encode(public_key.as_bytes()).into_string();
    info!(spool_address, "Acceptor server listening");
    println!("Spool address: {spool_address}");

    info!("Ready to accept connections");
    while let Some(incoming) = endpoint.accept().await {
        info!("Incoming connection");

        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = accept_connection(incoming, registry).await {
                error!(error = %e, "Connection error");
            } else {
                info!("Connection closed");
            }
        });
    }

    Ok(())
}
