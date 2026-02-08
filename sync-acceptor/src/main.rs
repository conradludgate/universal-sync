//! Paxos acceptor server for Universal Sync groups.

use std::path::PathBuf;

use clap::Parser;
use iroh::{Endpoint, SecretKey};
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::basic::BasicIdentityProvider;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tracing::{error, info};
use universal_sync_acceptor::{AcceptorRegistry, SharedFjallStateStore, accept_connection};
use universal_sync_core::{PAXOS_ALPN, SYNC_EXTENSION_TYPE, SYNC_PROPOSAL_TYPE, load_secret_key};

#[derive(Parser, Debug)]
#[command(name = "acceptor")]
#[command(about = "Run a Universal Sync acceptor server")]
struct Args {
    #[arg(short, long, default_value = "./acceptor-db")]
    database: PathBuf,

    #[arg(short, long)]
    key_file: Option<PathBuf>,

    #[arg(short, long, default_value = "0.0.0.0:0")]
    bind: String,
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
    info!(count = groups.len(), "Loaded existing groups");

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
        .alpns(vec![PAXOS_ALPN.to_vec()]);

    for addr in tokio::net::lookup_host(args.bind).await? {
        endpoint_builder = endpoint_builder.bind_addr(addr)?;
    }

    let endpoint = endpoint_builder.bind().await?;

    let registry =
        AcceptorRegistry::new(external_client, cipher_suite, state_store, endpoint.clone());

    let addr = endpoint.addr();
    info!(?addr, "Acceptor server listening");

    let addr_bytes = postcard::to_allocvec(&addr).expect("serialization should not fail");
    let addr_str = bs58::encode(addr_bytes).into_string();
    println!("Endpoint address (base58): {addr_str}");

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
