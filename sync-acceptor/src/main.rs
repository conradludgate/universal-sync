//! Universal Sync Acceptor Server
//!
//! Runs a Paxos acceptor server for Universal Sync groups.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use iroh::{Endpoint, SecretKey};
use mls_rs::external_client::ExternalClient;
use mls_rs::identity::basic::BasicIdentityProvider;
use mls_rs::{CipherSuite, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use tracing::{error, info};
use universal_sync_acceptor::{
    AcceptorRegistry, PAXOS_ALPN, SharedFjallStateStore, accept_connection,
};

/// Universal Sync Acceptor Server
#[derive(Parser, Debug)]
#[command(name = "acceptor")]
#[command(about = "Run a Universal Sync acceptor server")]
struct Args {
    /// Path to the database directory
    #[arg(short, long, default_value = "./acceptor-db")]
    database: PathBuf,

    /// Path to the iroh secret key file (32 bytes, hex or raw)
    /// If not provided, generates a new ephemeral key
    #[arg(short, long)]
    key_file: Option<PathBuf>,

    /// Bind address for the QUIC endpoint (IPv4)
    #[arg(short, long, default_value = "0.0.0.0:0")]
    bind: String,
}

/// Load a secret key from a file
///
/// Supports:
/// - Raw 32 bytes
/// - Hex-encoded 64 characters (with optional 0x prefix)
fn load_secret_key(path: &PathBuf) -> Result<SecretKey, Box<dyn std::error::Error>> {
    let contents = std::fs::read(path)?;

    // Try parsing as raw bytes first
    if contents.len() == 32 {
        let bytes: [u8; 32] = contents.try_into().unwrap();
        return Ok(SecretKey::from_bytes(&bytes));
    }

    // Try parsing as hex
    let hex_str = String::from_utf8(contents)?;
    let hex_str = hex_str.trim();
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);

    if hex_str.len() != 64 {
        return Err(format!(
            "Invalid key file: expected 32 raw bytes or 64 hex characters, got {} bytes",
            hex_str.len()
        )
        .into());
    }

    let mut bytes = [0u8; 32];
    hex::decode_to_slice(hex_str, &mut bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Load or generate secret key
    let secret_key = if let Some(ref key_path) = args.key_file {
        info!(?key_path, "Loading secret key from file");
        load_secret_key(key_path)?
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

    // Open the state store
    info!(path = ?args.database, "Opening database");
    let state_store = SharedFjallStateStore::open(&args.database).await?;

    // List existing groups
    let groups = state_store.list_groups();
    info!(count = groups.len(), "Loaded existing groups");

    // Create crypto provider
    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available");

    // Create external client for MLS
    let external_client = ExternalClient::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .build();

    // Create the registry
    let registry = AcceptorRegistry::new(
        external_client,
        cipher_suite,
        state_store,
        secret_key.clone(),
    );

    // Create the iroh endpoint
    let mut endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![PAXOS_ALPN.to_vec()]);

    for addr in tokio::net::lookup_host(args.bind).await? {
        endpoint = match addr {
            SocketAddr::V4(addr) => endpoint.bind_addr_v4(addr),
            SocketAddr::V6(addr) => endpoint.bind_addr_v6(addr),
        };
    }

    let endpoint = endpoint.bind().await?;

    let addr = endpoint.addr();
    info!(?addr, "Acceptor server listening");

    // Print the address as JSON for easy copy/paste to proposer REPL
    let addr_bytes = postcard::to_allocvec(&addr).expect("serialization should not fail");
    let addr_str = hex::encode(addr_bytes);
    println!("Endpoint address (hex): {addr_str}");

    // Accept connections
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
