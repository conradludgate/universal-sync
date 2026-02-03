//! Universal Sync Proposer REPL
//!
//! Interactive command-line interface for managing MLS groups with Paxos consensus.

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;
use iroh::{Endpoint, SecretKey};
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use tracing::info;
use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
    load_secret_key,
};
use universal_sync_proposer::connector::PAXOS_ALPN;
use universal_sync_proposer::repl::ReplContext;
use universal_sync_proposer::store::SharedProposerStore;

/// Universal Sync Proposer REPL
#[derive(Parser, Debug)]
#[command(name = "proposer")]
#[command(about = "Interactive REPL for Universal Sync proposers")]
struct Args {
    /// Path to the data directory for persistent state
    #[arg(short, long, default_value = "./proposer-data")]
    data: PathBuf,

    /// Path to the signing key file (32 bytes, hex or raw)
    /// If not provided, generates a new ephemeral key
    #[arg(short, long)]
    key_file: Option<PathBuf>,

    /// Name for this client identity
    #[arg(short, long, default_value = "proposer")]
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();

    // Load or generate iroh key
    let iroh_key = if let Some(ref key_path) = args.key_file {
        info!(?key_path, "Loading key from file");
        let bytes = load_secret_key(key_path)?;
        SecretKey::from_bytes(&bytes)
    } else {
        SecretKey::generate(&mut rand::rng())
    };

    let key_bytes = iroh_key.to_bytes();
    info!(
        public_key = %iroh_key.public(),
        "Using key"
    );

    // Create crypto provider and cipher suite
    let crypto = RustCryptoProvider::default();
    let cipher_suite = crypto
        .cipher_suite_provider(CipherSuite::CURVE25519_AES128)
        .expect("cipher suite should be available");

    // Generate MLS signing key
    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    info!(
        mls_public_key = %hex::encode(public_key.as_ref()),
        "MLS signing key generated"
    );

    // Create identity
    let credential = BasicCredential::new(args.name.as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    // Create MLS client with extension types
    let client = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(BasicIdentityProvider::new())
        .signing_identity(
            signing_identity,
            secret_key.clone(),
            CipherSuite::CURVE25519_AES128,
        )
        .extension_type(ACCEPTORS_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_ADD_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_REMOVE_EXTENSION_TYPE)
        .build();

    info!("MLS client created");

    // Open persistent store
    info!(path = ?args.data, "Opening data directory");
    let store = SharedProposerStore::open(&args.data).await?;

    // Create iroh endpoint
    let endpoint = Endpoint::builder()
        .secret_key(iroh_key)
        .alpns(vec![PAXOS_ALPN.to_vec()])
        .bind()
        .await?;

    info!(addr = ?endpoint.addr(), "Iroh endpoint ready");

    // Create REPL context
    let mut context = ReplContext {
        client,
        signer: secret_key,
        cipher_suite,
        endpoint,
        store,
        groups: HashMap::new(),
    };

    // Run REPL
    println!("Universal Sync Proposer REPL");
    println!("Iroh key: {}", hex::encode(key_bytes));
    println!("Type 'help' for available commands.\n");

    let mut rl = DefaultEditor::new()?;
    let history_path = args.data.join(".history");
    let _ = rl.load_history(&history_path);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Result<String, String>>(1);

    tokio::task::spawn_blocking(move || {
        loop {
            match rl.readline("sync> ") {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    if line == "exit" || line == "quit" {
                        break;
                    }

                    let _ = rl.add_history_entry(line);
                    if tx.blocking_send(line.to_string()).is_err() {
                        break;
                    }

                    match resp_rx.blocking_recv() {
                        Some(Ok(output)) if !output.is_empty() => println!("{output}"),
                        Some(Err(e)) => eprintln!("Error: {e}"),
                        _ => {}
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("Interrupted. Use 'exit' to quit.");
                }
                Err(ReadlineError::Eof) => {
                    println!("Goodbye!");
                    break;
                }
                Err(e) => {
                    eprintln!("Readline error: {e}");
                    break;
                }
            }
        }

        let _ = rl.save_history(&history_path);
    });

    // Main loop - just handle commands
    // Group learning happens in background tasks managed by each Group
    while let Some(line) = rx.recv().await {
        let res = context.execute(&line).await;
        let _ = resp_tx.send(res).await;
    }

    // Gracefully shutdown all groups
    for (_, group) in context.groups.drain() {
        group.shutdown().await;
    }

    Ok(())
}
