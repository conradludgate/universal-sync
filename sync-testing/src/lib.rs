//! Universal Sync Testing - test utilities and integration tests
//!
//! This crate provides test utilities for Universal Sync, importing both
//! proposer and acceptor functionality for integration testing.

use mls_rs::crypto::SignatureSecretKey;
use mls_rs::identity::SigningIdentity;
use mls_rs::identity::basic::{BasicCredential, BasicIdentityProvider};
use mls_rs::{CipherSuite, CipherSuiteProvider, Client, CryptoProvider};
use mls_rs_crypto_rustcrypto::RustCryptoProvider;
use universal_sync_core::{
    ACCEPTOR_ADD_EXTENSION_TYPE, ACCEPTOR_REMOVE_EXTENSION_TYPE, ACCEPTORS_EXTENSION_TYPE,
};

/// Default cipher suite for testing
pub const TEST_CIPHER_SUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

/// Create a test crypto provider
#[must_use]
pub fn test_crypto_provider() -> RustCryptoProvider {
    RustCryptoProvider::default()
}

/// Create a test cipher suite provider
///
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

/// Create a basic identity provider for testing
#[must_use]
pub fn test_identity_provider() -> BasicIdentityProvider {
    BasicIdentityProvider::new()
}

/// Type alias for the cipher suite provider used in tests
pub type TestCipherSuiteProvider = <RustCryptoProvider as CryptoProvider>::CipherSuiteProvider;

/// Result of creating a test client
pub struct TestClientResult<C> {
    /// The MLS client
    pub client: Client<C>,
    /// The signing secret key
    pub signer: SignatureSecretKey,
    /// The cipher suite provider
    pub cipher_suite: TestCipherSuiteProvider,
}

/// Create a test MLS client with the given identity name
///
/// This returns a fully configured client that can create groups.
///
/// # Arguments
/// * `name` - A human-readable name for this client (e.g., "alice", "bob")
///
/// # Panics
/// Panics if key generation or client building fails.
#[must_use]
pub fn test_client(name: &str) -> TestClientResult<impl mls_rs::client_builder::MlsConfig> {
    let crypto = test_crypto_provider();
    let cipher_suite = test_cipher_suite(&crypto);

    // Generate a signing key pair
    let (secret_key, public_key) = cipher_suite
        .signature_key_generate()
        .expect("key generation should succeed");

    // Create a basic credential
    let credential = BasicCredential::new(name.as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(credential.into_credential(), public_key);

    // Build the client with signing identity and custom extension types
    let client = Client::builder()
        .crypto_provider(crypto)
        .identity_provider(test_identity_provider())
        .signing_identity(signing_identity, secret_key.clone(), TEST_CIPHER_SUITE)
        .extension_type(ACCEPTORS_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_ADD_EXTENSION_TYPE)
        .extension_type(ACCEPTOR_REMOVE_EXTENSION_TYPE)
        .build();

    TestClientResult {
        client,
        signer: secret_key,
        cipher_suite,
    }
}

// Re-export for convenience
pub use universal_sync_acceptor::{
    AcceptorError, AcceptorRegistry, GroupAcceptor, GroupRegistry, GroupStateStore,
    IrohAcceptorConnection, SharedFjallStateStore, accept_connection,
};
pub use universal_sync_core::{
    AcceptorId, Epoch, GroupId, GroupMessage, GroupProposal, Handshake, HandshakeResponse, MemberId,
};
pub use universal_sync_proposer::{
    ConnectorError, Group, GroupContext, GroupError, GroupEvent, GroupLearner, IrohConnection,
    IrohConnector, LearnerError, PAXOS_ALPN, acceptors_extension, register_group,
    register_group_with_addr,
};
