//! High-level group creation and joining flows
//!
//! This module provides helper functions for common operations like
//! creating new groups and joining existing ones.

use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use universal_sync_core::{AcceptorsExt, GroupId};

use crate::connector::{ConnectorError, register_group_with_addr};
use crate::learner::GroupLearner;

/// Error type for flow operations
#[derive(Debug)]
pub enum FlowError {
    /// MLS error
    Mls(mls_rs::error::MlsError),
    /// Connection/registration error
    Connector(ConnectorError),
    /// No acceptors provided
    NoAcceptors,
    /// Failed to generate `GroupInfo`
    GroupInfo(String),
}

impl std::fmt::Display for FlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlowError::Mls(e) => write!(f, "MLS error: {e:?}"),
            FlowError::Connector(e) => write!(f, "connector error: {e}"),
            FlowError::NoAcceptors => write!(f, "no acceptors provided"),
            FlowError::GroupInfo(e) => write!(f, "failed to generate GroupInfo: {e}"),
        }
    }
}

impl std::error::Error for FlowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FlowError::Connector(e) => Some(e),
            FlowError::Mls(_) | FlowError::NoAcceptors | FlowError::GroupInfo(_) => None,
        }
    }
}

impl From<mls_rs::error::MlsError> for FlowError {
    fn from(e: mls_rs::error::MlsError) -> Self {
        FlowError::Mls(e)
    }
}

impl From<ConnectorError> for FlowError {
    fn from(e: ConnectorError) -> Self {
        FlowError::Connector(e)
    }
}

/// Result of creating a new group
pub struct CreatedGroup<C, CS>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// The group learner for this device
    pub learner: GroupLearner<C, CS>,
    /// The group ID
    pub group_id: GroupId,
    /// The `GroupInfo` message bytes (for sharing with others)
    pub group_info: Vec<u8>,
}

/// Create a new MLS group and register it with acceptors
///
/// This is the main entry point for creating a new synchronized group.
/// It performs the following steps:
/// 1. Creates a new MLS group
/// 2. Generates a `GroupInfo` message
/// 3. Registers the group with all provided acceptors
///
/// When adding members using [`GroupLearner`], use [`acceptors_extension`] to
/// create the extension that should be set via `commit_builder().set_group_info_ext()`.
/// New members will receive the acceptor list in the Welcome message.
///
/// # Arguments
/// * `client` - The MLS client to use for creating the group
/// * `signer` - The signing key for this device
/// * `cipher_suite` - The cipher suite provider
/// * `endpoint` - The iroh endpoint for connecting to acceptors
/// * `acceptors` - The endpoint addresses of acceptors to register with
///
/// # Returns
/// A [`CreatedGroup`] containing the learner, group ID, and `GroupInfo`.
///
/// # Errors
/// Returns an error if group creation, `GroupInfo` generation, or acceptor registration fails.
///
/// # Example
///
/// ```ignore
/// let created = create_group(
///     &client,
///     signer,
///     cipher_suite,
///     &endpoint,
///     &acceptor_addrs,
/// ).await?;
///
/// // Share created.group_info with other devices so they can join
/// let connector = IrohConnector::new(endpoint, created.group_id);
/// ```
pub async fn create_group<C, CS>(
    client: &Client<C>,
    signer: SignatureSecretKey,
    cipher_suite: CS,
    endpoint: &Endpoint,
    acceptors: &[EndpointAddr],
) -> Result<CreatedGroup<C, CS>, FlowError>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    if acceptors.is_empty() {
        return Err(FlowError::NoAcceptors);
    }

    // Create a new MLS group (no special context extensions needed)
    let group = client.create_group(ExtensionList::default(), ExtensionList::default())?;

    // Generate GroupInfo for external observers (acceptors)
    let group_info_msg = group.group_info_message(true)?;
    let group_info = group_info_msg.to_bytes()?;

    // Extract the group ID
    let mls_group_id = group.context().group_id.clone();
    let group_id = GroupId::from_slice(&mls_group_id);

    // Register with all acceptors
    for addr in acceptors {
        register_group_with_addr(endpoint, addr.clone(), &group_info).await?;
    }

    // Create the learner
    let learner = GroupLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

    Ok(CreatedGroup {
        learner,
        group_id,
        group_info,
    })
}

/// Create an extension list containing the acceptor IDs
///
/// Use this when adding members to a group to include the acceptor list
/// in the Welcome message's `GroupInfo` extensions.
///
/// # Panics
/// Panics if encoding the `AcceptorsExt` fails (should never happen).
///
/// # Example
///
/// ```ignore
/// let ext = acceptors_extension(learner.acceptors().values().cloned());
/// let commit = group
///     .commit_builder()
///     .add_member(key_package)
///     .unwrap()
///     .set_group_info_ext(ext)
///     .build()
///     .unwrap();
/// ```
#[must_use]
pub fn acceptors_extension(acceptors: impl IntoIterator<Item = EndpointAddr>) -> ExtensionList {
    let acceptors_ext = AcceptorsExt::new(acceptors);
    let mut extensions = ExtensionList::default();
    extensions
        .set_from(acceptors_ext)
        .expect("AcceptorsExt encoding should not fail");
    extensions
}

/// Result of joining an existing group
pub struct JoinedGroup<C, CS>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider,
{
    /// The group learner for this device
    pub learner: GroupLearner<C, CS>,
    /// The group ID
    pub group_id: GroupId,
}

/// Join an existing MLS group using a Welcome message
///
/// This is the main entry point for joining an existing synchronized group.
/// A Welcome message is received from another group member who has added
/// this device to the group.
///
/// The acceptor list is read from the [`AcceptorsExt`] extension in the
/// `GroupInfo` extensions of the Welcome message. The sender must have used
/// [`acceptors_extension`] with `set_group_info_ext` when building the commit.
///
/// # Arguments
/// * `client` - The MLS client to use
/// * `signer` - The signing key for this device
/// * `cipher_suite` - The cipher suite provider
/// * `welcome_bytes` - The serialized Welcome message
///
/// # Returns
/// A [`JoinedGroup`] containing the learner and group ID.
///
/// # Errors
/// Returns an error if the Welcome message is invalid, joining fails, or the acceptors
/// extension cannot be read.
///
/// # Example
///
/// ```ignore
/// // Receive welcome_bytes from another group member
/// let joined = join_group(
///     &client,
///     signer,
///     cipher_suite,
///     &welcome_bytes,
/// ).await?;
///
/// let connector = IrohConnector::new(endpoint, joined.group_id);
/// ```
pub async fn join_group<C, CS>(
    client: &Client<C>,
    signer: SignatureSecretKey,
    cipher_suite: CS,
    welcome_bytes: &[u8],
) -> Result<JoinedGroup<C, CS>, FlowError>
where
    C: MlsConfig + Clone,
    CS: CipherSuiteProvider + Clone,
{
    // Parse the Welcome message
    let welcome = MlsMessage::from_bytes(welcome_bytes)?;

    // Join the group - info contains the GroupInfo extensions
    let (group, info) = client.join_group(None, &welcome)?;

    // Extract the group ID
    let mls_group_id = group.context().group_id.clone();
    let group_id = GroupId::from_slice(&mls_group_id);

    // Read acceptors from GroupInfo extensions (set via set_group_info_ext)
    let acceptors = info
        .group_info_extensions
        .get_as::<AcceptorsExt>()
        .map_err(|e| FlowError::GroupInfo(format!("failed to read acceptors extension: {e:?}")))?
        .map(|ext| ext.0)
        .unwrap_or_default();

    // Create the learner
    let learner = GroupLearner::new(group, signer, cipher_suite, acceptors);

    Ok(JoinedGroup { learner, group_id })
}
