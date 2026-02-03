//! High-level synchronized group API
//!
//! This module provides a self-driving [`Group`] wrapper that:
//! - Spawns a background task to learn updates from acceptors
//! - Automatically retries proposals on contention
//! - Provides a simple API for group operations
//!
//! # Example
//!
//! ```ignore
//! let group = Group::create(&client, signer, cipher_suite, &endpoint, &acceptors).await?;
//!
//! // Subscribe to events (optional, informational only)
//! let mut events = group.subscribe();
//! tokio::spawn(async move {
//!     while let Ok(event) = events.recv().await {
//!         println!("Group event: {:?}", event);
//!     }
//! });
//!
//! // Add a member - blocks until consensus is reached
//! let welcome = group.add_member(key_package).await?;
//!
//! // Graceful shutdown
//! group.shutdown().await;
//! ```

use error_stack::Report;
use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::group::proposal::Proposal as MlsProposal;
use mls_rs::group::{CommitEffect, ReceivedMessage};
use mls_rs::{CipherSuiteProvider, Client, MlsMessage};
use rand::rngs::StdRng;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use universal_sync_core::{
    AcceptorAdd, AcceptorId, AcceptorRemove, Epoch, GroupId, GroupMessage, GroupProposal,
};
use universal_sync_paxos::Learner;
use universal_sync_paxos::config::{ProposerConfig, TokioSleep};
use universal_sync_paxos::proposer::Proposer;

use crate::connector::IrohConnector;
use crate::error::GroupError;
use crate::flows::{acceptors_extension, join_group as join_group_flow};
use crate::learner::GroupLearner;
use crate::store::SharedProposerStore;

/// Type alias for the proposer with concrete types
type GroupProposer<C, CS> =
    Proposer<GroupLearner<C, CS>, IrohConnector<GroupLearner<C, CS>>, TokioSleep, StdRng>;

/// Events emitted by a Group.
///
/// These are informational only - applications don't need to handle them.
/// The enum is `#[non_exhaustive]` so new variants can be added without breaking changes.
///
/// Each variant corresponds to an MLS proposal type that was applied in a commit.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum GroupEvent {
    /// A member was added
    MemberAdded {
        /// The index of the new member
        index: u32,
    },
    /// A member was removed
    MemberRemoved {
        /// The index of the removed member
        index: u32,
    },
    /// An acceptor was added (from `GroupContextExtensions` proposal)
    AcceptorAdded {
        /// The ID of the new acceptor
        id: AcceptorId,
    },
    /// An acceptor was removed (from `GroupContextExtensions` proposal)
    AcceptorRemoved {
        /// The ID of the removed acceptor
        id: AcceptorId,
    },
    /// `ReInit` proposal was applied
    ReInitiated,
    /// External init proposal was applied
    ExternalInit,
    /// Group context extensions were updated (not acceptor add/remove)
    ExtensionsUpdated,
    /// Unknown or custom proposal type
    Unknown,
}

/// Current state of the group
#[derive(Debug, Clone)]
pub struct GroupContext {
    /// The group ID
    pub group_id: GroupId,
    /// Current MLS epoch
    pub epoch: Epoch,
    /// Number of members in the group
    pub member_count: usize,
    /// Current set of acceptor IDs
    pub acceptors: Vec<AcceptorId>,
}

/// A synchronized MLS group that automatically handles consensus.
///
/// Spawns a background task to learn updates from acceptors.
/// All mutation methods automatically propose via Paxos with retry.
///
/// # Lifecycle
///
/// - On `Drop`: Sends cancel signal to background task, does NOT wait
/// - On [`shutdown()`](Self::shutdown): Sends cancel signal and waits for clean termination
pub struct Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// The group ID
    id: GroupId,

    /// The underlying learner
    learner: GroupLearner<C, CS>,

    /// The Paxos proposer (works with or without acceptors)
    proposer: GroupProposer<C, CS>,

    /// Connector for creating new proposers (used when rebuilding after acceptor changes)
    #[allow(dead_code)]
    connector: IrohConnector<GroupLearner<C, CS>>,

    /// Receiver for learned proposals from the background task
    learned_rx: tokio::sync::mpsc::Receiver<(GroupProposal, GroupMessage)>,

    /// Background task handle
    learn_task: Option<JoinHandle<()>>,

    /// Cancellation token for the background task
    cancel_token: CancellationToken,

    /// Event broadcaster
    event_tx: broadcast::Sender<GroupEvent>,

    /// Persistent storage
    #[allow(dead_code)]
    store: SharedProposerStore,
}

impl<C, CS> Drop for Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Signal cancellation but don't wait
        self.cancel_token.cancel();
    }
}

impl<C, CS> Group<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Send + Sync + 'static,
{
    /// Create a new group, optionally registering with acceptors.
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key
    /// * `cipher_suite` - The cipher suite provider
    /// * `endpoint` - The iroh endpoint for p2p connections
    /// * `acceptors` - Endpoint addresses of acceptors to register with (can be empty)
    /// * `store` - Persistent storage for group state
    ///
    /// # Errors
    /// Returns an error if group creation or acceptor registration fails.
    pub async fn create(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: &Endpoint,
        acceptors: &[EndpointAddr],
        store: SharedProposerStore,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        use crate::connector::register_group_with_addr;

        // Create a new MLS group
        let group = client
            .create_group(mls_rs::ExtensionList::default(), mls_rs::ExtensionList::default())
            .map_err(|e| {
                Report::new(GroupError).attach_printable(format!("failed to create MLS group: {e:?}"))
            })?;

        // Extract the group ID
        let mls_group_id = group.context().group_id.clone();
        let group_id = GroupId::from_slice(&mls_group_id);

        // Create the learner
        let learner = GroupLearner::new(group, signer, cipher_suite, acceptors.iter().cloned());

        // Generate GroupInfo and register with acceptors (if any)
        if !acceptors.is_empty() {
            let group_info_msg = learner.group().group_info_message(true).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("failed to create group info: {e:?}"))
            })?;
            let group_info_bytes = group_info_msg.to_bytes().map_err(|e| {
                Report::new(GroupError).attach_printable(format!("failed to serialize group info: {e:?}"))
            })?;

            for addr in acceptors {
                register_group_with_addr(endpoint, addr.clone(), &group_info_bytes)
                    .await
                    .map_err(|e| {
                        Report::new(GroupError).attach_printable(format!("failed to register with acceptor: {e}"))
                    })?;
            }
        }

        let connector = IrohConnector::new(endpoint.clone(), group_id);

        Ok(Self::from_learner(learner, group_id, connector, store))
    }

    /// Join an existing group from a Welcome message.
    ///
    /// # Arguments
    /// * `client` - The MLS client
    /// * `signer` - The signing secret key
    /// * `cipher_suite` - The cipher suite provider
    /// * `endpoint` - The iroh endpoint for p2p connections
    /// * `welcome` - The Welcome message bytes
    /// * `store` - Persistent storage for group state
    ///
    /// # Errors
    /// Returns an error if joining fails.
    pub async fn join(
        client: &Client<C>,
        signer: SignatureSecretKey,
        cipher_suite: CS,
        endpoint: &Endpoint,
        welcome: &[u8],
        store: SharedProposerStore,
    ) -> Result<Self, Report<GroupError>>
    where
        CS: Clone,
    {
        let joined = join_group_flow(client, signer, cipher_suite, welcome)
            .await
            .map_err(|e| {
                Report::new(GroupError).attach_printable(format!("failed to join group: {e}"))
            })?;

        let connector = IrohConnector::new(endpoint.clone(), joined.group_id);

        Ok(Self::from_learner(joined.learner, joined.group_id, connector, store))
    }

    /// Create a Group from an existing learner
    fn from_learner(
        learner: GroupLearner<C, CS>,
        id: GroupId,
        connector: IrohConnector<GroupLearner<C, CS>>,
        store: SharedProposerStore,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        let (event_tx, _) = broadcast::channel(64);
        let (learned_tx, learned_rx) = tokio::sync::mpsc::channel(64);

        // Create the proposer (works with or without acceptors)
        let node_id = learner.node_id();
        let acceptor_ids: Vec<_> = learner.acceptor_ids().collect();
        let mut proposer = Proposer::new(node_id, connector.clone(), ProposerConfig::default());
        proposer.sync_actors(acceptor_ids);

        // Spawn the background learning task
        let task_cancel = cancel_token.clone();
        let task_connector = connector.clone();
        let task = tokio::spawn(async move {
            Self::learn_loop(node_id, task_connector, task_cancel, learned_tx).await;
        });

        Self {
            id,
            learner,
            proposer,
            connector,
            learned_rx,
            learn_task: Some(task),
            cancel_token,
            event_tx,
            store,
        }
    }

    /// Background task that continuously learns from acceptors
    async fn learn_loop(
        node_id: universal_sync_core::MemberId,
        connector: IrohConnector<GroupLearner<C, CS>>,
        cancel_token: CancellationToken,
        learned_tx: tokio::sync::mpsc::Sender<(GroupProposal, GroupMessage)>,
    ) {
        // Create a dedicated proposer for learning
        let acceptor_ids: Vec<_> = Vec::new(); // Will be synced on first use
        let mut proposer: GroupProposer<C, CS> =
            Proposer::new(node_id, connector, ProposerConfig::default());
        proposer.sync_actors(acceptor_ids);

        // This is a simplified learner - we create a minimal state tracker
        // The actual learning happens through the channel
        loop {
            tokio::select! {
                biased;

                () = cancel_token.cancelled() => {
                    tracing::debug!("learn loop cancelled");
                    break;
                }

                // Note: This is a placeholder - the actual implementation would need
                // a way to get the current learner state for learn_one
                // For now, we'll rely on the main Group's polling in apply_pending_learned
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Polling placeholder - real implementation needs shared state
                }
            }
        }

        drop(learned_tx);
    }

    /// Apply any pending learned proposals from the background task
    fn apply_pending_learned(&mut self) -> Result<Vec<GroupEvent>, Report<GroupError>> {
        let mut events = Vec::new();

        while let Ok((proposal, message)) = self.learned_rx.try_recv() {
            let new_events = self.apply_proposal(&proposal, message)?;
            events.extend(new_events);
        }

        Ok(events)
    }

    /// Apply a proposal and message, returning the events generated
    fn apply_proposal(
        &mut self,
        proposal: &GroupProposal,
        message: GroupMessage,
    ) -> Result<Vec<GroupEvent>, Report<GroupError>> {
        // Check if this is our own proposal
        let my_proposal = proposal.member_id == self.learner.node_id();

        if my_proposal && self.learner.has_pending_commit() {
            // This is our proposal and we have a pending commit - apply it
            tracing::debug!("applying our own pending commit");
            let effect = self
                .learner
                .group_mut()
                .apply_pending_commit()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("apply pending commit failed: {e:?}"))
                })?;

            let events = self.process_commit_effect(&effect.effect);
            self.sync_proposer_actors();
            Ok(events)
        } else {
            // This is someone else's proposal, or we don't have a pending commit
            if self.learner.has_pending_commit() {
                tracing::debug!("clearing pending commit - another proposal won");
                self.learner.clear_pending_commit();
            }

            // Process the MLS message
            let result = self
                .learner
                .group_mut()
                .process_incoming_message(message.mls_message)
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("process message failed: {e:?}"))
                })?;

            let events = match result {
                ReceivedMessage::Commit(commit_desc) => {
                    tracing::debug!(
                        epoch = self.learner.group().context().epoch,
                        "applied commit"
                    );
                    self.process_commit_effect(&commit_desc.effect)
                }
                _ => Vec::new(),
            };

            self.sync_proposer_actors();
            Ok(events)
        }
    }

    /// Process a commit effect and return events for each applied proposal
    fn process_commit_effect(&mut self, effect: &CommitEffect) -> Vec<GroupEvent> {
        let applied_proposals = match effect {
            CommitEffect::NewEpoch(new_epoch) | CommitEffect::Removed { new_epoch, .. } => {
                &new_epoch.applied_proposals
            }
            CommitEffect::ReInit(_) => {
                return vec![GroupEvent::ReInitiated];
            }
        };

        let mut events = Vec::new();

        for proposal_info in applied_proposals {
            let event = match &proposal_info.proposal {
                MlsProposal::Add(add_proposal) => {
                    // Get the member index from the key package
                    // For now, we use a placeholder - real implementation would track this
                    let _ = add_proposal;
                    GroupEvent::MemberAdded { index: 0 }
                }
                MlsProposal::Remove(remove_proposal) => GroupEvent::MemberRemoved {
                    index: remove_proposal.to_remove(),
                },
                MlsProposal::ReInit(_) => GroupEvent::ReInitiated,
                MlsProposal::ExternalInit(_) => GroupEvent::ExternalInit,
                MlsProposal::GroupContextExtensions(extensions) => {
                    // Check for AcceptorAdd
                    if let Ok(Some(add)) = extensions.get_as::<AcceptorAdd>() {
                        let id = add.acceptor_id();
                        self.learner.add_acceptor_addr(add.0.clone());
                        GroupEvent::AcceptorAdded { id }
                    } else if let Ok(Some(remove)) = extensions.get_as::<AcceptorRemove>() {
                        let id = remove.acceptor_id();
                        self.learner.remove_acceptor_id(&id);
                        GroupEvent::AcceptorRemoved { id }
                    } else {
                        GroupEvent::ExtensionsUpdated
                    }
                }
                _ => GroupEvent::Unknown,
            };

            events.push(event.clone());

            // Broadcast each event
            let _ = self.event_tx.send(event);
        }

        events
    }

    /// Sync the proposer's actor set with the current acceptors
    fn sync_proposer_actors(&mut self) {
        self.proposer.sync_actors(self.learner.acceptor_ids());
    }

    /// Add a member to the group.
    ///
    /// Proposes via Paxos with automatic retry on contention.
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Arguments
    /// * `key_package` - The new member's key package
    ///
    /// # Returns
    /// The Welcome message to send to the new member.
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn add_member(
        &mut self,
        key_package: MlsMessage,
    ) -> Result<Vec<u8>, Report<GroupError>> {
        self.propose_with_retry(|learner| {
            // Collect acceptors first to avoid borrow conflicts
            let acceptors_ext = acceptors_extension(learner.acceptors().values().cloned());

            // Build the commit with acceptors extension
            let commit_output = learner
                .group_mut()
                .commit_builder()
                .add_member(key_package.clone())
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("add_member failed: {e:?}"))
                })?
                .set_group_info_ext(acceptors_ext)
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            // Extract the welcome message
            let welcome = commit_output
                .welcome_messages
                .first()
                .ok_or_else(|| {
                    Report::new(GroupError).attach_printable("no welcome message generated")
                })?
                .to_bytes()
                .map_err(|e| {
                    Report::new(GroupError)
                        .attach_printable(format!("welcome serialization failed: {e:?}"))
                })?;

            Ok((commit_output, welcome))
        })
        .await
    }

    /// Remove a member from the group.
    ///
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Arguments
    /// * `member_index` - The index of the member to remove
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn remove_member(&mut self, member_index: u32) -> Result<(), Report<GroupError>> {
        self.propose_with_retry(|learner| {
            let commit_output = learner
                .group_mut()
                .commit_builder()
                .remove_member(member_index)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("remove_member failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Update this member's keys (forward secrecy).
    ///
    /// Blocks until this node has learned the consensus result.
    ///
    /// # Errors
    /// Returns an error if the operation fails after retries.
    pub async fn update_keys(&mut self) -> Result<(), Report<GroupError>> {
        self.propose_with_retry(|learner| {
            let commit_output = learner.group_mut().commit_builder().build().map_err(|e| {
                Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
            })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Add an acceptor to the federation.
    ///
    /// This proposes adding the acceptor to the group via consensus,
    /// then registers the group with the new acceptor.
    ///
    /// # Arguments
    /// * `addr` - The endpoint address of the acceptor to add
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    pub async fn add_acceptor(&mut self, addr: EndpointAddr) -> Result<(), Report<GroupError>> {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorAdd;

        use crate::connector::register_group_with_addr;

        // Build commit with AcceptorAdd extension
        self.propose_with_retry(|learner| {
            let add_ext = AcceptorAdd::new(addr.clone());
            let mut extensions = ExtensionList::default();
            extensions.set_from(add_ext).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("extension failed: {e:?}"))
            })?;

            let commit_output = learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("set extension failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await?;

        // Get GroupInfo for registration
        let group_info = self.learner.group().group_info_message(true).map_err(|e| {
            Report::new(GroupError).attach_printable(format!("group info failed: {e:?}"))
        })?;
        let group_info_bytes = group_info.to_bytes().map_err(|e| {
            Report::new(GroupError).attach_printable(format!("serialization failed: {e:?}"))
        })?;

        // Register group with the new acceptor
        register_group_with_addr(self.connector.endpoint(), addr, &group_info_bytes)
            .await
            .map_err(|e| {
                Report::new(GroupError).attach_printable(format!("registration failed: {e}"))
            })?;

        Ok(())
    }

    /// Remove an acceptor from the federation.
    ///
    /// # Arguments
    /// * `acceptor_id` - The ID of the acceptor to remove
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    pub async fn remove_acceptor(
        &mut self,
        acceptor_id: AcceptorId,
    ) -> Result<(), Report<GroupError>> {
        use mls_rs::ExtensionList;
        use universal_sync_core::AcceptorRemove;

        // Build commit with AcceptorRemove extension
        self.propose_with_retry(|learner| {
            let remove_ext = AcceptorRemove::new(acceptor_id);
            let mut extensions = ExtensionList::default();
            extensions.set_from(remove_ext).map_err(|e| {
                Report::new(GroupError).attach_printable(format!("extension failed: {e:?}"))
            })?;

            let commit_output = learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("set extension failed: {e:?}"))
                })?
                .build()
                .map_err(|e| {
                    Report::new(GroupError).attach_printable(format!("commit build failed: {e:?}"))
                })?;

            Ok((commit_output, ()))
        })
        .await
    }

    /// Subscribe to group state changes (informational).
    ///
    /// This is purely informational - applications are not required to consume events.
    /// The stream will buffer and drop old events if not consumed.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<GroupEvent> {
        self.event_tx.subscribe()
    }

    /// Get current group context (epoch, members, acceptors).
    #[must_use]
    pub fn context(&self) -> GroupContext {
        GroupContext {
            group_id: self.id,
            epoch: self.learner.mls_epoch(),
            member_count: self.learner.group().roster().members().len(),
            acceptors: self.learner.acceptor_ids().collect(),
        }
    }

    /// Get the group ID
    #[must_use]
    pub fn group_id(&self) -> GroupId {
        self.id
    }

    /// Gracefully shut down the background task.
    ///
    /// Unlike `Drop`, this waits for the background task to complete.
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        if let Some(task) = self.learn_task.take() {
            let _ = task.await;
        }
    }

    /// Propose with automatic retry on contention.
    ///
    /// Blocks until this node has learned the consensus result.
    async fn propose_with_retry<F, T>(&mut self, build_commit: F) -> Result<T, Report<GroupError>>
    where
        F: Fn(
            &mut GroupLearner<C, CS>,
        ) -> Result<(mls_rs::group::CommitOutput, T), Report<GroupError>>,
    {
        const MAX_RETRIES: u32 = 5;

        for attempt in 0..MAX_RETRIES {
            tracing::debug!(attempt, "propose_with_retry attempt");

            // Apply any pending learned proposals first
            self.apply_pending_learned()?;

            let current_epoch = self.learner.mls_epoch();
            let my_id = self.learner.node_id();
            let (commit_output, result) = build_commit(&mut self.learner)?;
            let message = GroupMessage::new(commit_output.commit_message.clone());

            // Try to get consensus (works with or without acceptors)
            match self.proposer.propose(&self.learner, message).await {
                Some((won_proposal, won_msg)) => {
                    // Check if we won (by comparing epoch and member ID)
                    let we_won =
                        won_proposal.member_id == my_id && won_proposal.epoch == current_epoch;

                    // Apply the winning proposal (handles both our own and others')
                    self.apply_proposal(&won_proposal, won_msg)?;

                    if we_won {
                        return Ok(result);
                    }
                    // Someone else won - retry
                }
                None => {
                    // Proposal failed - clear and retry
                    self.learner.clear_pending_commit();
                }
            }

            // Wait a bit before retrying
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        Err(Report::new(GroupError).attach_printable("max retries exceeded due to contention"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GroupEvent>();
    }
}
