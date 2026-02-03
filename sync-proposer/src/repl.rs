//! REPL command handling for the proposer CLI
//!
//! This module provides an interactive REPL for managing MLS groups with Paxos consensus.

#![allow(clippy::format_push_string)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::unused_self)]
#![allow(clippy::unnecessary_wraps)]

use std::collections::HashMap;

use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use rand::rngs::StdRng;
use tracing::info;
use universal_sync_core::{AcceptorId, GroupId, GroupMessage};
use universal_sync_paxos::Learner;
use universal_sync_paxos::config::{ProposerConfig, TokioSleep};
use universal_sync_paxos::proposer::Proposer;

use crate::connector::{IrohConnector, register_group_with_addr};
use crate::flows::{acceptors_extension, join_group};
use crate::learner::GroupLearner;
use crate::store::{SharedProposerStore, StoredAcceptor, StoredGroupInfo};
use crate::{AcceptorAdd, AcceptorRemove};

type ReplProposer<C, CS> =
    Proposer<GroupLearner<C, CS>, IrohConnector<GroupLearner<C, CS>>, TokioSleep, StdRng>;

/// Runtime state for a loaded group
pub struct LoadedGroup<C, CS>
where
    C: MlsConfig + Clone + 'static,
    CS: CipherSuiteProvider + 'static,
{
    /// The MLS group learner
    pub learner: GroupLearner<C, CS>,
    /// Paxos proposer (if we have acceptors)
    pub proposer: Option<ReplProposer<C, CS>>,
    /// Acceptor addresses for reconnection
    pub acceptor_addrs: HashMap<AcceptorId, EndpointAddr>,
}

/// REPL context holding all state
pub struct ReplContext<C, CS>
where
    C: MlsConfig + Clone + 'static,
    CS: CipherSuiteProvider + Clone + 'static,
{
    /// MLS client
    pub client: Client<C>,
    /// Signing key
    pub signer: SignatureSecretKey,
    /// Cipher suite provider
    pub cipher_suite: CS,
    /// Iroh endpoint for P2P connections
    pub endpoint: Endpoint,
    /// Persistent store for acceptor addresses
    pub store: SharedProposerStore,
    /// Currently loaded groups (in memory)
    pub loaded_groups: HashMap<GroupId, LoadedGroup<C, CS>>,
}

impl<C, CS> ReplContext<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Execute a REPL command
    ///
    /// # Errors
    /// Returns an error string on failure.
    pub async fn execute(&mut self, line: &str) -> Result<String, String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(String::new());
        }

        match parts[0] {
            "help" | "?" => Ok(Self::help()),
            "exit" | "quit" => Err("exit".to_string()),
            "key_package" => self.cmd_key_package(),
            "create_group" => self.cmd_create_group(),
            "join_group" => {
                if parts.len() < 2 {
                    return Err("Usage: join_group <welcome_hex>".to_string());
                }
                self.cmd_join_group(parts[1]).await
            }
            "add_acceptor" => {
                if parts.len() < 3 {
                    return Err("Usage: add_acceptor <group_id_hex> <address_json>".to_string());
                }
                // Address may contain spaces in JSON, so join remaining parts
                let addr_json = parts[2..].join(" ");
                self.cmd_add_acceptor(parts[1], &addr_json).await
            }
            "add_member" => {
                if parts.len() < 3 {
                    return Err("Usage: add_member <group_id_hex> <key_package_hex>".to_string());
                }
                self.cmd_add_member(parts[1], parts[2]).await
            }
            "update_keys" => {
                if parts.len() < 2 {
                    return Err("Usage: update_keys <group_id_hex>".to_string());
                }
                self.cmd_update_keys(parts[1]).await
            }
            "remove_acceptor" => {
                if parts.len() < 3 {
                    return Err(
                        "Usage: remove_acceptor <group_id_hex> <public_key_hex>".to_string()
                    );
                }
                self.cmd_remove_acceptor(parts[1], parts[2]).await
            }
            "remove_member" => {
                if parts.len() < 3 {
                    return Err("Usage: remove_member <group_id_hex> <member_index>".to_string());
                }
                self.cmd_remove_member(parts[1], parts[2]).await
            }
            "group_context" => {
                if parts.len() < 2 {
                    return Err("Usage: group_context <group_id_hex>".to_string());
                }
                self.cmd_group_context(parts[1])
            }
            "list_groups" => self.cmd_list_groups(),
            "sync" => {
                if parts.len() < 2 {
                    return Err("Usage: sync <group_id_hex>".to_string());
                }
                self.cmd_sync(parts[1]).await
            }
            _ => Err(format!(
                "Unknown command: {}. Type 'help' for available commands.",
                parts[0]
            )),
        }
    }

    fn help() -> String {
        r"Available commands:
  key_package                              - Generate a new key package (prints hex)
  create_group                             - Create a new group (prints group ID hex)
  join_group <welcome_hex>                 - Join a group from a Welcome message
  add_acceptor <group_id> <addr_json>       - Add an acceptor to a group
  add_member <group_id> <key_package>      - Add a member to a group (prints Welcome hex)
  update_keys <group_id>                   - Update keys in a group
  remove_acceptor <group_id> <pubkey>      - Remove an acceptor from a group
  remove_member <group_id> <member_index>  - Remove a member from a group
  sync <group_id>                          - Sync with acceptors to learn missed updates
  group_context <group_id>                 - Display group context
  list_groups                              - List all groups (loaded + stored)
  help                                     - Show this help
  exit / quit                              - Exit the REPL
"
        .to_string()
    }

    fn cmd_key_package(&self) -> Result<String, String> {
        let kp = self
            .client
            .generate_key_package_message(ExtensionList::default(), ExtensionList::default())
            .map_err(|e| format!("Failed to generate key package: {e:?}"))?;

        let bytes = kp
            .to_bytes()
            .map_err(|e| format!("Failed to serialize key package: {e:?}"))?;

        Ok(hex::encode(bytes))
    }

    fn cmd_create_group(&mut self) -> Result<String, String> {
        // Create a new MLS group with no acceptors
        let group = self
            .client
            .create_group(ExtensionList::default(), ExtensionList::default())
            .map_err(|e| format!("Failed to create group: {e:?}"))?;

        let mls_group_id = group.context().group_id.clone();
        let group_id = GroupId::from_slice(&mls_group_id);

        // Create learner (no acceptors initially)
        let learner = GroupLearner::new(
            group,
            self.signer.clone(),
            self.cipher_suite.clone(),
            std::iter::empty(),
        );

        // Store the empty acceptor list
        let stored = StoredGroupInfo { acceptors: vec![] };
        self.store
            .store_group(&group_id, &stored)
            .map_err(|e| format!("Failed to store group: {e:?}"))?;

        // Keep in memory
        self.loaded_groups.insert(
            group_id,
            LoadedGroup {
                learner,
                proposer: None,
                acceptor_addrs: HashMap::new(),
            },
        );

        Ok(format!(
            "Created group: {}",
            hex::encode(group_id.as_bytes())
        ))
    }

    async fn cmd_join_group(&mut self, welcome_hex: &str) -> Result<String, String> {
        let welcome_bytes = hex::decode(welcome_hex).map_err(|e| format!("Invalid hex: {e}"))?;

        let joined = join_group(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &welcome_bytes,
        )
        .await
        .map_err(|e| format!("Failed to join group: {e:?}"))?;

        let group_id = joined.group_id;

        // Get acceptors from learner (now includes addresses from Welcome)
        let acceptors = joined.learner.acceptors().clone();

        // Store acceptors with their addresses
        let stored = StoredGroupInfo {
            acceptors: acceptors
                .iter()
                .map(|(id, addr)| StoredAcceptor {
                    id: *id,
                    address: postcard::to_allocvec(addr)
                        .map(hex::encode)
                        .unwrap_or_default(),
                })
                .collect(),
        };
        self.store
            .store_group(&group_id, &stored)
            .map_err(|e| format!("Failed to store group: {e:?}"))?;

        // Note: No proposer yet - user needs to provide acceptor addresses
        self.loaded_groups.insert(
            group_id,
            LoadedGroup {
                learner: joined.learner,
                proposer: None,
                acceptor_addrs: HashMap::new(),
            },
        );

        self.rebuild_proposer(&group_id)?;

        // Sync actors with all acceptors
        if let Some(loaded) = self.loaded_groups.get_mut(&group_id)
            && let Some(proposer) = loaded.proposer.as_mut()
        {
            proposer.sync_actors(loaded.learner.acceptor_ids());
            // Trigger acceptors to start forwarding values
            proposer.start_sync(&loaded.learner);
        }

        let mut output = format!("Joined group: {}\n", hex::encode(group_id.as_bytes()));
        if !acceptors.is_empty() {
            output.push_str("Acceptors:\n");
            for (id, addr) in &acceptors {
                output.push_str(&format!("  {} -> {:?}\n", hex::encode(id.as_bytes()), addr));
            }
        }

        Ok(output)
    }

    async fn cmd_add_acceptor(
        &mut self,
        group_id_hex: &str,
        address_json: &str,
    ) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;
        let addr = self.parse_endpoint_addr(address_json)?;

        // Extract acceptor ID (public key) from the address
        let acceptor_id = AcceptorId::from_bytes(*addr.id.as_bytes());

        // Check group exists
        if !self.loaded_groups.contains_key(&group_id) {
            return Err(format!(
                "Group not loaded: {}. Create a new group or join via welcome.",
                group_id_hex
            ));
        }

        // Sync with existing acceptors before proposing
        self.sync_group(&group_id).await?;

        // Check if this is a new acceptor
        let is_new_acceptor = !self
            .loaded_groups
            .get(&group_id)
            .unwrap()
            .learner
            .acceptors()
            .contains_key(&acceptor_id);

        if is_new_acceptor {
            // Build commit with AcceptorAdd extension
            let add_ext = AcceptorAdd::new(addr.clone());
            let mut extensions = ExtensionList::default();
            extensions
                .set_from(add_ext)
                .map_err(|e| format!("Failed to create extension: {e:?}"))?;

            let loaded = self.loaded_groups.get_mut(&group_id).unwrap();

            let commit_output = loaded
                .learner
                .group_mut()
                .commit_builder()
                .set_group_context_ext(extensions)
                .map_err(|e| format!("Failed to set extension: {e:?}"))?
                .build()
                .map_err(|e| format!("Failed to build commit: {e:?}"))?;

            let commit_message = GroupMessage::new(commit_output.commit_message.clone());

            // If we have existing acceptors, propose via Paxos
            if let Some(proposer) = loaded.proposer.as_mut() {
                let (proposal, message) = proposer
                    .propose(&loaded.learner, commit_message)
                    .await
                    .ok_or("Paxos consensus failed")?;

                // Check if we won with our own proposal or adopted another's
                loaded
                    .learner
                    .apply(proposal, message)
                    .await
                    .map_err(|e| format!("Failed to apply: {e:?}"))?;
            } else {
                // No existing acceptors - apply locally
                loaded
                    .learner
                    .apply_pending_commit()
                    .map_err(|e| format!("Failed to apply commit: {e:?}"))?;
            }
        }

        // Get GroupInfo for registration
        let group_info_bytes = {
            let loaded = self.loaded_groups.get(&group_id).unwrap();
            let group_info = loaded
                .learner
                .group()
                .group_info_message(true)
                .map_err(|e| format!("Failed to get GroupInfo: {e:?}"))?;
            group_info
                .to_bytes()
                .map_err(|e| format!("Failed to serialize GroupInfo: {e:?}"))?
        };

        // Register group with the new acceptor (Create handshake)
        register_group_with_addr(&self.endpoint, addr.clone(), &group_info_bytes)
            .await
            .map_err(|e| format!("Failed to register with acceptor: {e:?}"))?;

        info!(?acceptor_id, "Registered with acceptor");

        // Store the address and rebuild proposer
        {
            let loaded = self.loaded_groups.get_mut(&group_id).unwrap();
            loaded.acceptor_addrs.insert(acceptor_id, addr);
        }

        self.rebuild_proposer(&group_id)?;

        // Sync actors with all acceptors
        if let Some(loaded) = self.loaded_groups.get_mut(&group_id)
            && let Some(proposer) = loaded.proposer.as_mut()
        {
            proposer.sync_actors(loaded.learner.acceptor_ids());
            // Trigger acceptors to start forwarding values
            proposer.start_sync(&loaded.learner);
        }

        // Update stored info
        self.save_acceptor_info(&group_id)?;

        let acceptor_hex = hex::encode(acceptor_id.as_bytes());
        if is_new_acceptor {
            Ok(format!("Added acceptor: {acceptor_hex}"))
        } else {
            Ok(format!("Updated acceptor address: {acceptor_hex}"))
        }
    }

    async fn cmd_add_member(
        &mut self,
        group_id_hex: &str,
        key_package_hex: &str,
    ) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;
        let kp_bytes = hex::decode(key_package_hex).map_err(|e| format!("Invalid hex: {e}"))?;
        let key_package =
            MlsMessage::from_bytes(&kp_bytes).map_err(|e| format!("Invalid key package: {e:?}"))?;

        // Sync with acceptors before proposing
        self.sync_group(&group_id).await?;

        let loaded = self
            .loaded_groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        // Build commit with member addition and acceptors extension
        let acceptors_ext = acceptors_extension(loaded.learner.acceptors().values().cloned());

        let commit_output = loaded
            .learner
            .group_mut()
            .commit_builder()
            .add_member(key_package)
            .map_err(|e| format!("Failed to add member: {e:?}"))?
            .set_group_info_ext(acceptors_ext)
            .build()
            .map_err(|e| format!("Failed to build commit: {e:?}"))?;

        let commit_message = GroupMessage::new(commit_output.commit_message.clone());

        // Propose via Paxos if we have acceptors
        if let Some(proposer) = loaded.proposer.as_mut() {
            let (proposal, message) = proposer
                .propose(&loaded.learner, commit_message)
                .await
                .ok_or("Paxos consensus failed")?;

            loaded
                .learner
                .apply(proposal, message)
                .await
                .map_err(|e| format!("Failed to apply: {e:?}"))?;
        } else {
            // No acceptors - apply locally
            loaded
                .learner
                .apply_pending_commit()
                .map_err(|e| format!("Failed to apply commit: {e:?}"))?;
        }

        // Get the Welcome message
        let welcome = commit_output
            .welcome_messages
            .first()
            .ok_or("No welcome message generated")?;
        let welcome_bytes = welcome
            .to_bytes()
            .map_err(|e| format!("Failed to serialize welcome: {e:?}"))?;

        Ok(format!("Welcome: {}", hex::encode(welcome_bytes)))
    }

    async fn cmd_update_keys(&mut self, group_id_hex: &str) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;

        // Sync with acceptors before proposing
        self.sync_group(&group_id).await?;

        let loaded = self
            .loaded_groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        // Build empty commit (updates keys)
        let commit_output = loaded
            .learner
            .group_mut()
            .commit_builder()
            .build()
            .map_err(|e| format!("Failed to build commit: {e:?}"))?;

        let commit_message = GroupMessage::new(commit_output.commit_message.clone());

        // Propose via Paxos if we have acceptors
        if let Some(proposer) = loaded.proposer.as_mut() {
            let (proposal, message) = proposer
                .propose(&loaded.learner, commit_message)
                .await
                .ok_or("Paxos consensus failed")?;

            loaded
                .learner
                .apply(proposal, message)
                .await
                .map_err(|e| format!("Failed to apply: {e:?}"))?;
        } else {
            loaded
                .learner
                .apply_pending_commit()
                .map_err(|e| format!("Failed to apply commit: {e:?}"))?;
        }

        Ok(format!(
            "Keys updated. New epoch: {}",
            loaded.learner.mls_epoch().0
        ))
    }

    async fn cmd_remove_acceptor(
        &mut self,
        group_id_hex: &str,
        public_key_hex: &str,
    ) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;
        let acceptor_id = self.parse_acceptor_id(public_key_hex)?;

        // Sync with acceptors before proposing
        self.sync_group(&group_id).await?;

        let loaded = self
            .loaded_groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        // Build commit with AcceptorRemove extension
        let remove_ext = AcceptorRemove::new(acceptor_id);
        let mut extensions = ExtensionList::default();
        extensions
            .set_from(remove_ext)
            .map_err(|e| format!("Failed to create extension: {e:?}"))?;

        let commit_output = loaded
            .learner
            .group_mut()
            .commit_builder()
            .set_group_context_ext(extensions)
            .map_err(|e| format!("Failed to set extension: {e:?}"))?
            .build()
            .map_err(|e| format!("Failed to build commit: {e:?}"))?;

        let commit_message = GroupMessage::new(commit_output.commit_message.clone());

        // Propose via Paxos (must have acceptors to remove one)
        if let Some(proposer) = loaded.proposer.as_mut() {
            let (proposal, message) = proposer
                .propose(&loaded.learner, commit_message)
                .await
                .ok_or("Paxos consensus failed")?;

            loaded
                .learner
                .apply(proposal, message)
                .await
                .map_err(|e| format!("Failed to apply: {e:?}"))?;
        } else {
            return Err("No acceptors configured".to_string());
        }

        // Remove address
        loaded.acceptor_addrs.remove(&acceptor_id);

        // Rebuild proposer
        self.rebuild_proposer(&group_id)?;

        // Sync actors
        if let Some(loaded) = self.loaded_groups.get_mut(&group_id)
            && let Some(proposer) = loaded.proposer.as_mut()
        {
            proposer.sync_actors(loaded.learner.acceptor_ids());
            // Trigger acceptors to start forwarding values
            proposer.start_sync(&loaded.learner);
        }

        // Update stored info
        self.save_acceptor_info(&group_id)?;

        Ok(format!("Removed acceptor: {public_key_hex}"))
    }

    async fn cmd_remove_member(
        &mut self,
        group_id_hex: &str,
        member_index_str: &str,
    ) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;
        let member_index: u32 = member_index_str
            .parse()
            .map_err(|_| "Invalid member index")?;

        // Sync with acceptors before proposing
        self.sync_group(&group_id).await?;

        let loaded = self
            .loaded_groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        // Build commit removing member
        let commit_output = loaded
            .learner
            .group_mut()
            .commit_builder()
            .remove_member(member_index)
            .map_err(|e| format!("Failed to remove member: {e:?}"))?
            .build()
            .map_err(|e| format!("Failed to build commit: {e:?}"))?;

        let commit_message = GroupMessage::new(commit_output.commit_message.clone());

        // Propose via Paxos if we have acceptors
        if let Some(proposer) = loaded.proposer.as_mut() {
            let (proposal, message) = proposer
                .propose(&loaded.learner, commit_message)
                .await
                .ok_or("Paxos consensus failed")?;

            loaded
                .learner
                .apply(proposal, message)
                .await
                .map_err(|e| format!("Failed to apply: {e:?}"))?;
        } else {
            loaded
                .learner
                .apply_pending_commit()
                .map_err(|e| format!("Failed to apply commit: {e:?}"))?;
        }

        Ok(format!("Removed member: {member_index}"))
    }

    /// Sync with acceptors to learn any missed updates
    async fn cmd_sync(&mut self, group_id_hex: &str) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;

        let loaded = self
            .loaded_groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        let proposer = loaded
            .proposer
            .as_mut()
            .ok_or("No acceptors configured - nothing to sync")?;

        let epoch_before = loaded.learner.mls_epoch().0;

        // Start sync to trigger acceptors to send their history
        proposer.start_sync(&loaded.learner);

        let mut learned_count = 0u64;

        // Learn values with a timeout
        loop {
            // Use a short timeout to catch up quickly
            let learn_result = tokio::time::timeout(
                std::time::Duration::from_millis(500),
                proposer.learn_one(&loaded.learner),
            )
            .await;

            match learn_result {
                Ok(Some((proposal, message))) => {
                    info!(
                        epoch = proposal.epoch.0,
                        member_id = ?proposal.member_id,
                        "learned value"
                    );

                    loaded
                        .learner
                        .apply(proposal, message)
                        .await
                        .map_err(|e| format!("Failed to apply learned value: {e:?}"))?;

                    learned_count += 1;
                }
                Ok(None) => {
                    // All connections closed
                    return Err("All acceptor connections closed".to_string());
                }
                Err(_timeout) => {
                    // No more values to learn (timeout)
                    break;
                }
            }
        }

        let epoch_after = loaded.learner.mls_epoch().0;

        Ok(format!(
            "Sync complete. Learned {} updates. Epoch: {} -> {}",
            learned_count, epoch_before, epoch_after
        ))
    }

    fn cmd_group_context(&self, group_id_hex: &str) -> Result<String, String> {
        let group_id = self.parse_group_id(group_id_hex)?;

        let loaded = self
            .loaded_groups
            .get(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        let ctx = loaded.learner.group().context();
        let roster = loaded.learner.group().roster();

        let mut output = String::new();
        output.push_str(&format!("Group ID: {}\n", hex::encode(&ctx.group_id)));
        output.push_str(&format!("Epoch: {}\n", ctx.epoch));
        output.push_str(&format!("Cipher Suite: {:?}\n", ctx.cipher_suite));

        output.push_str("\nMembers:\n");
        for member in roster.members() {
            let pk_bytes = member.signing_identity.signature_key.as_ref();
            output.push_str(&format!("  [{}] {}\n", member.index, hex::encode(pk_bytes)));
        }

        output.push_str("\nAcceptors:\n");
        for (acceptor_id, addr) in loaded.learner.acceptors() {
            output.push_str(&format!(
                "  {} -> {:?}\n",
                hex::encode(acceptor_id.as_bytes()),
                addr
            ));
        }

        Ok(output)
    }

    fn cmd_list_groups(&self) -> Result<String, String> {
        let stored_groups = self.store.list_groups();

        if stored_groups.is_empty() && self.loaded_groups.is_empty() {
            return Ok("No groups.".to_string());
        }

        let mut output = String::from("Groups:\n");

        // Show loaded groups first
        for group_id in self.loaded_groups.keys() {
            output.push_str(&format!(
                "  {} (loaded)\n",
                hex::encode(group_id.as_bytes())
            ));
        }

        // Show stored but not loaded
        for group_id in stored_groups {
            if !self.loaded_groups.contains_key(&group_id) {
                output.push_str(&format!(
                    "  {} (stored)\n",
                    hex::encode(group_id.as_bytes())
                ));
            }
        }

        Ok(output)
    }

    // Helper methods

    fn parse_group_id(&self, hex_str: &str) -> Result<GroupId, String> {
        let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {e}"))?;
        if bytes.len() > 32 {
            return Err("Group ID too long (max 32 bytes)".to_string());
        }
        Ok(GroupId::from_slice(&bytes))
    }

    fn parse_acceptor_id(&self, hex_str: &str) -> Result<AcceptorId, String> {
        let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {e}"))?;
        if bytes.len() != 32 {
            return Err("Acceptor ID must be exactly 32 bytes".to_string());
        }
        let arr: [u8; 32] = bytes.try_into().unwrap();
        Ok(AcceptorId::from_bytes(arr))
    }

    fn parse_endpoint_addr(&self, addr_str: &str) -> Result<EndpointAddr, String> {
        // Try deserializing as JSON (EndpointAddr implements Deserialize)
        let bytes = hex::decode(addr_str).map_err(|e| format!("Invalid hex: {e}"))?;
        let addr: EndpointAddr = postcard::from_bytes(&bytes)
            .map_err(|e| format!("Invalid address (expected JSON): {e}"))?;
        Ok(addr)
    }

    /// Sync a loaded group with acceptors to learn any pending updates
    ///
    /// This should be called before building a commit to ensure we're up to date.
    async fn sync_group(&mut self, group_id: &GroupId) -> Result<u64, String> {
        let loaded = self
            .loaded_groups
            .get_mut(group_id)
            .ok_or("Group not found")?;

        let Some(proposer) = loaded.proposer.as_mut() else {
            // No acceptors, nothing to sync
            return Ok(0);
        };

        // Start sync to trigger acceptors to send their history
        proposer.start_sync(&loaded.learner);

        let mut learned_count = 0u64;

        // Learn values with a short timeout
        loop {
            let learn_result = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                proposer.learn_one(&loaded.learner),
            )
            .await;

            match learn_result {
                Ok(Some((proposal, message))) => {
                    info!(
                        epoch = proposal.epoch.0,
                        member_id = ?proposal.member_id,
                        "synced value"
                    );

                    loaded
                        .learner
                        .apply(proposal, message)
                        .await
                        .map_err(|e| format!("Failed to apply learned value: {e:?}"))?;

                    learned_count += 1;
                }
                Ok(None) => {
                    // All connections closed - this is an error
                    return Err("All acceptor connections closed during sync".to_string());
                }
                Err(_timeout) => {
                    // No more values to learn
                    break;
                }
            }
        }

        if learned_count > 0 {
            info!(learned_count, "synced with acceptors");
        }

        Ok(learned_count)
    }

    fn rebuild_proposer(&mut self, group_id: &GroupId) -> Result<(), String> {
        let loaded = self
            .loaded_groups
            .get_mut(group_id)
            .ok_or("Group not found")?;

        // Check if we have acceptors
        let acceptors = loaded.learner.acceptors();

        if acceptors.is_empty() {
            loaded.proposer = None;
            return Ok(());
        }

        let connector = IrohConnector::new(self.endpoint.clone(), *group_id);

        let proposer = Proposer::new(
            loaded.learner.node_id(),
            connector,
            ProposerConfig::default(),
        );

        loaded.proposer = Some(proposer);
        Ok(())
    }

    fn save_acceptor_info(&self, group_id: &GroupId) -> Result<(), String> {
        let loaded = self.loaded_groups.get(group_id).ok_or("Group not found")?;

        let acceptors: Vec<StoredAcceptor> = loaded
            .learner
            .acceptors()
            .iter()
            .map(|(id, addr)| StoredAcceptor {
                id: *id,
                address: postcard::to_allocvec(addr)
                    .map(hex::encode)
                    .unwrap_or_default(),
            })
            .collect();

        let stored = StoredGroupInfo { acceptors };
        self.store
            .store_group(group_id, &stored)
            .map_err(|e| format!("Failed to store: {e:?}"))?;

        Ok(())
    }
}
