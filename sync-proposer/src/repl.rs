//! REPL command handling for the proposer CLI
//!
//! This module provides an interactive REPL for managing MLS groups with Paxos consensus.

#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::fmt::Write;

use iroh::{Endpoint, EndpointAddr};
use mls_rs::client_builder::MlsConfig;
use mls_rs::crypto::SignatureSecretKey;
use mls_rs::{CipherSuiteProvider, Client, ExtensionList, MlsMessage};
use tracing::info;
use universal_sync_core::{AcceptorId, GroupId};

use crate::group::Group;
use crate::store::SharedProposerStore;

/// REPL context holding all state
pub struct ReplContext<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// MLS client
    pub client: Client<C>,
    /// Signing key
    pub signer: SignatureSecretKey,
    /// Cipher suite provider
    pub cipher_suite: CS,
    /// Iroh endpoint for P2P connections
    pub endpoint: Endpoint,
    /// Persistent store for group state
    pub store: SharedProposerStore,
    /// Currently loaded groups
    pub groups: HashMap<GroupId, Group<C, CS>>,
}

impl<C, CS> ReplContext<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Execute a REPL command
    pub async fn execute(&mut self, line: &str) -> Result<String, String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(String::new());
        }

        match parts[0] {
            "help" | "?" => Ok(Self::help()),
            "exit" | "quit" => Err("exit".to_string()),
            "key_package" => self.cmd_key_package(),
            "create_group" => self.cmd_create_group().await,
            "join_group" => {
                if parts.len() < 2 {
                    return Err("Usage: join_group <welcome_hex>".to_string());
                }
                self.cmd_join_group(parts[1]).await
            }
            "add_acceptor" => {
                if parts.len() < 3 {
                    return Err("Usage: add_acceptor <group_id_hex> <address_hex>".to_string());
                }
                self.cmd_add_acceptor(parts[1], parts[2]).await
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
            "list_groups" => Ok(self.cmd_list_groups()),
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
  add_acceptor <group_id> <addr_hex>       - Add an acceptor to a group
  add_member <group_id> <key_package>      - Add a member to a group (prints Welcome hex)
  update_keys <group_id>                   - Update keys in a group
  remove_acceptor <group_id> <pubkey>      - Remove an acceptor from a group
  remove_member <group_id> <member_index>  - Remove a member from a group
  group_context <group_id>                 - Display group context
  list_groups                              - List all loaded groups
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

    async fn cmd_create_group(&mut self) -> Result<String, String> {
        let group = Group::create(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.endpoint,
            &[],
            self.store.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create group: {e:?}"))?;

        let group_id = group.group_id();
        let group_id_hex = hex::encode(group_id.as_bytes());

        self.groups.insert(group_id, group);

        Ok(format!("Created group: {group_id_hex}"))
    }

    async fn cmd_join_group(&mut self, welcome_hex: &str) -> Result<String, String> {
        let welcome_bytes = hex::decode(welcome_hex).map_err(|e| format!("Invalid hex: {e}"))?;

        let group = Group::join(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.endpoint,
            &welcome_bytes,
            self.store.clone(),
        )
        .await
        .map_err(|e| format!("Failed to join group: {e:?}"))?;

        let group_id = group.group_id();
        let context = group.context();

        let mut output = format!("Joined group: {}\n", hex::encode(group_id.as_bytes()));
        let _ = writeln!(output, "Epoch: {}", context.epoch.0);
        let _ = writeln!(output, "Members: {}", context.member_count);

        if !context.acceptors.is_empty() {
            output.push_str("Acceptors:\n");
            for id in &context.acceptors {
                let _ = writeln!(output, "  {}", hex::encode(id.as_bytes()));
            }
        }

        self.groups.insert(group_id, group);

        Ok(output)
    }

    async fn cmd_add_acceptor(
        &mut self,
        group_id_hex: &str,
        address_hex: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let addr = parse_endpoint_addr(address_hex)?;

        let acceptor_id = AcceptorId::from_bytes(*addr.id.as_bytes());

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .add_acceptor(addr)
            .await
            .map_err(|e| format!("Failed to add acceptor: {e:?}"))?;

        info!(?acceptor_id, "Added acceptor");

        Ok(format!(
            "Added acceptor: {}",
            hex::encode(acceptor_id.as_bytes())
        ))
    }

    async fn cmd_add_member(
        &mut self,
        group_id_hex: &str,
        key_package_hex: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let kp_bytes = hex::decode(key_package_hex).map_err(|e| format!("Invalid hex: {e}"))?;
        let key_package =
            MlsMessage::from_bytes(&kp_bytes).map_err(|e| format!("Invalid key package: {e:?}"))?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        let welcome = group
            .add_member(key_package)
            .await
            .map_err(|e| format!("Failed to add member: {e:?}"))?;

        Ok(format!("Welcome: {}", hex::encode(welcome)))
    }

    async fn cmd_update_keys(&mut self, group_id_hex: &str) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .update_keys()
            .await
            .map_err(|e| format!("Failed to update keys: {e:?}"))?;

        let context = group.context();
        Ok(format!("Keys updated. New epoch: {}", context.epoch.0))
    }

    async fn cmd_remove_acceptor(
        &mut self,
        group_id_hex: &str,
        public_key_hex: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let acceptor_id = parse_acceptor_id(public_key_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .remove_acceptor(acceptor_id)
            .await
            .map_err(|e| format!("Failed to remove acceptor: {e:?}"))?;

        Ok(format!("Removed acceptor: {public_key_hex}"))
    }

    async fn cmd_remove_member(
        &mut self,
        group_id_hex: &str,
        member_index_str: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let member_index: u32 = member_index_str
            .parse()
            .map_err(|_| "Invalid member index")?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .remove_member(member_index)
            .await
            .map_err(|e| format!("Failed to remove member: {e:?}"))?;

        Ok(format!("Removed member: {member_index}"))
    }

    fn cmd_group_context(&self, group_id_hex: &str) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        let context = group.context();

        let mut output = String::new();
        let _ = writeln!(output, "Group ID: {}", hex::encode(context.group_id.as_bytes()));
        let _ = writeln!(output, "Epoch: {}", context.epoch.0);
        let _ = writeln!(output, "Members: {}", context.member_count);

        output.push_str("\nAcceptors:\n");
        for acceptor_id in &context.acceptors {
            let _ = writeln!(output, "  {}", hex::encode(acceptor_id.as_bytes()));
        }

        Ok(output)
    }

    fn cmd_list_groups(&self) -> String {
        if self.groups.is_empty() {
            return "No groups loaded.".to_string();
        }

        let mut output = String::from("Groups:\n");

        for group_id in self.groups.keys() {
            let _ = writeln!(output, "  {}", hex::encode(group_id.as_bytes()));
        }

        output
    }
}

// Helper functions

fn parse_group_id(hex_str: &str) -> Result<GroupId, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {e}"))?;
    if bytes.len() > 32 {
        return Err("Group ID too long (max 32 bytes)".to_string());
    }
    Ok(GroupId::from_slice(&bytes))
}

fn parse_acceptor_id(hex_str: &str) -> Result<AcceptorId, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {e}"))?;
    if bytes.len() != 32 {
        return Err("Acceptor ID must be exactly 32 bytes".to_string());
    }
    let arr: [u8; 32] = bytes.try_into().unwrap();
    Ok(AcceptorId::from_bytes(arr))
}

fn parse_endpoint_addr(addr_hex: &str) -> Result<EndpointAddr, String> {
    let bytes = hex::decode(addr_hex).map_err(|e| format!("Invalid hex: {e}"))?;
    let addr: EndpointAddr =
        postcard::from_bytes(&bytes).map_err(|e| format!("Invalid address format: {e}"))?;
    Ok(addr)
}
