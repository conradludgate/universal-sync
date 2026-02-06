//! REPL command handling for the proposer CLI
//!
//! This module provides an interactive REPL for managing MLS groups with Paxos consensus.

#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::fmt::Write;

use iroh::EndpointAddr;
use mls_rs::client_builder::MlsConfig;
use mls_rs::{CipherSuiteProvider, MlsMessage};
use tracing::info;
use universal_sync_core::{AcceptorId, GroupId};

use crate::GroupClient;
use crate::group::Group;

/// REPL context holding all state
pub struct ReplContext<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Group client for creating and joining groups
    pub client: GroupClient<C, CS>,
    /// Currently loaded groups
    pub groups: HashMap<GroupId, Group<C, CS>>,
}

impl<C, CS> ReplContext<C, CS>
where
    C: MlsConfig + Clone + Send + Sync + 'static,
    CS: CipherSuiteProvider + Clone + Send + Sync + 'static,
{
    /// Create a new REPL context with the given client.
    #[must_use]
    pub fn new(client: GroupClient<C, CS>) -> Self {
        Self {
            client,
            groups: HashMap::new(),
        }
    }

    /// Process any pending welcome messages in the background.
    ///
    /// This should be called periodically (e.g., before each command) to
    /// automatically join groups when welcome messages are received.
    ///
    /// Returns a list of newly joined group IDs.
    pub async fn process_pending_welcomes(&mut self) -> Vec<GroupId> {
        let mut joined_groups = Vec::new();

        // Process all pending welcomes
        while let Some(welcome_bytes) = self.client.try_recv_welcome() {
            match self.client.join_group(&welcome_bytes).await {
                Ok(group) => {
                    let group_id = group.group_id();
                    info!(?group_id, "Automatically joined group from welcome");
                    self.groups.insert(group_id, group);
                    joined_groups.push(group_id);
                }
                Err(e) => {
                    tracing::warn!(?e, "Failed to join group from welcome");
                }
            }
        }

        joined_groups
    }

    /// Execute a REPL command
    pub async fn execute(&mut self, line: &str) -> Result<String, String> {
        // First, process any pending welcomes
        let joined = self.process_pending_welcomes().await;
        let mut output = String::new();
        if !joined.is_empty() {
            use std::fmt::Write;
            let _ = writeln!(
                output,
                "=== Automatically joined {} group(s) ===",
                joined.len()
            );
            for group_id in &joined {
                let _ = writeln!(
                    output,
                    "  Joined: {}",
                    bs58::encode(group_id.as_bytes()).into_string()
                );
            }
            output.push('\n');
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            if output.is_empty() {
                return Ok(String::new());
            }
            return Ok(output);
        }

        let result = match parts[0] {
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
                    return Err("Usage: add_member <group_id_hex> <key_package_base58>".to_string());
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
                self.cmd_group_context(parts[1]).await
            }
            "list_groups" => Ok(self.cmd_list_groups()),
            "send" => {
                if parts.len() < 3 {
                    return Err("Usage: send <group_id_hex> <message>".to_string());
                }
                // Join remaining parts as the message
                let message = parts[2..].join(" ");
                self.cmd_send_message(parts[1], &message).await
            }
            "recv" => {
                if parts.len() < 2 {
                    return Err("Usage: recv <group_id_hex>".to_string());
                }
                self.cmd_recv_message(parts[1]).await
            }
            _ => Err(format!(
                "Unknown command: {}. Type 'help' for available commands.",
                parts[0]
            )),
        };

        // Prepend any auto-join notifications to the result
        match result {
            Ok(cmd_output) => Ok(format!("{output}{cmd_output}")),
            Err(e) => Err(e),
        }
    }

    fn help() -> String {
        r"Available commands:
  key_package                              - Generate a new key package (prints base58)
  create_group                             - Create a new group (prints group ID base58)
  join_group <welcome_base58>              - Join a group from a Welcome message
  add_acceptor <group_id> <addr_base58>    - Add an acceptor to a group
  add_member <group_id> <key_package>      - Add a member to a group (sends welcome directly)
  update_keys <group_id>                   - Update keys in a group
  remove_acceptor <group_id> <pubkey>      - Remove an acceptor from a group
  remove_member <group_id> <member_index>  - Remove a member from a group
  group_context <group_id>                 - Display group context
  list_groups                              - List all loaded groups
  send <group_id> <message>                - Send an encrypted message to the group
  recv <group_id>                          - Receive and decrypt the next message
  help                                     - Show this help
  exit / quit                              - Exit the REPL

Adding a new member (two terminal workflow):
  1. New member: key_package           -> copy the base58 output
  2. Existing member: add_member <group_id> <key_package_base58>
  3. New member: (automatically joins - press Enter to see notification)

Note: Welcome messages are received automatically in the background.
      Press Enter or run any command to see if you've been added to a group.
"
        .to_string()
    }

    fn cmd_key_package(&self) -> Result<String, String> {
        let kp = self
            .client
            .generate_key_package()
            .map_err(|e| format!("Failed to generate key package: {e:?}"))?;

        let bytes = kp
            .to_bytes()
            .map_err(|e| format!("Failed to serialize key package: {e:?}"))?;

        Ok(bs58::encode(bytes).into_string())
    }

    async fn cmd_create_group(&mut self) -> Result<String, String> {
        let group = self
            .client
            .create_group(&[], "none")
            .await
            .map_err(|e| format!("Failed to create group: {e:?}"))?;

        let group_id = group.group_id();
        let group_id_hex = bs58::encode(group_id.as_bytes()).into_string();

        self.groups.insert(group_id, group);

        Ok(format!("Created group: {group_id_hex}"))
    }

    async fn cmd_join_group(&mut self, welcome_hex: &str) -> Result<String, String> {
        let welcome_bytes = bs58::decode(welcome_hex)
            .into_vec()
            .map_err(|e| format!("Invalid base58: {e}"))?;

        let mut group = self
            .client
            .join_group(&welcome_bytes)
            .await
            .map_err(|e| format!("Failed to join group: {e:?}"))?;

        let group_id = group.group_id();
        let output =
            Self::print_group_context(&group.context().await.map_err(|e| format!("{e:?}"))?);

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
            bs58::encode(acceptor_id.as_bytes()).into_string()
        ))
    }

    async fn cmd_add_member(
        &mut self,
        group_id_hex: &str,
        key_package_base58: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let kp_bytes = bs58::decode(key_package_base58)
            .into_vec()
            .map_err(|e| format!("Invalid base58: {e}"))?;
        let key_package =
            MlsMessage::from_bytes(&kp_bytes).map_err(|e| format!("Invalid key package: {e:?}"))?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .add_member(key_package)
            .await
            .map_err(|e| format!("Failed to add member: {e:?}"))?;

        Ok("Member added and welcome sent".to_string())
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

        let context = group.context().await.map_err(|e| format!("{e:?}"))?;
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

    async fn cmd_group_context(&mut self, group_id_hex: &str) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        let context = group.context().await.map_err(|e| format!("{e:?}"))?;
        Ok(Self::print_group_context(&context))
    }

    fn print_group_context(context: &crate::GroupContext) -> String {
        let mut output = String::new();
        let _ = writeln!(
            output,
            "Group ID: {}",
            bs58::encode(context.group_id.as_bytes()).into_string()
        );
        let _ = writeln!(output, "Epoch: {}", context.epoch.0);
        let _ = writeln!(
            output,
            "Transcript: {}",
            bs58::encode(&context.confirmed_transcript_hash).into_string()
        );
        let _ = writeln!(output, "Members: {}", context.member_count);

        if context.acceptors.is_empty() {
            output.push_str("Acceptors: none\n");
        } else {
            output.push_str("Acceptors:\n");
            for acceptor_id in &context.acceptors {
                let _ = writeln!(
                    output,
                    "  {}",
                    bs58::encode(acceptor_id.as_bytes()).into_string()
                );
            }
        }

        output
    }

    fn cmd_list_groups(&self) -> String {
        if self.groups.is_empty() {
            return "No groups loaded.".to_string();
        }

        let mut output = String::from("Groups:\n");

        for group_id in self.groups.keys() {
            let _ = writeln!(
                output,
                "  {}",
                bs58::encode(group_id.as_bytes()).into_string()
            );
        }

        output
    }

    async fn cmd_send_message(
        &mut self,
        group_id_hex: &str,
        _message: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .send_update()
            .await
            .map_err(|e| format!("Failed to send update: {e:?}"))?;

        Ok("Update sent".to_string())
    }

    async fn cmd_recv_message(&mut self, group_id_hex: &str) -> Result<String, String> {
        use std::time::Duration;

        use tokio::time::timeout;

        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        // Use a short timeout to avoid blocking the REPL forever
        let result = timeout(Duration::from_secs(5), group.wait_for_update()).await;

        match result {
            Ok(Some(())) => Ok("Update received and applied".to_string()),
            Ok(None) => Ok("No updates (channel closed)".to_string()),
            Err(_) => Ok("No updates (timeout)".to_string()),
        }
    }
}

// Helper functions

fn parse_group_id(b58_str: &str) -> Result<GroupId, String> {
    let bytes = bs58::decode(b58_str)
        .into_vec()
        .map_err(|e| format!("Invalid base58: {e}"))?;
    if bytes.len() > 32 {
        return Err("Group ID too long (max 32 bytes)".to_string());
    }
    Ok(GroupId::from_slice(&bytes))
}

fn parse_acceptor_id(b58_str: &str) -> Result<AcceptorId, String> {
    let bytes = bs58::decode(b58_str)
        .into_vec()
        .map_err(|e| format!("Invalid base58: {e}"))?;
    if bytes.len() != 32 {
        return Err("Acceptor ID must be exactly 32 bytes".to_string());
    }
    let arr: [u8; 32] = bytes.try_into().unwrap();
    Ok(AcceptorId::from_bytes(arr))
}

fn parse_endpoint_addr(addr_b58: &str) -> Result<EndpointAddr, String> {
    let bytes = bs58::decode(addr_b58)
        .into_vec()
        .map_err(|e| format!("Invalid base58: {e}"))?;
    let addr: EndpointAddr =
        postcard::from_bytes(&bytes).map_err(|e| format!("Invalid address format: {e}"))?;
    Ok(addr)
}
