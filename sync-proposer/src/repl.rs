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
  send <group_id> <message>                - Send an encrypted message to the group
  recv <group_id>                          - Receive and decrypt the next message
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

        Ok(bs58::encode(bytes).into_string())
    }

    async fn cmd_create_group(&mut self) -> Result<String, String> {
        let group = Group::create(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.endpoint,
            &[],
        )
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

        let mut group = Group::join(
            &self.client,
            self.signer.clone(),
            self.cipher_suite.clone(),
            &self.endpoint,
            &welcome_bytes,
        )
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
        key_package_hex: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;
        let kp_bytes = bs58::decode(key_package_hex)
            .into_vec()
            .map_err(|e| format!("Invalid base58: {e}"))?;
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

        Ok(format!("Welcome: {}", bs58::encode(welcome).into_string()))
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
        message: &str,
    ) -> Result<String, String> {
        let group_id = parse_group_id(group_id_hex)?;

        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group not loaded: {group_id_hex}"))?;

        group
            .send_message(message.as_bytes())
            .await
            .map_err(|e| format!("Failed to send message: {e:?}"))?;

        Ok("Message sent".to_string())
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
        let result = timeout(Duration::from_secs(5), group.recv_message()).await;

        match result {
            Ok(Some(msg)) => {
                let mut output = String::new();
                let _ = writeln!(output, "From: member {}", msg.sender.0);
                let _ = writeln!(output, "Epoch: {}", msg.epoch.0);
                let _ = writeln!(output, "Index: {}", msg.index);
                // Try to decode as UTF-8, fall back to hex
                match std::str::from_utf8(&msg.data) {
                    Ok(text) => {
                        let _ = writeln!(output, "Data: {text}");
                    }
                    Err(_) => {
                        let _ = writeln!(
                            output,
                            "Data (base58): {}",
                            bs58::encode(&msg.data).into_string()
                        );
                    }
                }
                Ok(output)
            }
            Ok(None) => Ok("No messages (channel closed)".to_string()),
            Err(_) => Ok("No messages (timeout)".to_string()),
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
