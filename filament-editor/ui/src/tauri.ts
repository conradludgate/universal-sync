import { invoke } from "@tauri-apps/api/core";
import type { Delta, DocumentInfo, GroupStatePayload, PeerEntry } from "./types";

export const createDocument = () =>
  invoke<DocumentInfo>("create_document");

export const getKeyPackage = () =>
  invoke<string>("get_key_package");

export const recvWelcome = () =>
  invoke<DocumentInfo>("recv_welcome");

export const joinDocumentBytes = (welcomeB58: string) =>
  invoke<DocumentInfo>("join_document_bytes", { welcomeB58 });

export const applyDelta = (groupId: string, delta: Delta, anchor: number, head: number) =>
  invoke<void>("apply_delta", { groupId, delta, anchor, head });

export const getDocumentText = (groupId: string) =>
  invoke<string>("get_document_text", { groupId });

export const addMember = (groupId: string, keyPackageB58: string) =>
  invoke<void>("add_member", { groupId, keyPackageB58 });

export const addSpool = (groupId: string, addrB58: string) =>
  invoke<void>("add_spool", { groupId, addrB58 });

export const listSpools = (groupId: string) =>
  invoke<string[]>("list_spools", { groupId });

export const listPeers = (groupId: string) =>
  invoke<PeerEntry[]>("list_peers", { groupId });

export const addPeer = (groupId: string, inputB58: string) =>
  invoke<void>("add_peer", { groupId, inputB58 });

export const removeMember = (groupId: string, memberIndex: number) =>
  invoke<void>("remove_member", { groupId, memberIndex });

export const removeSpool = (groupId: string, spoolIdB58: string) =>
  invoke<void>("remove_spool", { groupId, spoolIdB58 });

export const getGroupState = (groupId: string) =>
  invoke<GroupStatePayload>("get_group_state", { groupId });

export const updateKeys = (groupId: string) =>
  invoke<void>("update_keys", { groupId });

export const updateCursor = (groupId: string, anchor: number, head: number) =>
  invoke<void>("update_cursor", { groupId, anchor, head });
