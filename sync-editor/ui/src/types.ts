export interface DocumentInfo {
  group_id: string;
  text: string;
  member_count: number;
}

export type Delta =
  | { type: "Insert"; position: number; text: string }
  | { type: "Delete"; position: number; length: number }
  | { type: "Replace"; position: number; length: number; text: string };

export type PeerEntry =
  | { kind: "Member"; index: number; identity: string; is_self: boolean }
  | { kind: "Acceptor"; id: string };

export interface DocumentUpdatedPayload {
  group_id: string;
  text: string;
  deltas: Delta[];
}

export interface GroupStatePayload {
  group_id: string;
  epoch: number;
  transcript_hash: string;
  member_count: number;
  acceptor_count: number;
  connected_acceptor_count: number;
}

export interface AwarenessPeer {
  client_id: number;
  cursor: number | null;
  selection_end: number | null;
}

export interface AwarenessPayload {
  group_id: string;
  peers: AwarenessPeer[];
}

export type SyncStatus = "synced" | "syncing" | "error";
