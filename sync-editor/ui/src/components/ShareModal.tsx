import { useCallback, useEffect, useState } from "react";
import * as tauri from "../tauri";
import { useToast } from "../hooks/useToast";
import type { GroupStatePayload, PeerEntry } from "../types";

function shortId(id: string): string {
  return id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-8)}` : id;
}

interface ShareModalProps {
  groupId: string;
  groupState: GroupStatePayload | null;
  onClose: () => void;
}

export function ShareModal({ groupId, groupState, onClose }: ShareModalProps) {
  const showToast = useToast();
  const [peers, setPeers] = useState<PeerEntry[]>([]);
  const [peerInput, setPeerInput] = useState("");
  const [updatingKeys, setUpdatingKeys] = useState(false);

  const fetchPeers = useCallback(async () => {
    try {
      const result = await tauri.listPeers(groupId);
      setPeers(result);
    } catch (error) {
      console.error("Failed to fetch peers:", error);
    }
  }, [groupId]);

  useEffect(() => {
    fetchPeers();
  }, [fetchPeers, groupState]);

  const handleAddPeer = useCallback(async () => {
    const input = peerInput.trim();
    if (!input) {
      showToast("Paste an invite code or acceptor address", "error");
      return;
    }
    try {
      await tauri.addPeer(groupId, input);
      showToast("Peer added!", "success");
      setPeerInput("");
      await fetchPeers();
    } catch (error) {
      console.error("Failed to add peer:", error);
      showToast(`Failed to add peer: ${error}`, "error");
    }
  }, [groupId, peerInput, showToast, fetchPeers]);

  const handleRemoveMember = useCallback(
    async (index: number) => {
      try {
        await tauri.removeMember(groupId, index);
        showToast("Member removed", "success");
        await fetchPeers();
      } catch (error) {
        console.error("Failed to remove member:", error);
        showToast(`Failed to remove member: ${error}`, "error");
      }
    },
    [groupId, showToast, fetchPeers],
  );

  const handleRemoveAcceptor = useCallback(
    async (id: string) => {
      try {
        await tauri.removeAcceptor(groupId, id);
        showToast("Acceptor removed", "success");
        await fetchPeers();
      } catch (error) {
        console.error("Failed to remove acceptor:", error);
        showToast(`Failed to remove acceptor: ${error}`, "error");
      }
    },
    [groupId, showToast, fetchPeers],
  );

  const handleUpdateKeys = useCallback(async () => {
    try {
      setUpdatingKeys(true);
      await tauri.updateKeys(groupId);
      showToast("Keys updated! Epoch advanced.", "success");
    } catch (error) {
      console.error("Failed to update keys:", error);
      showToast(`Failed to update keys: ${error}`, "error");
    } finally {
      setUpdatingKeys(false);
    }
  }, [groupId, showToast]);

  const copyTranscriptHash = useCallback(async () => {
    if (!groupState) return;
    try {
      await navigator.clipboard.writeText(groupState.transcript_hash);
      showToast("Transcript hash copied!", "success");
    } catch {
      showToast("Failed to copy", "error");
    }
  }, [groupState, showToast]);

  const members = peers.filter(
    (p): p is Extract<PeerEntry, { kind: "Member" }> => p.kind === "Member",
  );
  const acceptors = peers.filter(
    (p): p is Extract<PeerEntry, { kind: "Acceptor" }> => p.kind === "Acceptor",
  );

  const shortHash = groupState
    ? groupState.transcript_hash.slice(0, 16) + "…"
    : "—";

  return (
    <div className="modal">
      <div className="modal-backdrop" onClick={onClose} />
      <div className="modal-content">
        <div className="modal-header">
          <h3>Share</h3>
          <button className="modal-close" onClick={onClose}>
            &times;
          </button>
        </div>
        <div className="modal-body">
          <div className="group-info-card">
            <div className="group-info-row">
              <span className="group-info-label">Epoch</span>
              <span className="group-info-value">
                {groupState ? groupState.epoch : "—"}
              </span>
            </div>
            <div className="group-info-row">
              <span className="group-info-label">Transcript</span>
              <span
                className="group-info-value group-info-hash"
                title={
                  groupState
                    ? `${groupState.transcript_hash}\n(click to copy)`
                    : ""
                }
                onClick={copyTranscriptHash}
              >
                {shortHash}
              </span>
            </div>
            <div className="group-info-row">
              <span className="group-info-label">Members</span>
              <span className="group-info-value">
                {groupState ? groupState.member_count : "—"}
              </span>
            </div>
            <button
              className="btn btn-secondary btn-sm btn-block"
              onClick={handleUpdateKeys}
              disabled={updatingKeys}
            >
              {updatingKeys ? "⏳ Updating…" : "🔑 Update Keys"}
            </button>
          </div>

          <div className="peer-list">
            {peers.length === 0 ? (
              <div className="peer-empty">No peers</div>
            ) : (
              <>
                {members.length > 0 && (
                  <>
                    <div className="peer-section-label">Members</div>
                    {members.map((m) => (
                      <div key={m.index} className="peer-item">
                        <div className="peer-item-info">
                          <span className="peer-item-id" title={m.identity}>
                            {shortId(m.identity)}
                            {m.is_self && (
                              <span className="peer-item-tag self-tag">
                                {" "}
                                (you)
                              </span>
                            )}
                          </span>
                        </div>
                        {!m.is_self && (
                          <button
                            className="peer-item-remove"
                            title="Remove"
                            onClick={() => handleRemoveMember(m.index)}
                          >
                            ✕
                          </button>
                        )}
                      </div>
                    ))}
                  </>
                )}
                {acceptors.length > 0 && (
                  <>
                    <div className="peer-section-label">Acceptors</div>
                    {acceptors.map((a) => (
                      <div key={a.id} className="peer-item">
                        <div className="peer-item-info">
                          <span className="peer-item-id" title={a.id}>
                            {shortId(a.id)}
                          </span>
                        </div>
                        <button
                          className="peer-item-remove"
                          title="Remove"
                          onClick={() => handleRemoveAcceptor(a.id)}
                        >
                          ✕
                        </button>
                      </div>
                    ))}
                  </>
                )}
              </>
            )}
          </div>

          <div className="peer-add-form">
            <label>
              <span>Add Peer</span>
              <small>Paste an invite code (member) or acceptor address</small>
            </label>
            <div className="peer-input-row">
              <textarea
                className="input-textarea"
                placeholder="Paste invite code or acceptor address..."
                rows={3}
                value={peerInput}
                onChange={(e) => setPeerInput(e.target.value)}
              />
              <button
                className="btn btn-primary btn-sm"
                onClick={handleAddPeer}
              >
                Add
              </button>
            </div>
          </div>
        </div>
        <div className="modal-footer">
          <button className="btn btn-primary" onClick={onClose}>
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
