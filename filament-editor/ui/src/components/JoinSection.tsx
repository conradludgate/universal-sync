import { useCallback, useRef, useState } from "react";
import * as tauri from "../tauri";
import { useToast } from "../hooks/useToast";
import type { DocumentInfo } from "../types";

interface JoinSectionProps {
  onDocumentJoined: (doc: DocumentInfo) => void;
}

export function JoinSection({ onDocumentJoined }: JoinSectionProps) {
  const showToast = useToast();
  const [active, setActive] = useState(false);
  const [inviteCode, setInviteCode] = useState("");
  const [joinStatus, setJoinStatus] = useState<"waiting" | "joined">(
    "waiting",
  );
  const welcomeActiveRef = useRef(false);

  const [externalInvite, setExternalInvite] = useState("");
  const [externalJoining, setExternalJoining] = useState(false);

  const startJoinFlow = useCallback(async () => {
    setActive(true);
    setInviteCode("Generating…");
    setJoinStatus("waiting");

    try {
      const code = await tauri.getKeyPackage();
      setInviteCode(code);

      welcomeActiveRef.current = true;
      try {
        const doc = await tauri.recvWelcome();
        if (doc && welcomeActiveRef.current) {
          setJoinStatus("joined");
          onDocumentJoined(doc);
          showToast("Joined document!", "success");
          setTimeout(() => {
            welcomeActiveRef.current = false;
            setActive(false);
          }, 1500);
        }
      } catch (error) {
        if (welcomeActiveRef.current) {
          console.error("Welcome listener error:", error);
          showToast(`Failed to join: ${error}`, "error");
        }
      } finally {
        welcomeActiveRef.current = false;
      }
    } catch {
      setInviteCode("Error");
      showToast("Failed to generate invite code", "error");
    }
  }, [onDocumentJoined, showToast]);

  const cancelJoinFlow = useCallback(() => {
    welcomeActiveRef.current = false;
    setActive(false);
  }, []);

  const copyInviteCode = useCallback(async () => {
    if (!inviteCode || inviteCode === "Generating…" || inviteCode === "Error")
      return;
    try {
      await navigator.clipboard.writeText(inviteCode);
      showToast("Invite code copied!", "success");
    } catch {
      showToast("Failed to copy", "error");
    }
  }, [inviteCode, showToast]);

  const handleJoinExternal = useCallback(async () => {
    const code = externalInvite.trim();
    if (!code) {
      showToast("Paste an external invite code", "error");
      return;
    }
    setExternalJoining(true);
    try {
      const doc = await tauri.joinExternal(code);
      onDocumentJoined(doc);
      showToast("Joined document!", "success");
      setExternalInvite("");
    } catch (error) {
      console.error("External join error:", error);
      showToast(`Failed to join: ${error}`, "error");
    } finally {
      setExternalJoining(false);
    }
  }, [externalInvite, onDocumentJoined, showToast]);

  return (
    <div className="sidebar-section">
      <div className="sidebar-section-header">
        <span className="sidebar-section-title">Join</span>
      </div>
      <div className="join-inline">
        {!active ? (
          <div className="join-idle">
            <button
              className="btn btn-secondary btn-block"
              onClick={startJoinFlow}
            >
              Generate Invite Code
            </button>
          </div>
        ) : (
          <div className="join-active">
            <div className="invite-code-box">
              <textarea
                className="input-textarea invite-code"
                readOnly
                rows={3}
                placeholder="Generating..."
                value={inviteCode}
              />
              <button
                className="btn-icon-only"
                title="Copy invite code"
                onClick={copyInviteCode}
              >
                📋
              </button>
            </div>
            <div
              className={`join-status${joinStatus === "joined" ? " success" : ""}`}
            >
              <span className="join-status-icon">
                {joinStatus === "joined" ? "✓" : "⏳"}
              </span>
              <span className="join-status-text">
                {joinStatus === "joined" ? "Joined!" : "Waiting to be added…"}
              </span>
            </div>
            <button
              className="btn btn-secondary btn-block btn-sm"
              onClick={cancelJoinFlow}
            >
              Cancel
            </button>
          </div>
        )}
      </div>

      <div className="sidebar-section-header" style={{ marginTop: "0.75rem" }}>
        <span className="sidebar-section-title">Join with Invite</span>
      </div>
      <div className="join-inline">
        <div className="peer-input-row">
          <textarea
            className="input-textarea"
            placeholder="Paste external invite code..."
            rows={3}
            value={externalInvite}
            onChange={(e) => setExternalInvite(e.target.value)}
          />
          <button
            className="btn btn-primary btn-sm"
            onClick={handleJoinExternal}
            disabled={externalJoining}
          >
            {externalJoining ? "Joining…" : "Join"}
          </button>
        </div>
      </div>
    </div>
  );
}
