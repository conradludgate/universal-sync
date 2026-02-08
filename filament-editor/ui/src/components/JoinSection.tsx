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
    </div>
  );
}
