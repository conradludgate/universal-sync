import { useCallback } from "react";
import type { SyncStatus } from "../types";
import { useToast } from "../hooks/useToast";

function shortId(id: string): string {
  return id.length > 16 ? id.slice(0, 8) + "…" + id.slice(-8) : id;
}

interface EditorToolbarProps {
  groupId: string;
  syncStatus: SyncStatus;
  onShare: () => void;
}

export function EditorToolbar({
  groupId,
  syncStatus,
  onShare,
}: EditorToolbarProps) {
  const showToast = useToast();

  const copyId = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(groupId);
      showToast("Document ID copied!", "success");
    } catch {
      showToast("Failed to copy", "error");
    }
  }, [groupId, showToast]);

  const statusClass =
    syncStatus === "synced"
      ? "synced"
      : syncStatus === "syncing"
        ? "syncing"
        : "error";
  const statusText =
    syncStatus === "synced"
      ? "Online"
      : syncStatus === "syncing"
        ? "Connecting…"
        : "Offline";

  return (
    <div className="editor-toolbar">
      <div className="doc-info">
        <span className="doc-id" title={groupId}>
          {shortId(groupId)}
        </span>
        <button
          className="btn-icon-only"
          title="Copy document ID"
          onClick={copyId}
        >
          📋
        </button>
      </div>
      <div className="toolbar-right">
        <div className="sync-status">
          <span className={`sync-indicator ${statusClass}`} />
          {statusText}
        </div>
        <button className="btn btn-secondary btn-sm" onClick={onShare}>
          🔗 Share
        </button>
      </div>
    </div>
  );
}
