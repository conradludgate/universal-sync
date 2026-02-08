import type { AwarenessPeer, Delta, SyncStatus } from "../types";
import { EditorToolbar } from "./EditorToolbar";
import { MonacoWrapper } from "./MonacoWrapper";

interface EditorViewProps {
  groupId: string;
  text: string;
  deltas: Delta[] | null;
  syncStatus: SyncStatus;
  awarenessPeers: AwarenessPeer[];
  onShare: () => void;
}

export function EditorView({
  groupId,
  text,
  deltas,
  syncStatus,
  awarenessPeers,
  onShare,
}: EditorViewProps) {
  return (
    <div className="editor-container">
      <EditorToolbar
        groupId={groupId}
        syncStatus={syncStatus}
        onShare={onShare}
      />
      <MonacoWrapper
        groupId={groupId}
        text={text}
        deltas={deltas}
        awarenessPeers={awarenessPeers}
      />
    </div>
  );
}
