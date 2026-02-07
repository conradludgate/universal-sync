import type { Delta, SyncStatus } from "../types";
import { EditorToolbar } from "./EditorToolbar";
import { MonacoWrapper } from "./MonacoWrapper";

interface EditorViewProps {
  groupId: string;
  text: string;
  deltas: Delta[] | null;
  syncStatus: SyncStatus;
  onShare: () => void;
}

export function EditorView({
  groupId,
  text,
  deltas,
  syncStatus,
  onShare,
}: EditorViewProps) {
  return (
    <div className="editor-container">
      <EditorToolbar
        groupId={groupId}
        syncStatus={syncStatus}
        onShare={onShare}
      />
      <MonacoWrapper groupId={groupId} text={text} deltas={deltas} />
    </div>
  );
}
