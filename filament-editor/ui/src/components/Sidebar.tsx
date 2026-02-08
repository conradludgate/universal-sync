import type { DocumentInfo } from "../types";
import { DocumentList } from "./DocumentList";
import { JoinSection } from "./JoinSection";

interface SidebarProps {
  collapsed: boolean;
  documents: Map<string, DocumentInfo>;
  currentGroupId: string | null;
  onSelectDocument: (groupId: string) => void;
  onCreateDocument: () => void;
  onDocumentJoined: (doc: DocumentInfo) => void;
}

export function Sidebar({
  collapsed,
  documents,
  currentGroupId,
  onSelectDocument,
  onCreateDocument,
  onDocumentJoined,
}: SidebarProps) {
  return (
    <aside className={`sidebar${collapsed ? " collapsed" : ""}`}>
      <DocumentList
        documents={documents}
        currentGroupId={currentGroupId}
        onSelect={onSelectDocument}
        onCreate={onCreateDocument}
      />
      <JoinSection onDocumentJoined={onDocumentJoined} />
    </aside>
  );
}
