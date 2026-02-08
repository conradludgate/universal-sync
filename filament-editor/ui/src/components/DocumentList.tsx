import type { DocumentInfo } from "../types";

function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 6) + "…" + id.slice(-6) : id;
}

interface DocumentListProps {
  documents: Map<string, DocumentInfo>;
  currentGroupId: string | null;
  onSelect: (groupId: string) => void;
  onCreate: () => void;
}

export function DocumentList({
  documents,
  currentGroupId,
  onSelect,
  onCreate,
}: DocumentListProps) {
  return (
    <div className="sidebar-section">
      <div className="sidebar-section-header">
        <span className="sidebar-section-title">Documents</span>
      </div>
      <div className="doc-list">
        {documents.size === 0 ? (
          <div className="doc-list-empty">No documents</div>
        ) : (
          Array.from(documents.entries()).map(([gid]) => (
            <div
              key={gid}
              className={`doc-item${gid === currentGroupId ? " active" : ""}`}
              onClick={() => onSelect(gid)}
            >
              <span className="doc-item-label" title={gid}>
                {shortId(gid)}
              </span>
            </div>
          ))
        )}
      </div>
      <button className="btn btn-primary btn-block" onClick={onCreate}>
        + New Document
      </button>
    </div>
  );
}
