import { useCallback, useEffect, useRef, useState } from "react";
import * as tauri from "./tauri";
import { useToast } from "./hooks/useToast";
import { useTauriEvents } from "./hooks/useTauriEvents";
import type {
  Delta,
  DocumentInfo,
  DocumentUpdatedPayload,
  GroupStatePayload,
  SyncStatus,
} from "./types";
import { Header } from "./components/Header";
import { Sidebar } from "./components/Sidebar";
import { EmptyState } from "./components/EmptyState";
import { EditorView } from "./components/EditorView";
import { ShareModal } from "./components/ShareModal";

export function App() {
  const showToast = useToast();

  const [documents, setDocuments] = useState<Map<string, DocumentInfo>>(
    () => new Map(),
  );
  const [currentGroupId, setCurrentGroupId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>("synced");
  const [shareModalOpen, setShareModalOpen] = useState(false);
  const [groupState, setGroupState] = useState<GroupStatePayload | null>(null);

  // Remote text updates: store text + deltas for the editor to apply
  const [remoteText, setRemoteText] = useState<string>("");
  const [remoteDeltas, setRemoteDeltas] = useState<Delta[] | null>(null);

  const currentGroupIdRef = useRef(currentGroupId);
  currentGroupIdRef.current = currentGroupId;
  const shareModalOpenRef = useRef(shareModalOpen);
  shareModalOpenRef.current = shareModalOpen;

  const fetchGroupState = useCallback(async (groupId: string) => {
    try {
      const gs = await tauri.getGroupState(groupId);
      setGroupState(gs);
      if (gs.acceptor_count === 0 || gs.connected_acceptor_count > 0) {
        setSyncStatus("synced");
      } else {
        setSyncStatus("error");
      }
    } catch (error) {
      console.error("Failed to fetch group state:", error);
    }
  }, []);

  const openDocument = useCallback(
    (doc: DocumentInfo) => {
      setDocuments((prev) => {
        const next = new Map(prev);
        next.set(doc.group_id, doc);
        return next;
      });
      setCurrentGroupId(doc.group_id);
      setRemoteText(doc.text);
      setRemoteDeltas(null);
      setSyncStatus("syncing");
      fetchGroupState(doc.group_id);
    },
    [fetchGroupState],
  );

  const handleCreateDocument = useCallback(async () => {
    try {
      setSyncStatus("syncing");
      const doc = await tauri.createDocument();
      openDocument(doc);
      showToast("Document created!", "success");
    } catch (error) {
      console.error("Failed to create document:", error);
      showToast(`Failed to create document: ${error}`, "error");
      setSyncStatus("error");
    }
  }, [openDocument, showToast]);

  const handleSelectDocument = useCallback(
    (groupId: string) => {
      setCurrentGroupId(groupId);
      setDocuments((prev) => {
        const doc = prev.get(groupId);
        if (doc) {
          setRemoteText(doc.text);
          setRemoteDeltas(null);
        }
        return prev;
      });
      setSyncStatus("syncing");
      fetchGroupState(groupId);
    },
    [fetchGroupState],
  );

  const toggleSidebar = useCallback(() => {
    setSidebarOpen((prev) => !prev);
  }, []);

  const handleOpenShareModal = useCallback(() => {
    if (!currentGroupIdRef.current) {
      showToast("No document open", "error");
      return;
    }
    setShareModalOpen(true);
    fetchGroupState(currentGroupIdRef.current);
  }, [showToast, fetchGroupState]);

  const handleCloseShareModal = useCallback(() => {
    setShareModalOpen(false);
  }, []);

  // Tauri events
  const handleDocumentUpdated = useCallback(
    (payload: DocumentUpdatedPayload) => {
      const { group_id, text, deltas } = payload;

      setDocuments((prev) => {
        const doc = prev.get(group_id);
        if (doc) {
          const next = new Map(prev);
          next.set(group_id, { ...doc, text });
          return next;
        }
        return prev;
      });

      if (currentGroupIdRef.current === group_id) {
        setRemoteText(text);
        setRemoteDeltas(deltas.length > 0 ? deltas : null);
      }
    },
    [],
  );

  const handleGroupStateChanged = useCallback(
    (payload: GroupStatePayload) => {
      if (currentGroupIdRef.current === payload.group_id) {
        setGroupState(payload);
        if (
          payload.acceptor_count === 0 ||
          payload.connected_acceptor_count > 0
        ) {
          setSyncStatus("synced");
        } else {
          setSyncStatus("error");
        }
      }
    },
    [],
  );

  useTauriEvents({
    onDocumentUpdated: handleDocumentUpdated,
    onGroupStateChanged: handleGroupStateChanged,
  });

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "n") {
        e.preventDefault();
        handleCreateDocument();
      }
      if (e.key === "Escape") {
        setShareModalOpen(false);
      }
      if ((e.metaKey || e.ctrlKey) && e.key === "b") {
        e.preventDefault();
        toggleSidebar();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [handleCreateDocument, toggleSidebar]);

  const currentDoc = currentGroupId
    ? documents.get(currentGroupId) ?? null
    : null;

  return (
    <div className="app">
      <Header onToggleSidebar={toggleSidebar} />
      <div className="app-body">
        <Sidebar
          collapsed={!sidebarOpen}
          documents={documents}
          currentGroupId={currentGroupId}
          onSelectDocument={handleSelectDocument}
          onCreateDocument={handleCreateDocument}
          onDocumentJoined={openDocument}
        />
        <main className="main">
          {currentDoc ? (
            <EditorView
              groupId={currentDoc.group_id}
              text={remoteText}
              deltas={remoteDeltas}
              syncStatus={syncStatus}
              onShare={handleOpenShareModal}
            />
          ) : (
            <EmptyState />
          )}
        </main>
      </div>

      {shareModalOpen && currentGroupId && (
        <ShareModal
          groupId={currentGroupId}
          groupState={groupState}
          onClose={handleCloseShareModal}
        />
      )}
    </div>
  );
}
