import { listen } from "@tauri-apps/api/event";
import { useEffect } from "react";
import type {
  AwarenessPayload,
  DocumentUpdatedPayload,
  GroupStatePayload,
} from "../types";

export function useTauriEvents(handlers: {
  onDocumentUpdated: (payload: DocumentUpdatedPayload) => void;
  onGroupStateChanged: (payload: GroupStatePayload) => void;
  onAwarenessChanged: (payload: AwarenessPayload) => void;
}) {
  useEffect(() => {
    const unlisteners: Array<() => void> = [];

    (async () => {
      unlisteners.push(
        await listen<DocumentUpdatedPayload>("document-updated", (event) => {
          handlers.onDocumentUpdated(event.payload);
        }),
      );
      unlisteners.push(
        await listen<GroupStatePayload>("group-state-changed", (event) => {
          handlers.onGroupStateChanged(event.payload);
        }),
      );
      unlisteners.push(
        await listen<AwarenessPayload>("awareness-changed", (event) => {
          handlers.onAwarenessChanged(event.payload);
        }),
      );
    })();

    return () => {
      unlisteners.forEach((fn) => fn());
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
}
