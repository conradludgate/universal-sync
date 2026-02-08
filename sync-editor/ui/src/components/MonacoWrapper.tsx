import Editor, { type OnMount } from "@monaco-editor/react";
import { useCallback, useEffect, useRef } from "react";
import type * as monacoNs from "monaco-editor";
import * as tauri from "../tauri";
import type { AwarenessPeer, Delta } from "../types";

const PEER_COLORS = [
  "#4fc3f7",
  "#81c784",
  "#ffb74d",
  "#e57373",
  "#ba68c8",
  "#4dd0e1",
  "#aed581",
  "#ff8a65",
];

function colorForPeer(clientId: number): string {
  return PEER_COLORS[Math.abs(clientId) % PEER_COLORS.length];
}

interface MonacoWrapperProps {
  groupId: string;
  text: string;
  deltas: Delta[] | null;
  awarenessPeers: AwarenessPeer[];
}

export function MonacoWrapper({
  groupId,
  text,
  deltas,
  awarenessPeers,
}: MonacoWrapperProps) {
  const editorRef = useRef<monacoNs.editor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<typeof monacoNs | null>(null);
  const isApplyingRemote = useRef(false);
  const groupIdRef = useRef(groupId);
  groupIdRef.current = groupId;
  const decorationsRef = useRef<monacoNs.editor.IEditorDecorationsCollection | null>(null);
  const cursorTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleEditorMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    decorationsRef.current = editor.createDecorationsCollection([]);
    editor.focus();

    editor.onDidChangeCursorSelection(() => {
      if (cursorTimerRef.current) clearTimeout(cursorTimerRef.current);
      cursorTimerRef.current = setTimeout(() => {
        const sel = editor.getSelection();
        if (!sel) return;
        const model = editor.getModel();
        if (!model) return;
        const anchor = model.getOffsetAt(sel.getStartPosition());
        const head = model.getOffsetAt(sel.getEndPosition());
        tauri
          .updateCursor(groupIdRef.current, anchor, head)
          .catch(() => {});
      }, 200);
    });
  }, []);

  const handleChange = useCallback(
    (
      _value: string | undefined,
      event: monacoNs.editor.IModelContentChangedEvent,
    ) => {
      if (isApplyingRemote.current) return;

      for (const change of event.changes) {
        const { rangeOffset, rangeLength, text: changeText } = change;

        let delta: Delta;
        if (rangeLength > 0 && changeText.length > 0) {
          delta = {
            type: "Replace",
            position: rangeOffset,
            length: rangeLength,
            text: changeText,
          };
        } else if (rangeLength > 0) {
          delta = {
            type: "Delete",
            position: rangeOffset,
            length: rangeLength,
          };
        } else if (changeText.length > 0) {
          delta = { type: "Insert", position: rangeOffset, text: changeText };
        } else {
          continue;
        }

        tauri.applyDelta(groupIdRef.current, delta).catch((error) => {
          console.error("Failed to apply delta:", error);
        });
      }
    },
    [],
  );

  useEffect(() => {
    const editor = editorRef.current;
    const monaco = monacoRef.current;
    if (!editor || !monaco) return;

    const currentText = editor.getValue();
    if (text === currentText) return;

    isApplyingRemote.current = true;
    if (deltas && deltas.length > 0) {
      const model = editor.getModel();
      if (model) {
        const edits = deltas
          .map((d) => {
            if (d.type === "Insert") {
              const pos = model.getPositionAt(d.position);
              return {
                range: new monaco.Range(
                  pos.lineNumber,
                  pos.column,
                  pos.lineNumber,
                  pos.column,
                ),
                text: d.text,
              };
            } else if (d.type === "Delete") {
              const start = model.getPositionAt(d.position);
              const end = model.getPositionAt(d.position + d.length);
              return {
                range: new monaco.Range(
                  start.lineNumber,
                  start.column,
                  end.lineNumber,
                  end.column,
                ),
                text: null as string | null,
              };
            } else if (d.type === "Replace") {
              const start = model.getPositionAt(d.position);
              const end = model.getPositionAt(d.position + d.length);
              return {
                range: new monaco.Range(
                  start.lineNumber,
                  start.column,
                  end.lineNumber,
                  end.column,
                ),
                text: d.text,
              };
            }
            return null;
          })
          .filter((e) => e !== null);
        editor.executeEdits(
          "remote-sync",
          edits as monacoNs.editor.IIdentifiedSingleEditOperation[],
        );
      }
    } else {
      editor.setValue(text);
    }
    isApplyingRemote.current = false;
  }, [text, deltas]);

  // Render remote cursors via decorations
  useEffect(() => {
    const editor = editorRef.current;
    const monaco = monacoRef.current;
    const collection = decorationsRef.current;
    if (!editor || !monaco || !collection) return;
    const model = editor.getModel();
    if (!model) return;

    // Inject per-peer CSS classes
    let styleEl = document.getElementById("remote-cursor-styles");
    if (!styleEl) {
      styleEl = document.createElement("style");
      styleEl.id = "remote-cursor-styles";
      document.head.appendChild(styleEl);
    }
    const rules: string[] = [];
    const peerIds = new Set<number>();
    for (const peer of awarenessPeers) {
      if (peer.cursor == null || peerIds.has(peer.client_id)) continue;
      peerIds.add(peer.client_id);
      const c = colorForPeer(peer.client_id);
      rules.push(
        `.rc-cursor-${peer.client_id} { border-left: 2px solid ${c}; }`,
        `.rc-sel-${peer.client_id} { background-color: ${c}33; }`,
      );
    }
    styleEl.textContent = rules.join("\n");

    const newDecorations: monacoNs.editor.IModelDeltaDecoration[] = [];
    for (const peer of awarenessPeers) {
      if (peer.cursor == null) continue;
      const cursorPos = model.getPositionAt(
        Math.min(peer.cursor, model.getValueLength()),
      );

      if (peer.selection_end != null && peer.selection_end !== peer.cursor) {
        const endPos = model.getPositionAt(
          Math.min(peer.selection_end, model.getValueLength()),
        );
        const [startPos, selEnd] =
          peer.cursor < peer.selection_end
            ? [cursorPos, endPos]
            : [endPos, cursorPos];
        newDecorations.push({
          range: new monaco.Range(
            startPos.lineNumber,
            startPos.column,
            selEnd.lineNumber,
            selEnd.column,
          ),
          options: {
            inlineClassName: `rc-sel-${peer.client_id}`,
            stickiness:
              monaco.editor.TrackedRangeStickiness
                .NeverGrowsWhenTypingAtEdges,
          },
        });
      }

      newDecorations.push({
        range: new monaco.Range(
          cursorPos.lineNumber,
          cursorPos.column,
          cursorPos.lineNumber,
          cursorPos.column,
        ),
        options: {
          beforeContentClassName: `rc-cursor-${peer.client_id}`,
          stickiness:
            monaco.editor.TrackedRangeStickiness
              .NeverGrowsWhenTypingAtEdges,
        },
      });
    }

    collection.set(newDecorations);
  }, [awarenessPeers]);

  return (
    <div className="editor-wrapper">
      <Editor
        defaultLanguage="plaintext"
        theme="vs-dark"
        onMount={handleEditorMount}
        onChange={handleChange}
        options={{
          fontFamily: "'IBM Plex Mono', monospace",
          fontSize: 15,
          lineHeight: 26,
          minimap: { enabled: false },
          wordWrap: "on",
          automaticLayout: true,
          scrollBeyondLastLine: false,
          padding: { top: 16, bottom: 16 },
        }}
      />
    </div>
  );
}
