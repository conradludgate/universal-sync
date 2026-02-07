import Editor, { type OnMount } from "@monaco-editor/react";
import { useCallback, useEffect, useRef } from "react";
import type * as monacoNs from "monaco-editor";
import * as tauri from "../tauri";
import type { Delta } from "../types";

interface MonacoWrapperProps {
  groupId: string;
  text: string;
  deltas: Delta[] | null;
}

export function MonacoWrapper({ groupId, text, deltas }: MonacoWrapperProps) {
  const editorRef = useRef<monacoNs.editor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<typeof monacoNs | null>(null);
  const isApplyingRemote = useRef(false);
  const groupIdRef = useRef(groupId);
  groupIdRef.current = groupId;

  const handleEditorMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    editor.focus();
  }, []);

  const handleChange = useCallback(
    (_value: string | undefined, event: monacoNs.editor.IModelContentChangedEvent) => {
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
          delta = { type: "Delete", position: rangeOffset, length: rangeLength };
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
