import React, { useCallback, useState, useEffect } from "react";
import { createRoot } from "react-dom/client";
import {
  VSCodePanels,
  VSCodePanelTab,
  VSCodePanelView,
} from "@vscode/webview-ui-toolkit/react";

import { RunExprResult } from "api";

import Table from "./visualizations/table";
import Viz from "./visualizations/viz";

import "./app.css";
import { boolean } from "ts-pattern/dist/patterns";

interface Panel {
  tab: string;
  panel: JSX.Element;
}

declare global {
  interface Window {
    darkMode: boolean;
  }
}

interface vscode {
  postMessage(message: any): void;
}

declare let acquireVsCodeApi: any;
const vscode: vscode = acquireVsCodeApi();

type Message = { theme: { darkMode: boolean }; data: RunExprResult };

const App = () => {
  const [data, setData] = useState<RunExprResult>({
    value: null,
    type: { Atom: "Null" },
    viz: null,
  });

  const [darkMode, setDarkMode] = useState<boolean>(false);

  useEffect(() => {
    const onMessage = (event: MessageEvent<Message>) => {
      setDarkMode(event.data.theme.darkMode);
      setData(event.data.data);
    };

    window.addEventListener("message", onMessage);
    vscode.postMessage({ command: "ready" });
    return () => window.removeEventListener("message", onMessage);
  });

  const panels = [];

  if (data.viz !== null) {
    panels.push({
      tab: "Viz",
      panel: (
        <Viz
          data={data.value}
          schema={data.type}
          viz={data.viz}
          darkMode={darkMode}
        />
      ),
    });
  }

  if (data.value !== null) {
    panels.push({
      tab: "Table",
      panel: <Table data={data.value} schema={data.type} />,
    });
  }

  panels.push({
    tab: "Raw",
    panel: <pre>{JSON.stringify(data, null, 2)}</pre>,
  });

  return (
    <VSCodePanels>
      {panels.map((p) => (
        <VSCodePanelTab key={p.tab}>{p.tab}</VSCodePanelTab>
      ))}
      {panels.map((p) => (
        <VSCodePanelView key={p.tab}>{p.panel}</VSCodePanelView>
      ))}
    </VSCodePanels>
  );
};

createRoot(document.getElementById("root")!).render(<App />);
