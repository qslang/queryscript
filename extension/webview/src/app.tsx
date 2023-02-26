import React, { useCallback, useState, useEffect } from "react";
import ReactDOM from "react-dom";
import {
  VSCodePanels,
  VSCodePanelTab,
  VSCodePanelView,
} from "@vscode/webview-ui-toolkit/react";

import { RunExprResult } from "api";

import Table from "./visualizations/table";
import BarChart from "./visualizations/barchart";

import "./app.css";

interface Panel {
  tab: string;
  panel: JSX.Element;
}

const App = () => {
  const [data, setData] = useState<RunExprResult>({
    value: null,
    type: { Atom: "Null" },
  });
  useEffect(() => {
    const onMessage = (event: MessageEvent<RunExprResult>) => {
      setData(event.data);
    };

    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  });

  const panels = [];

  if (data.value !== null) {
    panels.push({
      tab: "Table",
      panel: <Table data={data.value} schema={data.type} />,
    });
  }

  panels.push({
    tab: "Bar chart",
    panel: <BarChart data={data.value} schema={data.type} />,
  });

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

ReactDOM.render(<App />, document.getElementById("root"));
