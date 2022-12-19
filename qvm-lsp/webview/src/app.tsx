import React, { useCallback, useState, useEffect } from "react";
import ReactDOM from "react-dom";

import { Type } from "qvm/Type";
import { RunQueryResult } from "api";

import Table from "./visualizations/table";

import "./app.css";

const App = () => {
    const [data, setData] = useState<RunQueryResult>({ value: null, type: { Atom: "Null" } });
    useEffect(() => {
        const onMessage = (event: MessageEvent<RunQueryResult>) => {
            setData(event.data);
        };

        window.addEventListener("message", onMessage);
        return () => window.removeEventListener("message", onMessage);
    });

    return (<>
        {
            data.value !== null ?
                <Table data={data.value} schema={data.type} /> :
                <pre>{JSON.stringify(data, null, 2)}</pre>
        }
    </>);
};

ReactDOM.render(
    <App />,
    document.getElementById("root")
);
