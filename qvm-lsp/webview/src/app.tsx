import React, { useCallback, useState, useEffect } from "react";
import ReactDOM from "react-dom";

import { Type } from "qvm/Type";
import { RunQueryResult } from "api";

import Table from "./visualizations/table";

import "./app.css";

const App = (props: { message: string }) => {
    const [count, setCount] = useState(0);
    const increment = useCallback(() => {
        setCount(count => count + 1);
    }, [count]);

    const [data, setData] = useState<RunQueryResult>({ value: null, type: { Atom: "Null" } });
    useEffect(() => {
        const onMessage = (event: MessageEvent<RunQueryResult>) => {
            console.log("EVENT");
            console.log(event);
            console.log(event.data);
            setData(event.data);
        };

        window.addEventListener("message", onMessage);
        return () => window.removeEventListener("message", onMessage);
    }
    );


    return (<>
        {data.value !== null ? <Table data={data.value} schema={data.type} /> : null}
        <pre>{JSON.stringify(data, null, 2)}</pre>
    </>);
};

ReactDOM.render(
    <App message="Hello World! Simple Counter App built on ESBuild + React + Typescript" />,
    document.getElementById("root")
);
