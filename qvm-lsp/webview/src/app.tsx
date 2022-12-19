import React, { useCallback, useState, useEffect } from "react";
import ReactDOM from "react-dom";

import "./app.css";

const App = (props: { message: string }) => {
    const [count, setCount] = useState(0);
    const increment = useCallback(() => {
        setCount(count => count + 1);
    }, [count]);

    const [data, setData] = useState(null);
    useEffect(() => {
        const onMessage = (event: MessageEvent<any>) => {
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
        <h1>{props.message}</h1>
        <h2>Count: {count}</h2>
        <button onClick={increment}>Increment</button>
        <pre>{JSON.stringify(data, null, 2)}</pre>
    </>);
};

ReactDOM.render(
    <App message="Hello World! Simple Counter App built on ESBuild + React + Typescript" />,
    document.getElementById("root")
);
