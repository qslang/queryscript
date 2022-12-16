import { Type } from "qvm/Type";

interface RunQueryResult {
	value: any;
	type: Type;
}

export function runQuery(client) {
	return async (uri, idx) => {
		console.log("runQuery");
		console.log(uri, idx);
		const foo = await client.sendRequest("qvm/runQuery", { uri, idx }) as RunQueryResult;
		console.log(foo);
	};
}