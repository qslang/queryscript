export function runQuery(client) {
	return async (uri, idx) => {
		console.log("runQuery");
		console.log(uri, idx);
		const foo = await client.sendRequest("qvm/runQuery", { uri, idx });
	};
}