import * as vscode from 'vscode';

import { Type } from "qvm/Type";


interface RunQueryResult {
	value: any;
	type: Type;
}

export function runQuery(client) {
	return async (uri: string, idx: number) => {
		console.log("runQuery");
		console.log(uri, idx);
		const foo = await client.sendRequest("qvm/runQuery", { uri, idx }) as RunQueryResult;

		const panel = vscode.window.createWebviewPanel(
			'queryResult',
			'Query Result',
			vscode.ViewColumn.Two,
			{}
		);

		panel.webview.html = '<pre>' + JSON.stringify(foo, null, 2) + '</pre>';
	};
}