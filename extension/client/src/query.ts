import * as vscode from "vscode";
import { LanguageClient } from "vscode-languageclient/node";
import * as path from "path";

import { RunExprType, RunExprResult } from "./api";

export function runExpr(
  context: vscode.ExtensionContext,
  client: LanguageClient
) {
  return async (arg_uri?: string, arg_expr?: RunExprType) => {
    const uri =
      arg_uri || vscode.window.activeTextEditor?.document.uri.toString();
    const expr: RunExprType = arg_expr || {
      Position: vscode.window.activeTextEditor?.selection.active,
    };

    const resp = (await client.sendRequest("queryscript/runExpr", {
      uri,
      expr,
    })) as RunExprResult;

    const panel = ReactPanel.createOrShow(
      context.extensionPath,
      vscode.ViewColumn.Beside
    );

    panel.ready.then(() => {
      panel.sendMessage(resp);
    });
  };
}

// This is modified from https://github.com/rebornix/vscode-webview-react
/**
 * Manages react webview panels
 */
class ReactPanel {
  /**
   * Track the currently panel. Only allow a single panel to exist at a time.
   */
  public static currentPanel: ReactPanel | undefined;

  private static readonly viewType = "react";

  private readonly _panel: vscode.WebviewPanel;
  private readonly _extensionPath: string;
  private _disposables: vscode.Disposable[] = [];

  public ready: Promise<void>;

  public static createOrShow(
    extensionPath: string,
    viewColumn?: vscode.ViewColumn
  ): ReactPanel {
    // If we already have a panel, show it.
    // Otherwise, create a new panel.
    if (ReactPanel.currentPanel) {
      ReactPanel.currentPanel._panel.reveal(viewColumn, true /*preserveFocus*/);
    } else {
      ReactPanel.currentPanel = new ReactPanel(
        extensionPath,
        viewColumn || vscode.ViewColumn.One
      );
    }
    return ReactPanel.currentPanel;
  }

  private constructor(extensionPath: string, viewColumn: vscode.ViewColumn) {
    this._extensionPath = extensionPath;

    // Create and show a new webview panel
    this._panel = vscode.window.createWebviewPanel(
      ReactPanel.viewType,
      "React",
      {
        viewColumn,
        preserveFocus: true,
      },
      {
        // Enable javascript in the webview
        enableScripts: true,

        // And restrict the webview to only loading content from our extension's `media` directory.
        localResourceRoots: [
          vscode.Uri.file(path.join(this._extensionPath, "out")),
        ],
      }
    );

    // Set the webview's initial html content
    this._panel.webview.html = this._getHtmlForWebview();

    // Listen for when the panel is disposed
    // This happens when the user closes the panel or when the panel is closed programatically
    this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

    let readyResolve: () => void;
    this.ready = new Promise((resolve) => {
      readyResolve = resolve;
    });

    // Handle messages from the webview
    this._panel.webview.onDidReceiveMessage(
      (message) => {
        switch (message.command) {
          case "alert":
            vscode.window.showErrorMessage(message.text);
            return;
          case "ready":
            readyResolve();
            return;
        }
      },
      null,
      this._disposables
    );
  }

  public sendMessage(message: any) {
    // Send a message to the webview webview.
    // You can send any JSON serializable data.
    this._panel.webview.postMessage(message);
  }

  public dispose() {
    ReactPanel.currentPanel = undefined;

    // Clean up our resources
    this._panel.dispose();

    while (this._disposables.length) {
      const x = this._disposables.pop();
      if (x) {
        x.dispose();
      }
    }
  }

  private _getHtmlForWebview() {
    const mainScript = path.join(this._extensionPath, "out", "webview.js");
    const mainStyle = path.join(this._extensionPath, "out", "webview.css");

    const scriptPathOnDisk = vscode.Uri.file(mainScript);
    const scriptUri = this._panel.webview.asWebviewUri(scriptPathOnDisk);
    const stylePathOnDisk = vscode.Uri.file(mainStyle);
    const styleUri = this._panel.webview.asWebviewUri(stylePathOnDisk);

    // Use a nonce to whitelist which scripts can be run
    const nonce = getNonce();

    return `<!DOCTYPE html>
			<html lang="en">
			<head>
				<meta charset="utf-8">
				<meta name="viewport" content="width=device-width, initial-scale=1.0">
				<title>QueryVM Results</title>
				<link rel="stylesheet" type="text/css" href="${styleUri}">
				<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src vscode-resource: https:; script-src 'nonce-${nonce}' 'unsafe-eval';style-src vscode-resource: 'unsafe-inline' http: https: data:;">
			</head>
			<body>
				<noscript>You need to enable JavaScript to run this app.</noscript>
				<div id="root"></div>
				<script nonce="${nonce}" src="${scriptUri}"></script>
			</body>
			</html>`;
  }
}

function getNonce() {
  let text = "";
  const possible =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}
