/* --------------------------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 * ------------------------------------------------------------------------------------------ */

import { commands, window, workspace, ExtensionContext, Uri } from "vscode";

import {
  Executable,
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

import { runExpr } from "./query";

let client: LanguageClient;

export function activate(context: ExtensionContext) {
  const ext = process.platform === "win32" ? ".exe" : "";
  const binName = `qs-lsp${ext}`;

  // TODO: We should allow the user's global qs-lsp to be an option here too.
  const command =
    process.env.SERVER_PATH ||
    Uri.joinPath(context.extensionUri, "server", binName).fsPath;

  context.extensionUri;
  const run: Executable = {
    command,
    options: {
      env: {
        ...process.env,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        RUST_LOG: "debug",
      },
    },
  };
  const serverOptions: ServerOptions = {
    run,
    debug: run,
  };

  // Options to control the language client
  const clientOptions: LanguageClientOptions = {
    // Register the server for plain text documents
    documentSelector: [{ scheme: "file", language: "QueryScript" }],
    synchronize: {
      // Notify the server about file changes to '.clientrc files contained in the workspace
      fileEvents: workspace.createFileSystemWatcher("**/.clientrc"),
    },
  };

  // Create the language client and start the client.
  client = new LanguageClient(
    "QueryScriptLSP",
    "QueryScript Language Server",
    serverOptions,
    clientOptions
  );

  context.subscriptions.push(
    commands.registerCommand("runExpr.start", runExpr(context, client))
  );

  // Start the client. This will also launch the server
  client.start();
}

export function deactivate(): Thenable<void> | undefined {
  if (!client) {
    return undefined;
  }
  return client.stop();
}
