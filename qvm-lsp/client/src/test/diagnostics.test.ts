/* --------------------------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for license information.
 * ------------------------------------------------------------------------------------------ */

import * as vscode from "vscode";
import * as assert from "assert";
import { getDocUri, activate } from "./helper";

suite("Should get diagnostics", () => {
  const syntaxErrorDoc = getDocUri("syntax_error.qs");

  test("Diagnoses uppercase texts", async () => {
    await testDiagnostics(syntaxErrorDoc, [
      {
        message:
          "Compiler error: Syntax error: SQL parser error: sql parser error: Expected ;, found: error",
        range: toRange(2, 0, 2, 12),
        severity: vscode.DiagnosticSeverity.Warning,
        source: "qvm",
      },
    ]);
  });

  const bindErrorDoc = getDocUri("bind_error.qs");

  test("Diagnoses uppercase texts", async () => {
    await testDiagnostics(bindErrorDoc, [
      {
        message: 'Compiler error: No such entry: "dne"',
        range: toRange(2, 8, 2, 10),
        severity: vscode.DiagnosticSeverity.Error,
        source: "qvm",
      },
    ]);
  });
});

function toRange(sLine: number, sChar: number, eLine: number, eChar: number) {
  const start = new vscode.Position(sLine, sChar);
  const end = new vscode.Position(eLine, eChar);
  return new vscode.Range(start, end);
}

async function testDiagnostics(
  docUri: vscode.Uri,
  expectedDiagnostics: vscode.Diagnostic[]
) {
  await activate(docUri);

  const actualDiagnostics = vscode.languages.getDiagnostics(docUri);

  assert.equal(actualDiagnostics.length, expectedDiagnostics.length);

  expectedDiagnostics.forEach((expectedDiagnostic, i) => {
    const actualDiagnostic = actualDiagnostics[i];
    assert.equal(actualDiagnostic.message, expectedDiagnostic.message);
    assert.deepEqual(actualDiagnostic.range, expectedDiagnostic.range);
    assert.equal(actualDiagnostic.severity, expectedDiagnostic.severity);
  });
}
