import React from "react";
import {
  VSCodeDataGrid,
  VSCodeDataGridRow,
  VSCodeDataGridCell,
} from "@vscode/webview-ui-toolkit/react";
import { match, P } from "ts-pattern";

import { Type } from "queryscript/Type";
import { normalize } from "path";

interface TableProps {
  data: any;

  // TODO We should make this take the list's type as input, or assert that we
  // have a list here up front
  schema: Type;
}

export const Table = ({ data, schema }: TableProps) => {
  // Switch on the options for schema, and get the fields of the list
  // if it is one, otherwise return an error
  const { fields, error } = match<Type>(schema)
    .with({ List: { Record: P.select("fields") } }, ({ fields }) => ({
      fields,
      error: null,
    }))
    .with(P._, () => ({
      fields: [],
      error: "Table visualization only supports lists of records",
    }))
    .exhaustive();

  if (error) {
    // NOTE: This should really be an assert (because we should only offer this visualization option if the
    // above logic typechecks).
    return <pre>{error}</pre>;
  }

  return (
    <>
      <VSCodeDataGrid
        aria-label="Results"
        gridTemplateColumns={"minmax(120px, 1fr) ".repeat(fields.length)}
      >
        <VSCodeDataGridRow row-type="header">
          {fields.map((field, idx) => (
            <VSCodeDataGridCell
              cell-type="columnheader"
              grid-column={idx + 1}
              key={idx}
            >
              {field.name}
            </VSCodeDataGridCell>
          ))}
        </VSCodeDataGridRow>
        {(data as any[]).map((row, ridx) => (
          <VSCodeDataGridRow key={ridx}>
            {fields.map((field, idx) => (
              <VSCodeDataGridCell grid-column={idx + 1} key={idx}>
                {normalizeForDisplay(row[field.name as string])}
              </VSCodeDataGridCell>
            ))}
          </VSCodeDataGridRow>
        ))}
      </VSCodeDataGrid>
    </>
  );
};

function normalizeForDisplay(x: any): string {
  if (x === null) {
    return "null";
  } else if (x === undefined) {
    return "undefined";
  } else if (typeof x === "string") {
    return x;
  } else if (typeof x === "number") {
    return x.toString();
  } else if (typeof x === "boolean") {
    return x.toString();
  } else if (Array.isArray(x)) {
    return x.map(normalizeForDisplay).join(", ");
  } else if (typeof x === "object") {
    return JSON.stringify(x);
  } else {
    return x.toString();
  }
}

export default Table;
