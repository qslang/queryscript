import React from "react";
import { VSCodeDataGrid, VSCodeDataGridRow, VSCodeDataGridCell } from "@vscode/webview-ui-toolkit/react";
import { match, P } from "ts-pattern";

import { Type } from "qvm/Type";

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
		.with(
			{ List: { Record: P.select("fields") } }, ({ fields }) => ({
				fields, error: null
			})
		)
		.with(P._, () => (
			{ fields: [], error: "Table visualization only supports lists of records" }
		))
		.exhaustive();

	if (error) {
		// NOTE: This should really be an assert (because we should only offer this visualization option if the
		// above logic typechecks).
		return (<pre>{error}</pre>);
	}

	return (
		<>
			<VSCodeDataGrid aria-label="Results" gridTemplateColumns={"1fr ".repeat(fields.length)}>
				<VSCodeDataGridRow row-type="header">
					{fields.map((field, idx) => (
						<VSCodeDataGridCell cell-type="columnheader" grid-column={idx + 1}>{field.name}</VSCodeDataGridCell>
					))}
				</VSCodeDataGridRow>
				{
					(data as any[]).map((row) => (
						<VSCodeDataGridRow>
							{fields.map((field, idx) => (
								<VSCodeDataGridCell grid-column={idx + 1}>{row[field.name]}</VSCodeDataGridCell>
							))}
						</VSCodeDataGridRow>
					))
				}
			</VSCodeDataGrid>
		</>
	);

};

export default Table;