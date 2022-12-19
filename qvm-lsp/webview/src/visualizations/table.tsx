import React from "react";
import { VSCodeDataGrid, VSCodeDataGridRow, VSCodeDataGridCell } from "@vscode/webview-ui-toolkit/react";

import { Type } from "qvm/Type";

interface TableProps {
	data: any;

	// XXX TODO We should make this take the list's type as input, or assert that we
	// have a list here up front
	schema: Type;
}

export const Table = ({ data: input_data, schema }: TableProps) => {
	const data = React.useMemo(
		() => [
			{
				col1: "Hello",
				col2: "World",
			},
			{
				col1: "react-table",
				col2: "rocks",
			},
			{
				col1: "whatever",
				col2: "you want",
			},
		],
		[]
	);

	const columns = React.useMemo(
		() => [
			{
				Header: "Column 1",
				accessor: "col1", // accessor is the "key" in the data
			},
			{
				Header: "Column 2",
				accessor: "col2",
			},
		],
		[]
	);

	return (
		<>
			<VSCodeDataGrid aria-label="Results">
				<VSCodeDataGridRow row-type="header">
					{columns.map((col, idx) => (
						<VSCodeDataGridCell cell-type="columnheader" grid-column={idx + 1}>{col.Header}</VSCodeDataGridCell>
					))}
				</VSCodeDataGridRow>
			</VSCodeDataGrid>
		</>
	);

};

export default Table;