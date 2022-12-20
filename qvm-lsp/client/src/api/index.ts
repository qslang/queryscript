import { Type } from "qvm/Type";
import { Position } from "vscode";

export type RunExprType = { Query: number } | { Expr: string } | { Position: Position };
export interface RunExprResult {
	value: any;
	type: Type;
}
