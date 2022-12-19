import { Type } from "qvm/Type";

export type RunExprType = { Query: number } | { Expr: string };
export interface RunExprResult {
	value: any;
	type: Type;
}
