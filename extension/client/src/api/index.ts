import { Type } from "queryscript/Type";
import { Position } from "vscode";

export type RunExprType =
  | { Query: number }
  | { Expr: string }
  | { Position: Position };
export interface RunExprResult {
  value: any;
  type: Type;
  viz: any;
}
