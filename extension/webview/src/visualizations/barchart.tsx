import React, { useEffect } from "react";
import { VegaLite, VisualizationSpec } from "react-vega";
import { Mark } from "vega-lite/build/src/mark";

import { Type } from "queryscript/Type";
import { normalize } from "path";

interface BarChartProps {
  data: any;

  // TODO We should make this take the list's type as input, or assert that we
  // have a list here up front
  schema: Type;
}

export const BarChart = ({ data, schema }: BarChartProps) => {
  const spec = {
    width: 500,
    height: 500,
    mark: "bar",
    encoding: {
      x: { field: "a", type: "ordinal" },
      y: { field: "b", type: "quantitative" },
    },
    data: { name: "table" }, // note: vega-lite data attribute is a plain object instead of an array
  } as VisualizationSpec;

  const barData = {
    table: [
      { a: "A", b: 28 },
      { a: "B", b: 55 },
      { a: "C", b: 43 },
      { a: "D", b: 91 },
      { a: "E", b: 81 },
      { a: "F", b: 53 },
      { a: "G", b: 19 },
      { a: "H", b: 87 },
      { a: "I", b: 52 },
    ],
  };

  return (
    <>
      <VegaLite spec={spec} data={barData} />
    </>
  );
};

export default BarChart;
