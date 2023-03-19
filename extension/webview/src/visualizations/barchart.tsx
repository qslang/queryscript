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
  console.log(JSON.stringify(data));
  const spec = {
    width: 500,
    height: 500,
    mark: "bar",
    encoding: {
      x: { field: "round_number", type: "ordinal" },
      y: { field: "teams", type: "quantitative" },
    },
    data: { name: "table" }, // note: vega-lite data attribute is a plain object instead of an array
  } as VisualizationSpec;

  const barData = {
    table: JSON.parse(
      "[{\"round_number\":0,\"teams\":30},{\"round_number\":1,\"teams\":30},{\"round_number\":2,\"teams\":27},{\"round_number\":3,\"teams\":21},{\"round_number\":4,\"teams\":13}]"
    ),
  };

  return (
    <>
      <VegaLite spec={spec} data={barData} />
    </>
  );
};

export default BarChart;
