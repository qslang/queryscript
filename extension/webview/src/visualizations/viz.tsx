import React, { useEffect, useState } from "react";
import { VegaLite, VisualizationSpec } from "react-vega";
import { Type } from "queryscript/Type";

interface VizProps {
  data: any;

  // TODO We should make this take the list's type as input, or assert that we
  // have a list here up front
  schema: Type;

  viz: any;
}

function approxSize(dim: number) {
  return Math.min(Math.max(dim / 2, 500), dim);
}

export const Viz = ({ data, schema, viz: vizProp }: VizProps) => {
  const [viz, setViz] = useState<VisualizationSpec | null>(null);

  useEffect(() => {
    if (vizProp) {
      vizProp["data"] = { name: "table" };
      const squareSize = Math.min(
        approxSize(window.innerWidth),
        approxSize(window.innerHeight)
      );
      if (!vizProp["width"]) {
        vizProp["width"] = squareSize;
      }
      if (!vizProp["height"]) {
        vizProp["height"] = squareSize;
      }
    }
    console.log(vizProp);
    setViz(vizProp);
  }, [vizProp]);

  return <>{viz ? <VegaLite spec={viz} data={{ table: data }} /> : null}</>;
};

export default Viz;
