import React from 'react';



/**
 * View rendering the DAG of related scalding steps.
 */
export default class extends React.Component {
  render() {
    let job = this.props.job;
    let jobs = this.props.relatives;

    // Graph settings.
    let margin = 20, size = 30;

    // Create a new dagre graph.
    let g = new dagre.graphlib.Graph().setGraph({
      marginx: margin,
      marginy: margin
    });

    g.setDefaultEdgeLabel(function() { return {}; });

    // Find all jobs and files. Compute the time span.
    let inputMap = [], outputMap = [], files = new Set();
    for (const job of jobs) {
      // Find input & output jobs.
      let inputs = job.conf.input ? job.conf.input.split(/,/g) : [];
      for (const input of inputs) {
        (inputMap[input] = inputMap[input] || []).push(job.id);
        files.add(input);
      }
      let outputs = job.conf.output ? job.conf.output.split(/,/g) : [];
      for (const output of outputs) {
        (outputMap[output] = outputMap[output] || []).push(job.id);
        files.add(output);
      }

      // Create a graph node.
      g.setNode(job.id, {
        label: /^[^0-9]+\(([0-9]+)/.exec(job.name)[1],
        width: size,
        height: size
      });
    }

    // Link jobs based on their inputs & outputs.
    for (const file of files) {
      for (const input of inputMap[file] || []) {
        for (const output of outputMap[file] || []) {
          g.setEdge(output, input);
        }
      }
    }

    dagre.layout(g);

    // Make the canvas large enough to hold the graph.
    let width = 0, height = 0;
    g.nodes().forEach(key => {
      const node = g.node(key);
      width = Math.max(width, node.x + margin * 2 + size);
      height = Math.max(height, node.y + margin * 2 + size);
    });

    return (
      <div>
        <h4>DAG</h4>
        <div>
          <svg width={width} height={height} style={{display: "block", margin: "auto"}}>
            <defs>
              <marker
                  id="arrow"
                  markerWidth="10"
                  markerHeight="10"
                  refX="11"
                  refY="3"
                  orient="auto"
                  markerUnits="strokeWidth">
                <path d="M0,0 L0,6 L9,3 z" fill="#000" />
              </marker>
            </defs>
            <g transform={`translate(${margin}, ${margin})`}>
              {g.edges().map((key, i) => {
                const edge = g.edge(key);
                let path = `M ${edge.points[0].x},${edge.points[0].y}`;
                for (let i = 1; i < edge.points.length; ++i) {
                  path += ` L ${edge.points[i].x},${edge.points[i].y}`;
                }
                return (
                    <path
                        key={i}
                        d={path}
                        style={{
                            markerEnd: "url(#arrow)",
                            stroke: "black",
                            strokeWidth: 2,
                            fill: "transparent"
                        }}
                    />
                );
              })}
              {g.nodes().map((key, i) => {
                const node = g.node(key);
                return (
                    <g key={i} transform={`translate(${node.x}, ${node.y})`}>
                      <circle
                          r="20"
                          style={{
                              fill: key == job.id ? "rgb(100, 232, 130)" : "rgb(91, 192, 222)",
                              stroke: key == job.id ? "rgb(100, 255, 130)" : "rgb(91, 192, 255)"
                          }} />
                      <a href={`/#/job/${key}`}>
                        <text style={{
                            textAnchor: "middle",
                            alignmentBaseline: "middle"}}>
                            {node.label}
                        </text>
                      </a>
                    </g>
                );
              })}
            </g>
          </svg>
        </div>
      </div>
    );
  }
}
