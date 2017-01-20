import React from 'react';



/**
 * View rendering the DAG of related scalding steps.
 */
export default class extends React.Component {
  render() {
    let job = this.props.job;
    let jobs = this.props.relatives;

    // Create a new dagre graph.
    var g = new dagre.graphlib.Graph().setGraph({
      marginx: 20,
      marginy: 20
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
        width: 29,
        height: 29
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

    // Link related jobs.
    dagre.layout(g);

    let width = 0, height = 0;
    g.nodes().forEach(key => {
      const node = g.node(key);
      width = Math.max(width, node.x + 40);
      height = Math.max(height, node.y + 40);
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
                            fill: key == job.id ? "rgb(232, 100, 130)" : "rgb(91, 192, 222)",
                            stroke: key == job.id ? "rgb(255, 100, 130)" : "rgb(91, 192, 255)"
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
          </svg>
        </div>
      </div>
    );
  }
}
