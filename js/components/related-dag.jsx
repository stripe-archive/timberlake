import React from 'react';
import DAGEdge from './DAGEdge';
import DAGNode from './DAGNode';

const {dagre} = window;

/**
 * View rendering the DAG of related scalding steps.
 */
export default class extends React.Component {
  render() {
    const {job} = this.props;
    const jobs = this.props.relatives;

    // Graph settings.
    const margin = 20;
    const size = 30;

    // Create a new dagre graph.
    const g = new dagre.graphlib.Graph().setGraph({
      marginx: margin,
      marginy: margin,
    });

    g.setDefaultEdgeLabel(function() { return {}; });

    // Find all jobs and files. Compute the time span.
    const inputMap = [];
    const outputMap = [];
    const files = new Set();
    jobs.forEach((currentJob) => {
      // Find input & output jobs.
      const inputs = currentJob.conf.input ? currentJob.conf.input.split(/,/g) : [];
      inputs.forEach((input) => {
        (inputMap[input] = inputMap[input] || []).push(currentJob.id);
        files.add(input);
      });
      const outputs = currentJob.conf.output ? currentJob.conf.output.split(/,/g) : [];
      outputs.forEach((output) => {
        (outputMap[output] = outputMap[output] || []).push(currentJob.id);
        files.add(output);
      });

      // Create a graph node.
      g.setNode(currentJob.id, {
        label: /^[^(]+\(([0-9]+)/.exec(currentJob.name)[1],
        job: currentJob,
        width: size,
        height: size,
      });
    });

    // Link jobs based on their inputs & outputs.
    files.forEach((file) => {
      (inputMap[file] || []).forEach((input) => {
        (outputMap[file] || []).forEach((output) => {
          g.setEdge(output, input);
        });
      });
    });

    dagre.layout(g);

    // Make the canvas large enough to hold the graph.
    let width = 0;
    let height = 0;
    g.nodes().forEach((key) => {
      const node = g.node(key);
      width = Math.max(width, node.x + margin * 2 + size);
      height = Math.max(height, node.y + margin * 2 + size);
    });

    return (
      <div>
        <h4>DAG</h4>
        <div>
          <svg width={width} height={height} style={{display: 'block', margin: 'auto'}}>
            <defs>
              <marker
                id="arrow"
                markerWidth="10"
                markerHeight="10"
                refX="11"
                refY="3"
                orient="auto"
                markerUnits="strokeWidth"
              >
                <path d="M0,0 L0,6 L9,3 z" fill="#000" />
              </marker>
            </defs>
            <g transform={`translate(${margin}, ${margin})`}>
              {g.edges().map((id, i) => <DAGEdge key={i} points={g.edge(id).points} />)}
              {g.nodes().map((id, i) => (<DAGNode
                key={i}
                node={g.node(id)}
                selected={id === job.id}
                hover={this.props.hover}
              />))}
            </g>
          </svg>
        </div>
      </div>
    );
  }
}
