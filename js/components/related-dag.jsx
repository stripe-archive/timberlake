import React from 'react';
import DAGEdge from './DAGEdge';
import DAGNode from './DAGNode';
import {jobLabel} from '../utils';

const {dagre} = window;

/**
 * View rendering the DAG of related scalding steps.
 */
export default class RelatedDAG extends React.Component {
  render() {
    const {job, jobConfs} = this.props;
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
      const jobConf = jobConfs[currentJob.id];
      if (jobConf === undefined) { return null; }
      const {conf} = jobConf;
      const inputs = conf.input ? conf.input.split(/,/g) : [];
      inputs.forEach((input) => {
        (inputMap[input] = inputMap[input] || []).push(currentJob.id);
        files.add(input);
      });
      const outputs = conf.output ? conf.output.split(/,/g) : [];
      outputs.forEach((output) => {
        (outputMap[output] = outputMap[output] || []).push(currentJob.id);
        files.add(output);
      });

      // this regex breaks sometimes, but we still use it by default for
      // backcompat
      const oldLabel = /^[^(]+\(([0-9]+)/.exec(currentJob.name);
      let label;
      if (oldLabel === null) {
        label = jobLabel(currentJob.name);
      } else {
        [, label] = oldLabel;
      }
      // Create a graph node.
      g.setNode(currentJob.id, {
        label,
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
          <svg
            height={height}
            style={{display: 'block', margin: 'auto'}}
            width={width}
          >
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
              {g.edges().map((id, i) => (<DAGEdge
                key={id.v + id.w}
                points={g.edge(id).points}
              />))}
              {g.nodes().map((id, i) => (<DAGNode
                key={id}
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
