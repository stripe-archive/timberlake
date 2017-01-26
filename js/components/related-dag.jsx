import React from 'react';



/**
 * Renders an edge in the DAG view.
 */
class DAGEdge extends React.Component {
  render() {
    const points = this.props.points;
    let path = `M ${points[0].x},${points[0].y}`;
    for (let i = 1; i < points.length; ++i) {
      path += ` L ${points[i].x},${points[i].y}`;
    }
    return (
        <path
            d={path}
            style={{
                markerEnd: "url(#arrow)",
                stroke: "black",
                strokeWidth: 2,
                fill: "transparent"
            }}
        />
    );
  }
}

/**
 * Renders a node in the DAG view.
 */
class DAGNode extends React.Component {
  constructor(props) {
    super(props);

    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onMouseLeave = this.onMouseLeave.bind(this);

    this.state = {
      hover: false
    };
  }

  onMouseEnter() {
    this.setState({ hover: true });
    if (this.props.hover) {
      this.props.hover(this.props.node.job);
    }
  }

  onMouseLeave() {
    this.setState({ hover: false });
    if (this.props.hover) {
      this.props.hover(null);
    }
  }

  render() {
    const node = this.props.node;
    const selected = this.props.selected;

    let fillColour, strokeColour;
    if (selected) {
      fillColour = "rgb(100, 232, 130)";
      strokeColour = "rgb(100, 255, 130)";
    } else if (this.state.hover) {
      fillColour = "rgb(100, 232, 200)";
      strokeColour = "rgb(100, 255, 200)";
    } else {
      fillColour = "rgb(91, 192, 222)";
      strokeColour = "rgb(91, 192, 255)";
    }

    return (
        <g
            onMouseEnter={this.onMouseEnter}
            onMouseLeave={this.onMouseLeave}
            transform={`translate(${node.x}, ${node.y})`}>
          <a href={`/#/job/${node.job.id}`}>
            <circle r="20" style={{ fill: fillColour, stroke: strokeColour }} />
            <text
                style={{
                  textAnchor: "middle",
                  alignmentBaseline: "middle"
                }}>
                {node.label}
            </text>
          </a>
        </g>
    );
  }
}

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
        label: /^[^\(]+\(([0-9]+)/.exec(job.name)[1],
        job: job,
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
              {g.edges().map((id, i) => <DAGEdge key={i} points={g.edge(id).points}/>)}
              {g.nodes().map((id, i) => <DAGNode
                  key={i}
                  node={g.node(id)}
                  selected={id==job.id}
                  hover={this.props.hover}/>
              )}
            </g>
          </svg>
        </div>
      </div>
    );
  }
}
