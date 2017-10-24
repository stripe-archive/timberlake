import React from 'react';

/**
 * Renders an edge in the DAG view.
 */
export default class extends React.Component {
  render() {
    const {points} = this.props;
    let path = `M ${points[0].x},${points[0].y}`;
    for (let i = 1; i < points.length; i += 1) {
      path += ` L ${points[i].x},${points[i].y}`;
    }
    return (
      <path
        d={path}
        style={{
          markerEnd: 'url(#arrow)',
          stroke: 'black',
          strokeWidth: 2,
          fill: 'transparent',
        }}
      />
    );
  }
}
