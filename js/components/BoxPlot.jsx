import React from 'react';

const {d3} = window;

function boxplot(data, node, tickFormat) {
  const margin = {
    top: 10,
    right: 100,
    bottom: 20,
    left: 100,
  };
  const width = 220 - margin.left - margin.right;
  const height = 400 - margin.top - margin.bottom;

  const chart = d3.box()
    .whiskers(iqr(1.5))
    .width(width)
    .height(height)
    .tickFormat(tickFormat);

  chart.domain(d3.extent(data));

  d3.select(node).selectAll('svg')
    .data([data])
    .enter().append('svg')
    .attr('class', 'box')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.bottom + margin.top)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)
    .call(chart);

  function iqr(k) {
    return function(d) {
      const q1 = d.quartiles[0];
      const q3 = d.quartiles[2];
      const iqrange = (q3 - q1) * k;
      let i = 0;
      let j = d.length - 1;
      while (d[i] < q1 - iqrange) {
        i += 1;
      }
      while (d[j] > q3 + iqrange) {
        j -= 1;
      }
      return [i, j];
    };
  }
}

export default class extends React.Component {
  componentDidMount() {
    boxplot(this.props.data, this.node, this.props.tickFormat);
  }

  shouldComponentUpdate() {
    d3.select(this.node).selectAll('svg').remove();
    boxplot(this.props.data, this.node, this.props.tickFormat);
    return false;
  }

  render() {
    return <div />;
  }
}
