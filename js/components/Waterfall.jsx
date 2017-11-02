import React from 'react';
import {
  COLOUR_MAP,
  COLOUR_REDUCE,
} from '../utils';

const {_, d3} = window;

function waterfall(data, node, optsIn) {
  const defaults = {
    lineHeight: 1,
    barHeight: 1,
    width: 550,
    textFormat: (t) => '',
    linkFormat: null,
    fillStyle: (d) => {
      return d.type == 'map' ? COLOUR_MAP : COLOUR_REDUCE; // eslint-disable-line eqeqeq
    },
  };
  const opts = _.extend(defaults, optsIn);

  const margin = {
    top: 10, right: 20, bottom: 20, left: 20,
  };
  const width = opts.width - margin.left - margin.right;
  const height = Math.max(100, data.length * opts.lineHeight) - margin.top - margin.bottom;

  const chart = d3.waterfall()
    .width(width)
    .height(height)
    .barHeight(opts.barHeight)
    .textFormat(opts.textFormat)
    .linkFormat(opts.linkFormat)
    .barStyle(opts.fillStyle);

  const start = d3.min(_.pluck(data, 'start'));
  const finish = d3.max(_.pluck(data, 'finish'));
  chart.domain([start, finish]);

  if (((finish - start) / 1000) < 180) {
    // Show seconds if the x domain is less than three minutes.
    chart.tickFormat(d3.time.format.utc('%H:%M:%S'));
  }

  d3.select(node).selectAll('svg')
    .data([data])
    .enter().append('svg')
    .attr('class', 'waterfall')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.bottom + margin.top)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)
    .call(chart);
}

export default class extends React.Component {
  componentDidMount() {
    waterfall(this.props.data, this.node, this.props);
  }

  shouldComponentUpdate() {
    d3.select(this.node).selectAll('svg').remove();
    waterfall(this.props.data, this.node, this.props);
    return false;
  }

  render() {
    return <div ref={(node) => { this.node = node; }} />;
  }
}
