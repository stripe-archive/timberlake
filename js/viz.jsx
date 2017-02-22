import React from 'react';
import ReactDOM from 'react-dom';
import {
  COLOUR_MAP,
  COLOUR_REDUCE
} from './utils';

export default class extends React.Component {
  render() {
    return <div className="viz"></div>;
  }

  componentDidMount() {
    var elem = ReactDOM.findDOMNode(this);
    this.width = elem.parentNode.offsetWidth - 100;
    this.height = 500 - 100;

    this.svg = d3.select(elem)
      .append("svg")
      .attr('width', this.width + 100)
      .attr('height', this.height + 100);

    this.plot(this.props.jobs);
  }

  componentDidUpdate() {
    this.svg.selectAll("g").remove();
    this.plot(this.props.jobs);
  }

  plot(jobs) {
    const now = new Date;
    const start = now - 60 * 60 * 1000;

    // Helper to count the number of runing tasks in each interval.
    var findIntervals = function(key) {
      // Fetch all the tasks' start & end dates.
      let tasks = [];
      for (let job of jobs) {
        for (let task of job.tasks[key]) {
          if (task.startTime >= start) {
            tasks.push({type: 'start', time: task.startTime});
            if (task.finishTime) {
              tasks.push({type: 'end', time: task.finishTime});
            }
          }
        }
      }

      // Sort the tasks by time. If time is the same, start precedes end.
      tasks.sort((a, b) => {
        if (a.time == b.time) {
          if (a.type == b.type) {
            return 0;
          } else if (a.type == 'start') {
            return -1;
          } else {
            return +1;
          }
        } else {
          return a.time - b.time
        }
      });
      if (tasks.length == 0) {
        return [];
      }

      // Split into intervals & count tasks in each interval.
      let intervals = [];
      let running = 0;
      for (let task of tasks) {
        if (task.type == 'start') {
          running += 1;
        } else {
          running -= 1;
        }

        if (task.time >= start) {
          intervals.push({
            time: task.time,
            count: running
          });
        }
      }
      return intervals;
    }

    let map = findIntervals('maps');
    let reduce = findIntervals('reduces');
    let intervals = map.concat(reduce);

    // Figure out the domains.
    var x = d3.time.scale()
      .rangeRound([0, this.width])
      .domain([start, now]);
    var y = d3.scale.linear()
      .rangeRound([this.height, 0])
      .domain([0, Math.max(1,d3.max(map, d => d.count))]);

    // Draw the lines.
    const line = d3.svg.line()
      .x(function(d) { return x(d.time); })
      .y(function(d) { return y(d.count); });
    this.g = this.svg
      .append("g")
      .attr("transform", "translate(50, 50)")
    this.g.append("path")
      .datum(map)
      .attr("fill", "none")
      .attr("stroke", COLOUR_MAP)
      .attr("stroke-width", 1.5)
      .attr("d", line);
    this.g.append("path")
      .datum(reduce)
      .attr("fill", "none")
      .attr("stroke", COLOUR_REDUCE)
      .attr("stroke-width", 1.5)
      .attr("d", line);

    // Draw the X axis.
    const xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");
    this.svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(50," + (this.height + 50) + ")")
      .call(xAxis);

    // Draw the Y axis.
    const yAxis = d3.svg.axis()
      .scale(y)
      .orient("left");
    this.svg.append("g")
      .attr("class", "y axis")
      .attr("transform", "translate(50,50)")
      .call(yAxis);
  }
}
