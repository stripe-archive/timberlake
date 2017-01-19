import React from 'react';
import { Link, hashHistory } from 'react-router';

import {
  timeFormat,
  secondFormat,
  percentFormat,
  jobState,
  ACTIVE_STATES,
  FINISHED_STATES
} from './utils';


// Lifted from react-router.
var isLeftClickEvent = e => e.button === 0;
var isModifiedEvent = e => !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);


export default class extends React.Component {
  constructor(props) {
    super(props);

    this.onFilter = this.onFilter.bind(this);

    this.state = {
      filter: "",
      flushed: true
    };
  }

  componentDidMount() {
    document.title = "Timberlake :: The 20/20 Experience";

    // Two reasons for debouncing:
    // 1. Transitioning on every keypress was leading to dropped keys.
    // 2. Debouncing prevents every single keypress from becoming a history item.
    this.updateLocation = _.debounce(() => {
      var q = {filter: this.state.filter};
      hashHistory.push({
        pathname: '/',
        query: _.extend(this.props.location.query, q)
      });
      this.state.flushed = true;
    }, 250, false);
  }

  componentWillReceiveProps(nextProps) {
    // A change to the filter when we're flushed indicates a legit transition, probably via browser
    // back button. If we're not flushed then it's just the location bar catching up to our state.
    if (this.state.flushed && this.state.filter != nextProps.location.query.filter) {
      this.setState({filter: nextProps.location.query.filter || ""});
    }
  }

  onFilter(e) {
    this.updateLocation();
    this.setState({filter: e.target.value, flushed: false});
  }

  render() {
    console.time('BigData');
    var jobs = this.props.jobs;
    var filter = this.state.filter.toLowerCase();
    var rv = (
      <div>
        <RunningJobs jobs={jobs} query={this.props.location.query} onFilter={this.onFilter} filter={filter} />
        <FinishedJobs jobs={jobs} query={this.props.location.query} onFilter={this.onFilter} filter={filter} />
      </div>
    );
    console.timeEnd('BigData');
    return rv;
  }
}

class JobTable extends React.Component {
  sorting(sort) {
    var s = sort.split('-');
    return s.length == 1 ? {key: s[0], dir: 1} : {key: s[1], dir: -1};
  }

  sort(key) {
    var s = this.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    var n = s.key == key && s.dir == -1 ? key : '-' + key;
    var q = _.object([[this.sortKey, n]]);

    hashHistory.push({
      pathname: '/',
      query: _.extend(this.props.query, q)
    });
  }

  sortedJobs() {
    var jobs = this.props.jobs.filter(j => _.contains(this.states, j.state));
    if (this.props.filter) {
      var parts = this.props.filter.split(/\s+/);
      jobs = jobs.filter(job => {
        return parts.every(p => job.searchString.indexOf(p) != -1);
      });
    }
    var sort = this.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    jobs = _.sortBy(jobs, row => {
      switch (sort.key) {
        case 'user': return row.user;
        case 'name': return row.name;
        case 'started': return row.startTime;
        case 'finished': return row.finishTime;
        case 'duration': return row.duration();
        case 'map': return row.maps.progress;
        case 'reduce': return row.reduces.progress;
        case 'state': return row.state;
      }
    });
    if (sort.dir == -1) jobs.reverse();
    return [sort, jobs];
  }

  render() {
    var [sort, jobs] = this.sortedJobs();
    var sortDir = 'sort-' + (sort.dir > 0 ? 'asc' : 'desc');
    var Row = this.rowClass();
    var rows = jobs.slice(0, 150).map(job => <Row key={job.id} job={job} />);
    return (
      <div>
        <h3>
          {this.title} ({jobs.length})
          <input className="form-control" placeholder="Filter by user or text" onChange={this.props.onFilter} value={this.props.filter} autoFocus={this.autoFocus} />
        </h3>
        <table className="table sortable list-view">
          <thead>
            <tr>
              {this.headers.map(h => {
                var cls = sort.key == h ? sortDir : "";
                var click = this.sort.bind(this, h);
                return <th key={h} className={cls} onClick={click}>{h}</th>;
              })}
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    );
  }
};


class FinishedJobs extends JobTable {
  constructor(props) {
    super(props);

    this.sortKey = 'fsort';
    this.filterKey = 'filter';
    this.states = FINISHED_STATES;
    this.defaultSortKey = '-finished';
    this.title = 'Finished';
    this.headers = 'user name started finished duration state'.split(' ');
    this.rowClass = () => FinishedJobRow;
  }
}

class RunningJobs extends JobTable {
  constructor(props) {
    super(props);

    this.sortKey = 'rsort';
    this.filterKey = 'filter';
    this.states = ACTIVE_STATES;
    this.defaultSortKey = '-started';
    this.title = 'Running';
    this.headers = 'user name started duration map reduce'.split(' ');
    this.rowClass = () => RunningJobRow;
    this.autoFocus = true;
  }
}

class JobRow extends React.Component {
  constructor() {
    super();
    this.onClick = this.onClick.bind(this);
  }

  onClick(e) {
    if (isLeftClickEvent(e) && !isModifiedEvent(e)) {
      hashHistory.push(`/job/${this.props.job.id}`);
    }
  }

  render() {
    var columns = this.columns();
    return <tr onClick={this.onClick.bind(this)}>{columns.map((d, i) => <td key={i}>{d}</td>)}</tr>;
  }
}

class RunningJobRow extends JobRow {
  columns() {
    var job = this.props.job;
    return [
      job.user,
      <Link to={`/job/${job.id}`}>{job.name}</Link>,
      timeFormat(job.startTime),
      secondFormat(job.duration()),
      percentFormat(job.maps.progress),
      percentFormat(job.reduces.progress),
    ];
  }
}


class FinishedJobRow extends JobRow {
  columns() {
    var job = this.props.job;
    return [
      job.user,
      <Link to={`/job/${job.id}`}>{job.name}</Link>,
      timeFormat(job.startTime),
      timeFormat(job.finishTime),
      secondFormat(job.duration()),
      jobState(job),
    ];
  }
}
