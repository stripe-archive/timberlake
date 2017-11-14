import React from 'react';
import {hashHistory} from 'react-router';
import {FinishedJobRow, RunningJobRow} from './jobrow';
import {
  ACTIVE_STATES,
  FINISHED_STATES,
  HEADERS,
} from './utils';

const {_} = window;

class JobTable extends React.Component {
  static sorting(sort) {
    const s = sort.split('-');
    return s.length === 1 ? {key: s[0], dir: 1} : {key: s[1], dir: -1};
  }

  sort(key) {
    const s = JobTable.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    const n = s.key === key && s.dir === -1 ? key : `-${key}`;
    const q = _.object([[this.sortKey, n]]);

    hashHistory.push({
      pathname: '/',
      query: _.extend(this.props.query, q),
    });
  }

  sortedJobs() {
    let jobs = this.props.jobs.filter((j) => _.contains(this.states, j.state));
    if (this.props.filter) {
      const parts = this.props.filter.split(/\s+/);
      jobs = jobs.filter((job) => {
        return parts.every((p) => job.searchString.indexOf(p) !== -1);
      });
    }
    const sort = JobTable.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    jobs = _.sortBy(jobs, (row) => {
      switch (sort.key) {
        case 'user': return row.user;
        case 'name': return row.name;
        case 'started': return row.startTime;
        case 'finished': return row.finishTime;
        case 'duration': return row.duration();
        case 'map': return row.maps.progress;
        case 'reduce': return row.reduces.progress;
        case 'state': return row.state;
        case 'cluster': return row.cluster;
        default: return undefined;
      }
    });
    if (sort.dir === -1) jobs.reverse();
    return [sort, jobs];
  }

  render() {
    const [sort, jobs] = this.sortedJobs();
    const sortDir = `sort-${sort.dir > 0 ? 'asc' : 'desc'}`;
    const Row = this.rowClass();
    const rows = jobs.slice(0, 150).map((job) => <Row key={job.id} job={job} />);
    return (
      <div>
        <h3>
          {this.title} ({jobs.length})
          <input
            autoFocus={this.autoFocus}
            className="form-control"
            onChange={this.props.onFilter}
            placeholder="Filter by user or text"
            value={this.props.filter}
          />
        </h3>
        <table className="table sortable list-view">
          <thead>
            <tr>
              {HEADERS.map((h) => {
                const cls = sort.key === h ? sortDir : '';
                const click = this.sort.bind(this, h); // eslint-disable-line react/jsx-no-bind
                return <th key={h} className={cls} onClick={click}>{h}</th>;
              })}
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    );
  }
}

export class FinishedJobs extends JobTable {
  constructor(props) {
    super(props);

    this.sortKey = 'fsort';
    this.states = FINISHED_STATES;
    this.defaultSortKey = '-finished';
    this.title = 'Finished';
    this.rowClass = () => FinishedJobRow;
  }
}

export class RunningJobs extends JobTable {
  constructor(props) {
    super(props);

    this.sortKey = 'rsort';
    this.states = ACTIVE_STATES;
    this.defaultSortKey = '-started';
    this.title = 'Running';
    this.rowClass = () => RunningJobRow;
    this.autoFocus = true;
  }
}
