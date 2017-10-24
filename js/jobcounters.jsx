import React from 'react';
import {Link} from 'react-router';

import {Store} from './store';
import {lolhadoop} from './utils';

const {_} = window;

export default class extends React.Component {
  componentDidMount() {
    Store.getJob(this.props.params.jobId);
  }

  componentWillReceiveProps(next) {
    if (this.props.params.jobId != next.params.jobId) {
      Store.getJob(next.params.jobId);
    }
  }

  getJob() {
    const jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) == jobId);
  }

  render() {
    const job = this.getJob();
    if (!job) {
      return null;
    }
    return (
      <div>
        <h3><Link to={`/job/${job.id}`}>{job.name}</Link></h3>
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Total</th>
              <th>Map</th>
              <th>Reduce</th>
            </tr>
          </thead>
          <tbody>
            {Object.keys(job.counters.data).sort().map(function(key) {
              const counter = job.counters.get(key);
              return (
                <tr key={key}>
                  <th>{counter.name}</th>
                  <td>{counter.total || 0}</td>
                  <td>{counter.map || 0}</td>
                  <td>{counter.reduce || 0}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }
}
