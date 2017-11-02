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
    if (this.props.params.jobId != next.params.jobId) { // eslint-disable-line eqeqeq
      Store.getJob(next.params.jobId);
    }
  }

  getJob() {
    const jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) == jobId); // eslint-disable-line eqeqeq
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
              <th>Flag</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {Object.keys(job.conf.flags || {}).map(function(key) {
              return (
                <tr key={key}>
                  <th>{key}</th>
                  <th>{job.conf.flags[key]}</th>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }
}
