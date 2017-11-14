import React from 'react';
import {Link} from 'react-router';

import {Store} from './store';
import {lolhadoop} from './utils';

const {_} = window;

export default class JobLogs extends React.Component {
  componentDidMount() {
    Store.getJob(this.props.params.jobId);
  }

  getJob() {
    const jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) === jobId);
  }

  render() {
    const job = this.getJob();
    if (!job) return null;
    const logs = _.sortBy(_.pairs(job.tasks.errors), (x) => x[1].length).reverse().map((p) => {
      const attempts = p[1];
      const errorMessage = p[0].split('\n')[0];
      const errorBody = p[0].split('\n').slice(1).join('\n');
      return (
        <dl key={attempts[0].id}>
          <dt>{attempts.length} time{attempts.length === 1 ? '' : 's'}</dt>
          <pre>
            <b>{errorMessage}</b><br />
            {errorBody}
          </pre>
        </dl>
      );
    });
    return (
      <div>
        <h3><Link to={`/job/${job.id}`}>{job.name}</Link></h3>
        <br />
        {logs}
      </div>
    );
  }
}
