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
    var jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) == jobId);
  }

  render() {
    var job = this.getJob();
    if (!job) return null;
    var logs = _.sortBy(_.pairs(job.tasks.errors), (x) => x[1].length).reverse().map((p) => {
      var attempts = p[1];
      var errorMessage = p[0].split('\n')[0];
      var errorBody = p[0].split('\n').slice(1).join('\n');
      return (
        <dl>
          <dt>{attempts.length} time{attempts.length == 1 ? '' : 's'}</dt>
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
