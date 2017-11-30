import React from 'react';
import {Link} from 'react-router';

import {ConfStore} from '../store';

export default class JobConf extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    ConfStore.on('jobConf', (jobConf) => {
      if (jobConf.id === this.props.params.jobId) {
        this.setState({jobConf});
      }
    });
    ConfStore.getJobConf(this.props.params.jobId);
  }

  componentWillReceiveProps(next) {
    if (this.props.params.jobId !== next.params.jobId) {
      ConfStore.getJobConf(next.params.jobId);
    }
  }

  render() {
    const {jobConf} = this.state;
    if (jobConf === undefined) { return null; }
    return (
      <div>
        <h3>
          <Link to={`/job/${jobConf.id}`}>{jobConf.name}</Link>
        </h3>
        <table className="table">
          <thead>
            <tr>
              <th>Flag</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {Object.keys(jobConf.conf.flags || {}).map((key) => {
              return (
                <tr key={key}>
                  <th>{key}</th>
                  <th>{jobConf.conf.flags[key]}</th>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }
}
