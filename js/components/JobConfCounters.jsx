import PropTypes from 'prop-types';
import React from 'react';
import {Link} from 'react-router';

import {ConfCountersStore} from '../store';

class JobConfCounters extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    ConfCountersStore.on('jobConfCounters', (jobConfCounters) => {
      if (jobConfCounters.id === this.props.params.jobId) {
        this.setState({jobConfCounters}); // eslint-disable-line react/no-unused-state
      }
    });
    ConfCountersStore.getJobConfCounters(this.props.params.jobId);
  }

  componentWillReceiveProps(next) {
    if (this.props.params.jobId !== next.params.jobId) {
      ConfCountersStore.getJobConfCounters(next.params.jobId);
    }
  }
}

export class JobConf extends JobConfCounters {
  render() {
    const {jobConfCounters} = this.state;
    if (jobConfCounters === undefined) { return null; }
    return (
      <div>
        <h3>
          <Link to={`/job/${jobConfCounters.id}`}>{jobConfCounters.name}</Link>
        </h3>
        <table className="table">
          <thead>
            <tr>
              <th>Flag</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {Object.keys(jobConfCounters.conf.flags || {}).map((key) => {
              return (
                <tr key={key}>
                  <th>{key}</th>
                  <th>{jobConfCounters.conf.flags[key]}</th>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }
}

export class JobCounters extends JobConfCounters {
  render() {
    const {jobConfCounters} = this.state;
    if (jobConfCounters === undefined) { return null; }
    return (
      <div>
        <h3>
          <Link to={`/job/${jobConfCounters.id}`}>{jobConfCounters.name}</Link>
        </h3>
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
            {Object.keys(jobConfCounters.counters.data).sort().map((key) => {
              const counter = jobConfCounters.counters.get(key);
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

JobConfCounters.propTypes = {
  params: PropTypes.shape({jobId: PropTypes.string.isRequired}).isRequired,
};
