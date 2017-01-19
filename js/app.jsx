import React from 'react';
import { render } from 'react-dom'
import { Router, Route, IndexRoute, hashHistory } from 'react-router';

import BigData from './list';
import Job from './job';
import { JobLogs } from './joblogs';
import { Store } from './store';
import { numFormat } from './utils';


/**
 * Component responsible for rendering the top navigation bar.
 */
class NavBar extends React.Component {
  render() {
    var running = this.props.jobs.filter(j => j.state == 'RUNNING');
    var mappers = running.map(j => j.maps.running).reduce((x, y) => x + y, 0);
    var reducers = running.map(j => j.reduces.running).reduce((x, y) => x + y, 0);

    return (
      <nav className="navbar navbar-default">
        <div className="container">
          <div className="navbar-header">
            <a className="navbar-brand" href="#">Timberlake</a>
          </div>
          <div className="navbar-right">
            <p className="navbar-text">mappers: {numFormat(reducers)}</p>
            <p className="navbar-text">reducers: {numFormat(reducers)}</p>
          </div>
        </div>
      </nav>
    );
  }
}


/**
 * Component responsible for the application state.
 */
class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      jobs: {}
    };
  }

  componentDidMount() {
    Store.on('job', job => {
      this.updates[job.id] = job;
    });

    Store.on('jobs', jobs => {
      var jobs = _.object(jobs.map(d => [d.id, d]));
      // We may have more specific info this.state.jobs already, so merge that
      // into what we're getting from /jobs.
      this.setState({jobs: _.extend(jobs, this.state.jobs)});
    });

    Store.getJobs();
    Store.startSSE();

    // Updates are flushed once per second so that the clocks tick, and because we're not getting
    // updates much faster than that.
    this.updates = {};
    this.interval = setInterval(this.flushUpdates.bind(this), 1000);
  }

  flushUpdates() {
    this.setState({jobs: _.extend(this.state.jobs, this.updates)});
    this.updates = {};
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  render() {
    var jobs = _.values(this.state.jobs);
    return (
      <div>
        <NavBar jobs={jobs} />
        <div id="main" className="container">
          {this.props.children && React.cloneElement(this.props.children, {
              jobs: jobs
          })}
        </div>
      </div>
    );
  }
}


render(
  <Router history={hashHistory}>
    <Route path="/" component={App}>
      <IndexRoute component={BigData} />
      <Route name="job" path="job/:jobId"      component={Job} />
      <Route name="log" path="job/:jobId/logs" component={JobLogs} />
    </Route>
  </Router>
, document.getElementById("timberlake"));

