import React from 'react';
import { render } from 'react-dom'
import { Router, Route, IndexRoute, hashHistory } from 'react-router';

import BigData from './list';
import Job from './job';
import { JobLogs } from './joblogs';
import { Store } from './store';
import { numFormat } from './utils';

/**
 * Number of most recent jobs to keep in the finished tab.
 */
const JOBS_TO_KEEP = 5000;


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
            <p className="navbar-text">mappers: {numFormat(mappers)}</p>
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
      // We may have more specific info this.state.jobs already, so merge that
      // into what we're getting from /jobs.
      this.updates = _.extend(this.updates, _.object(jobs.map(d => [d.id, d])));
      this.flushUpdates();
    });

    Store.getJobs();
    Store.startSSE();

    // Updates are flushed once per second so that the clocks tick, and because we're not getting
    // updates much faster than that.
    this.updates = {};
    this.interval = setInterval(this.flushUpdates.bind(this), 1000);
  }

  flushUpdates() {
    let jobs = _.extend(this.state.jobs, this.updates);

    // Drop mapper & reducer info of jobs that are not viewed on the detail page.
    for (let key in jobs) {
      if (key != Store.lastJob) {
        let job = jobs[key];
        job.tasks.maps = [];
        job.tasks.reduces = [];
      }
    }

    // Drop old finished jobs, keeping only 5000 of them.
    let ids = Object.keys(jobs).filter(id => jobs[id].finishTime != null);
    if (ids.length > JOBS_TO_KEEP) {
      ids.sort((a, b) => jobs[a].finishTime - jobs[b].finishTime);
      for (let id of _.first(ids, ids.length - JOBS_TO_KEEP)) {
        delete jobs[id];
      }
    }

    this.setState({jobs: jobs});
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

