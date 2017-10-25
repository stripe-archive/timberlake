import React from 'react';
import {render} from 'react-dom';
import {Router, Route, IndexRoute, hashHistory} from 'react-router';

import BigData from './list';
import Job from './job';
import JobConf from './jobconf';
import JobLogs from './joblogs';
import JobCounters from './jobcounters';
import {NavBar} from './NavBar';
import {Store} from './store';

const {_} = window;

/**
 * Number of most recent jobs to keep in the finished tab.
 */
const JOBS_TO_KEEP = 5000;

/**
 * Component responsible for the application state.
 */
class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      jobs: {},
    };
  }

  componentDidMount() {
    Store.on('job', (job) => {
      this.updates[job.id] = job;
    });

    Store.on('jobs', (jobs) => {
      // We may have more specific info this.state.jobs already, so merge that
      // into what we're getting from /jobs.
      this.setState({jobs: _.extend(_.object(jobs.map((d) => [d.id, d])), this.state.jobs)});
    });

    Store.getJobs();
    Store.startSSE();

    // Updates are flushed once per second so that the clocks tick, and because we're not getting
    // updates much faster than that.
    this.updates = {};
    this.interval = setInterval(this.flushUpdates.bind(this), 1000);
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  flushUpdates() {
    // Merge the updates into the list of jobs. Updates are merged into the existing map
    // since they contain more specific & more recent information than the existing list.
    const jobs = _.extend(this.state.jobs, this.updates);

    // Drop mapper, reducer & config info of jobs that are not viewed on the detail page.
    for (const key in jobs) {
      if (key != Store.lastJob) {
        jobs[key].compact();
      }
    }

    // Drop old finished jobs, keeping only 5000 of them.
    const ids = Object.keys(jobs).filter((id) => jobs[id].finishTime != null && id != Store.lastJob);
    if (ids.length > JOBS_TO_KEEP) {
      ids.sort((a, b) => jobs[a].finishTime - jobs[b].finishTime);
      for (const id of _.first(ids, ids.length - JOBS_TO_KEEP)) {
        delete jobs[id];
      }
    }

    this.setState({jobs});
    this.updates = {};
  }

  render() {
    const jobs = _.values(this.state.jobs);
    return (
      <div>
        <NavBar jobs={jobs} />
        <div id="main" className="container">
          {this.props.children && React.cloneElement(this.props.children, {
            jobs,
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
      <Route name="job" path="job/:jobId" component={Job} />
      <Route name="log" path="job/:jobId/logs" component={JobLogs} />
      <Route name="cfg" path="job/:jobId/conf" component={JobConf} />
      <Route name="cnt" path="job/:jobId/counters" component={JobCounters} />
    </Route>
  </Router>
  , document.getElementById('timberlake'),
);

