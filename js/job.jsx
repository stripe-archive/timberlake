import React from 'react';
import {Link} from 'react-router';
import ReactTooltip from 'react-tooltip';

import KillModal from './components/KillModal';
import MapSummary from './components/MapSummary';
import ReduceSummary from './components/ReduceSummary';
import RelatedDAG from './components/related-dag';
import RelatedJobs from './components/RelatedJobs';
import TaskStats from './components/TaskStats';
import TaskWaterfall from './components/TaskWaterfall';
import {ConfStore, Store} from './store';
import {
  ACTIVE_STATES,
  cleanJobPath,
  FAILED_STATES,
  jobState,
  lolhadoop,
  secondFormat,
  timeFormat,
} from './utils';
import {notAvailable} from './mr';

const {$, _, d3} = window;

function bytesFormat(n) {
  if (n === notAvailable || !n) return null;
  const M = 1024.0 * 1024;
  const G = M * 1024;
  if (n < G) {
    return `${d3.format(',.1f')(n / M)}M`;
  } else {
    return `${d3.format(',.1f')(n / G)}G`;
  }
}

function inputs(job, allJobs, conf, jobConfs) {
  const relatives = relatedJobs(job, allJobs);
  const outputs = _.object(_.flatten(relatives.map((j) => {
    const jcc = jobConfs[j.id];
    const output = jcc ? jcc.conf.output : '';
    return (output || '').split(/,/g).map((o) => [o, j]);
  }), 1));
  return (conf.input || '').split(/,/g).map((input) => outputs[input] || input);
}

function relatedJobs(job, allJobs) {
  if (job) {
    return allJobs.filter((d) => d.fullName.indexOf(job.taskFamily) === 1);
  }
  return [];
}

export default class Job extends React.Component {
  constructor(props) {
    super(props);
    console.log('Initializing Job component');
    this.state = {jobConfs: {}};

    this.kill = this.kill.bind(this);
    this.handleShowKillModal = this.handleShowKillModal.bind(this);
    this.handleHideKillModal = this.handleHideKillModal.bind(this);
  }

  componentDidMount() {
    console.log('componentDidMount');
    const {jobId} = this.props.params;
    Store.getJob(jobId);

    ConfStore.on('jobConf', (jobConf) => (
      this.setState({
        jobConfs: _.extend(
          this.state.jobConfs,
          {[jobConf.id]: jobConf},
        ),
      })
    ));
  }

  componentWillReceiveProps(next) {
    console.log('componentWillReceiveProps', this.props, this.next);
    if (this.props.params.jobId !== next.params.jobId) {
      Store.getJob(next.params.jobId);
      ConfStore.getJobConf(next.params.jobId);
    }
    relatedJobs(this.getJob(), next.jobs).forEach((job) => {
      if (this.state.jobConfs[job.id] === undefined) {
        ConfStore.getJobConf(job.id);
      }
    });
  }

  getJob() {
    const jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) === jobId);
  }

  kill() {
    this.handleHideKillModal();
    this.setState({killing: true});
    const job = this.getJob();
    $.post(`/jobs/${job.id}/kill`, (data, status) => {
      console.log(data, status);
      const result = data.err ? data.stderr : null;
      this.setState({killResult: result});
    }).then(null, (err) => {
      console.error(err);
      this.setState({killing: false});
    });
  }

  handleHideKillModal() {
    this.setState({showKillModal: false});
  }

  handleShowKillModal() {
    this.setState({showKillModal: true});
  }

  render() {
    console.log('Beginning render()');
    const job = this.getJob();
    if (!job) return null;
    const jobConfCounter = this.state.jobConfs[this.props.params.jobId];
    if (jobConfCounter === undefined) { return null; }
    const {conf} = this.state.jobConfs[this.props.params.jobId];
    document.title = job.name;
    /* eslint-disable react/no-array-index-key */
    const renderedInputs = (
      <ul className="list-unstyled">
        {inputs(job, this.props.jobs, conf, this.state.jobConfs).map((input, i) =>
          (
            <li key={i}>
              {_.isString(input) ?
                cleanJobPath(input) :
                <Link to={`job/${input.id}`}>{input.name}</Link>}
            </li>
          ))}
      </ul>
    );
    /* eslint-enable react/no-array-index-key */

    let similar = this.props.jobs.filter((j) => j.name.indexOf(job.name) !== -1);
    similar = similar.filter((j) => j.startTime < job.startTime);
    const prev = _.last(_.sortBy(similar, 'startTime'));
    const previous = prev ? <Link to={`job/${prev.id}`}>previous: {secondFormat(prev.duration())}</Link> : null;

    let state = jobState(job);
    if (_.contains(ACTIVE_STATES, job.state)) {
      const {killing} = this.state;
      state = (
        <span>
          {state}
          <button onClick={this.handleShowKillModal} className="btn btn-danger kill">
            <span className="label label-danger">{killing ? 'Killing' : 'Kill'}</span>
          </button>
          {this.state.showKillModal ? <KillModal onHideModal={this.handleHideKillModal} killJob={this.kill} /> : null}
          {this.state.killResult ? <code>{this.state.killResult}</code> : null}
        </span>
      );
    } else if (_.contains(FAILED_STATES, job.state)) {
      const link = <Link to={`job/${job.id}/logs`} className="logs-link">view logs</Link>;
      state = <div>{state} {link}</div>;
    }

    const pairs = [
      ['User', job.user],
      ['Name', job.name],
      ['ID', job.id],
      ['Start', timeFormat(job.startTime)],
      ['Duration', previous ? <span>{secondFormat(job.duration())} ({previous})</span> : secondFormat(job.duration())],
      ['State', state],
      ['Input', renderedInputs],
      ['Output', cleanJobPath(conf.output)],
    ];

    const stepsStr = conf.scaldingSteps;
    if (stepsStr) {
      const lines = stepsStr.split(',').map((val) => {
        const trimmed = val.trim();
        const matches = trimmed.match(/[\w.]+:\d+/i);
        return {full: trimmed, short: matches ? matches[0] : trimmed};
      });
      const steps = (
        <ul className="list-unstyled">
          {_.uniq(lines).map((line, i) => (
            <li key={line.short}>
              <span data-tip data-for={line.short} title={line.full}>
                {line.short}
              </span>
              <ReactTooltip effect="solid" id={line.short} >
                <span>{line.full}</span>
              </ReactTooltip>
            </li>
          ))}
        </ul>
      );
      pairs.push(['Line Numbers', steps]);
    }

    const bytes = {
      hdfs_read: job.counters.get('FileSystemCounter.HDFS_BYTES_READ').map,
      s3_read: job.counters.get('FileSystemCounter.S3_BYTES_READ').map || 0,
      file_read: job.counters.get('FileSystemCounter.FILE_BYTES_READ').map || 0,
      hdfs_written: job.counters.get('FileSystemCounter.HDFS_BYTES_WRITTEN').total || 0,
      s3_written: job.counters.get('FileSystemCounter.S3_BYTES_WRITTEN').total || 0,
      file_written: job.counters.get('FileSystemCounter.FILE_BYTES_WRITTEN').total || 0,
      shuffled: job.counters.get('TaskCounter.REDUCE_SHUFFLE_BYTES').reduce || 0,
    };
    bytes.total_read = bytes.hdfs_read + bytes.s3_read + bytes.file_read;
    bytes.total_written = bytes.hdfs_written + bytes.s3_written + bytes.file_written;
    Object.keys(bytes).forEach((key) => {
      bytes[key] = bytesFormat(bytes[key]);
    });
    const bytesReadTitle = `HDFS: ${bytes.hdfs_read}\nS3: ${bytes.s3_read}\nFile: ${bytes.file_read}`;
    const bytesWrittenTitle = `HDFS: ${bytes.hdfs_written}\nS3: ${bytes.s3_written}\nFile: ${bytes.file_written}`;

    const sortedRelatedJobs = _.sortBy(relatedJobs(job, this.props.jobs), (relatedJob) => relatedJob.id);
    console.log('Actually rendering');

    return (
      <div>
        <div className="row">
          <div className="col-md-5">
            <div>
              <h4>Job Details</h4>
            </div>
            <table className="table job-details">
              <tbody>
                {pairs.map((d, i) => <tr key={d[0]}><th>{d[0]}</th><td>{d[1]}</td></tr>)}
                <tr>
                  <th>Details</th>
                  <td>
                    <Link to={`job/${job.id}/conf`}>Configuration</Link>
                    <br />
                    <Link to={`job/${job.id}/counters`}>Counters</Link>
                  </td>
                </tr>
                <tr>
                  <th>Bytes</th>
                  <td>
                    <dl className="bytes">
                      <dt title={bytesReadTitle}>Read</dt> <dd>{bytes.total_read}</dd>
                      <dt title={bytesWrittenTitle}>Write</dt> <dd>{bytes.total_written}</dd>
                      <dt>Shuffle</dt> <dd>{bytes.shuffled}</dd>
                    </dl>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div className="col-md-3 col-md-offset-1">
            <h4>Map Jobs</h4>
            <MapSummary job={job} counters={job.counters} />
          </div>
          <div className="col-md-3">
            <h4>Reduce Jobs</h4>
            <ReduceSummary job={job} counters={job.counters} />
          </div>
        </div>
        <div className="row" style={{minHeight: 450}}>
          <div className="col-md-5">
            <TaskWaterfall tasks={job.tasks} />
          </div>
          <div className="col-md-3 col-md-offset-1">
            <TaskStats title="Map" tasks={job.tasks.maps} />
          </div>
          <div className="col-md-3">
            <TaskStats title="Reduce" tasks={job.tasks.reduces} />
          </div>
        </div>
        <div className="row">
          <RelatedJobs job={job} relatives={sortedRelatedJobs} hover={this.state.hover} />
        </div>
        <div className="row">
          <RelatedDAG
            job={job}
            jobConfs={this.state.jobConfs}
            relatives={sortedRelatedJobs}
            hover={(j) => { this.setState({hover: j}); }}
          />
        </div>
      </div>
    );
  }
}
