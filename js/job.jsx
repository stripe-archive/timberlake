import React from 'react';
import ReactDOM from 'react-dom';
import {Link} from 'react-router';

import ProgressBar from './components/progress-bar';
import RelatedDAG from './components/related-dag';
import {Store} from './store';
import {
  lolhadoop,
  jobState,
  ACTIVE_STATES,
  FAILED_STATES,
  timeFormat,
  secondFormat,
  cleanJobPath,
  humanFormat,
  numFormat,
  plural,
  COLOUR_MAP,
  COLOUR_REDUCE,
  COLOUR_SELECTED,
  COLOUR_HOVER,
} from './utils';
import {notAvailable} from './mr';

const {$, _, d3} = window;

function bytesFormat(n) {
  if (n == notAvailable || !n) return null;
  const M = 1024.0 * 1024;
  const G = M * 1024;
  if (n < G) {
    return `${d3.format(',.1f')(n / M)}M`;
  } else {
    return `${d3.format(',.1f')(n / G)}G`;
  }
}

function relatedJobs(job, allJobs) {
  return allJobs.filter((d) => d.fullName.indexOf(job.taskFamily) == 1);
}

export default class extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
    };

    this.kill = this.kill.bind(this);
    this.showKillModal = this.showKillModal.bind(this);
    this.hideKillModal = this.hideKillModal.bind(this);
  }

  componentDidMount() {
    Store.getJob(this.props.params.jobId);
    $('.scalding-step-description').each(function() { $(this).tooltip(); });
  }

  componentWillReceiveProps(next) {
    if (this.props.params.jobId != next.params.jobId) {
      Store.getJob(next.params.jobId);
    }
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  getJob() {
    const jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, (d) => lolhadoop(d.id) == jobId);
  }

  inputs(job, allJobs) {
    const relatives = relatedJobs(job, allJobs);
    const outputs = _.object(_.flatten(relatives.map((j) => (j.conf.output || '').split(/,/g).map((o) => [o, j])), 1));
    return (job.conf.input || '').split(/,/g).map((input) => outputs[input] || input);
  }

  kill() {
    this.hideKillModal();
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

  hideKillModal() {
    this.setState({showKillModal: false});
  }

  showKillModal() {
    this.setState({showKillModal: true});
  }

  render() {
    console.time('Render Job');
    const job = this.getJob();
    if (!job) return null;
    document.title = job.name;
    const inputs = (
      <ul className="list-unstyled">
        {this.inputs(job, this.props.jobs).map((input, i) =>
          (
            <li key={i}>
              {_.isString(input) ?
                cleanJobPath(input) :
                <Link to={`job/${input.id}`}>{input.name}</Link>}
            </li>
          ))}
      </ul>
    );

    let similar = this.props.jobs.filter((j) => j.name.indexOf(job.name) != -1);
    similar = similar.filter((j) => j.startTime < job.startTime);
    const prev = _.last(_.sortBy(similar, 'startTime'));
    const previous = prev ? <Link to={`job/${prev.id}`}>previous: {secondFormat(prev.duration())}</Link> : null;

    let state = jobState(job);
    if (_.contains(ACTIVE_STATES, job.state)) {
      const {killing} = this.state;
      state = (
        <span>
          {state}
          <button onClick={this.showKillModal} className="btn btn-danger kill">
            <span className="label label-danger">{killing ? 'Killing' : 'Kill'}</span>
          </button>
          {this.state.showKillModal ? <KillModal hideModal={this.hideKillModal} killJob={this.kill} /> : null}
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
      ['Input', inputs],
      ['Output', cleanJobPath(job.conf.output)],
    ];

    const stepsStr = job.conf.scaldingSteps;
    if (stepsStr) {
      const lines = stepsStr.split(',').map((val) => {
        const trimmed = val.trim();
        const matches = trimmed.match(/[\w.]+:\d+/i);
        return {full: trimmed, short: matches ? matches[0] : trimmed};
      });
      const steps = (
        <ul className="list-unstyled">
          {_.uniq(lines).map((line, i) => <li key={i}><span className="scalding-step-description" title={line.full}>{line.short}</span></li>)}
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
    for (const key of Object.keys(bytes)) {
      bytes[key] = bytesFormat(bytes[key]);
    }
    const bytesReadTitle = `HDFS: ${bytes.hdfs_read}\nS3: ${bytes.s3_read}\nFile: ${bytes.file_read}`;
    const bytesWrittenTitle = `HDFS: ${bytes.hdfs_written}\nS3: ${bytes.s3_written}\nFile: ${bytes.file_written}`;

    const sortedRelatedJobs = _.sortBy(relatedJobs(job, this.props.jobs), (relatedJob) => relatedJob.id);

    const rv = (
      <div>
        <div className="row">
          <div className="col-md-5">
            <div>
              <h4>Job Details</h4>
            </div>
            <table className="table job-details">
              <tbody>
                {pairs.map((d, i) => <tr key={i}><th>{d[0]}</th><td>{d[1]}</td></tr>)}
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
          <RelatedDAG job={job} relatives={sortedRelatedJobs} hover={(j) => { this.setState({hover: j}); }} />
        </div>
      </div>
    );
    console.timeEnd('Render Job');
    return rv;
  }
}

class KillModal extends React.Component {
  render() {
    const {hideModal, killJob} = this.props;
    return (
      <div className="modal show" tabIndex="-1" role="dialog">
        <div className="modal-dialog modal-kill">
          <div className="modal-content">
            <div className="modal-body">
              <h4>Are you sure you want to kill this job?</h4>
            </div>
            <div className="modal-footer">
              <button onClick={hideModal} className="btn btn-default">Close</button>
              <button onClick={killJob} className="btn btn-danger">Kill</button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

class RelatedJobs extends React.Component {
  render() {
    const {hover, job, relatives} = this.props;

    if (relatives.length < 2) return null;

    let data = relatives.map((j) => ({
      start: j.startTime,
      finish: j.finishTime || new Date(),
      job: j,
    }));
    data = _.sortBy(data, 'start');

    const fmt = (d) => `${secondFormat(d.job.duration())} ${d.job.name}`;
    const links = (d) => `#/job/${d.job.id}`;
    const fs = (d) => {
      if (d.job.id == job.id) {
        return COLOUR_SELECTED;
      } else if (hover && d.job.id == hover.id) {
        return COLOUR_HOVER;
      } else {
        return COLOUR_MAP;
      }
    };

    return (
      <div>
        <h4>Related Jobs</h4>
        <Waterfall
          data={data}
          barHeight={30}
          lineHeight={40}
          width={1200}
          textFormat={fmt}
          fillStyle={fs}
          linkFormat={links}
        />
      </div>
    );
  }
}


class TaskStats extends React.Component {
  render() {
    const tasks = this.props.tasks.filter((t) => !t.bogus);
    const durations = sample(tasks.map((x) => x.duration()).sort(), 100, _.identity);
    return (
      <div>
        <h4>{this.props.title} Timing</h4>
        {durations.length ? <BoxPlot data={durations} tickFormat={secondFormat} /> : null}
      </div>
    );
  }
}

function sample(arr, limit, comparator) {
  // Sample arr down to size limit using comparator to take the largest value at
  // each step. Works best if arr is already sorted.
  if (arr.length <= limit) return arr;
  const rv = [];
  const sampleSize = arr.length / limit;
  for (let i = 0; i < arr.length / sampleSize; i += 1) {
    const vals = arr.slice(i * sampleSize, (i + 1) * sampleSize);
    if (vals.length !== 0) {
      rv.push(_.max(vals, comparator));
    }
  }
  return rv;
}


class TaskWaterfall extends React.Component {
  render() {
    let {tasks} = this.props;
    tasks.maps.forEach((d) => d.type = 'map');
    tasks = tasks.maps.concat(tasks.reduces).filter((t) => !t.bogus).map((t) => {
      return {start: t.startTime, finish: t.finishTime || new Date(), type: t.type};
    });
    const data = sample(_.sortBy(tasks, 'start'), 400, (d) => d.finish - d.start);
    return (
      <div>
        <h4>Map <i className="map-label" /> / <i className="reduce-label" /> Reduce Waterfall</h4>
        {data.length ? <Waterfall data={data} /> : null}
      </div>
    );
  }
}


class Waterfall extends React.Component {
  componentDidMount() {
    waterfall(this.props.data, ReactDOM.findDOMNode(this), this.props);
  }

  shouldComponentUpdate() {
    d3.select(ReactDOM.findDOMNode(this)).selectAll('svg').remove();
    waterfall(this.props.data, ReactDOM.findDOMNode(this), this.props);
    return false;
  }

  render() {
    return <div />;
  }
}

function waterfall(data, node, optsIn) {
  const defaults = {
    lineHeight: 1,
    barHeight: 1,
    width: 550,
    textFormat: (t) => '',
    linkFormat: null,
    fillStyle: (d) => {
      return d.type == 'map' ? COLOUR_MAP : COLOUR_REDUCE;
    },
  };
  const opts = _.extend(defaults, optsIn);

  const margin = {
    top: 10, right: 20, bottom: 20, left: 20,
  };
  const width = opts.width - margin.left - margin.right;
  const height = Math.max(100, data.length * opts.lineHeight) - margin.top - margin.bottom;

  const chart = d3.waterfall()
    .width(width)
    .height(height)
    .barHeight(opts.barHeight)
    .textFormat(opts.textFormat)
    .linkFormat(opts.linkFormat)
    .barStyle(opts.fillStyle);

  const start = d3.min(_.pluck(data, 'start'));
  const finish = d3.max(_.pluck(data, 'finish'));
  chart.domain([start, finish]);

  if (((finish - start) / 1000) < 180) {
    // Show seconds if the x domain is less than three minutes.
    chart.tickFormat(d3.time.format.utc('%H:%M:%S'));
  }

  d3.select(node).selectAll('svg')
    .data([data])
    .enter().append('svg')
    .attr('class', 'waterfall')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.bottom + margin.top)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)
    .call(chart);
}


class BoxPlot extends React.Component {
  componentDidMount() {
    boxplot(this.props.data, ReactDOM.findDOMNode(this), this.props.tickFormat);
  }

  shouldComponentUpdate() {
    d3.select(ReactDOM.findDOMNode(this)).selectAll('svg').remove();
    boxplot(this.props.data, ReactDOM.findDOMNode(this), this.props.tickFormat);
    return false;
  }

  render() {
    return <div />;
  }
}

function boxplot(data, node, tickFormat) {
  const margin = {
    top: 10,
    right: 100,
    bottom: 20,
    left: 100,
  };
  const width = 220 - margin.left - margin.right;
  const height = 400 - margin.top - margin.bottom;

  const chart = d3.box()
    .whiskers(iqr(1.5))
    .width(width)
    .height(height)
    .tickFormat(tickFormat);

  chart.domain(d3.extent(data));

  d3.select(node).selectAll('svg')
    .data([data])
    .enter().append('svg')
    .attr('class', 'box')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.bottom + margin.top)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`)
    .call(chart);

  function iqr(k) {
    return function(d) {
      const q1 = d.quartiles[0];
      const q3 = d.quartiles[2];
      const iqrange = (q3 - q1) * k;
      let i = -1;
      let j = d.length;
      while (d[i += 1] < q1 - iqrange);
      while (d[j -= 1] > q3 + iqrange);
      return [i, j];
    };
  }
}


class Summarizer extends React.Component {
  render() {
    const {progress} = this.props;
    const inputRecords = this.props.input_records;
    const outputRecords = this.props.output_records;
    const recordsPerSec = inputRecords != notAvailable ? Math.floor(inputRecords / (progress.totalTime / 1000)) : null;
    const computeTime = recordsPerSec ?
      <span>{humanFormat(progress.totalTime)}<br />{numFormat(recordsPerSec)} {plural(recordsPerSec, 'record')}/sec</span>
      : <span>{humanFormat(progress.totalTime)}</span>;
    const pairs = [
      ['Progress', <ProgressBar value={progress.progress} />],
      ['Total', numFormat(progress.total)],
      ['Completed', numFormat(progress.completed)],
      ['Running', numFormat(progress.running)],
      ['Pending', numFormat(progress.pending)],
      ['Killed', numFormat(progress.killed)],
      ['Failed', numFormat(progress.failed)],
      ['Input Records', inputRecords != notAvailable ? numFormat(inputRecords) : null],
      ['Output Records', outputRecords != notAvailable ? numFormat(outputRecords) : null],
      ['Compute Time', progress.totalTime > 0 ? computeTime : null],
    ];
    return (
      <table className="table">
        <tbody>{pairs.map((t, i) => <tr key={i}><th>{t[0]}</th><td>{t[1]}</td></tr>)}</tbody>
      </table>
    );
  }
}


class MapSummary extends React.Component {
  render() {
    const {job} = this.props;
    const inputRecords = this.props.counters.get('TaskCounter.MAP_INPUT_RECORDS');
    const outputRecords = this.props.counters.get('TaskCounter.MAP_OUTPUT_RECORDS');
    return <Summarizer progress={job.maps} input_records={inputRecords.map} output_records={outputRecords.map} />;
  }
}


class ReduceSummary extends React.Component {
  render() {
    const {job} = this.props;
    const inputRecords = this.props.counters.get('TaskCounter.REDUCE_INPUT_RECORDS');
    const outputRecords = this.props.counters.get('TaskCounter.REDUCE_OUTPUT_RECORDS');
    return <Summarizer progress={job.reduces} input_records={inputRecords.reduce} output_records={outputRecords.reduce} />;
  }
}
