import React from 'react';
import ReactDOM from 'react-dom';
import { Link } from 'react-router';

import ProgressBar from './components/progress-bar';
import { Store } from './store';
import {
  lolhadoop,
  jobState,
  ACTIVE_STATES,
  timeFormat,
  secondFormat,
  cleanJobPath,
  humanFormat,
  numFormat,
  plural
} from './utils';
import { notAvailable } from './mr';

function bytesFormat(n) {
  if (n == notAvailable || !n) return null;
  var M = 1024.0 * 1024;
  var G = M * 1024;
  if (n < G) {
    return d3.format(',.1f')(n / M) + 'M';
  } else {
    return d3.format(',.1f')(n / G) + 'G';
  }
}


export default class extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
    };
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
    var jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, d => lolhadoop(d.id) == jobId);
  }

  relatedJobs(job, allJobs) {
    return allJobs.filter(d => d.fullName.indexOf(job.taskFamily) == 1);
  }

  inputs(job, allJobs) {
    var relatives = this.relatedJobs(job, allJobs);
    var outputs = _.object(_.flatten(relatives.map(j => (j.conf.output || '').split(/,/g).map(o => [o, j])), 1));
    return (job.conf.input || '').split(/,/g).map(input => outputs[input] || input);
  }

  kill() {
    this.hideKillModal();
    this.setState({killing: true});
    var job = this.getJob();
    $.post('/jobs/' + job.id + '/kill', (data, status) => {
      console.log(data, status);
      var result = data.err ? data.stderr : null;
      this.setState({killResult: result});
    }).then(null, err => {
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
    var job = this.getJob();
    if (!job) return null;
    document.title = job.name;

    var inputs = (
      <ul className="list-unstyled">
        {this.inputs(job, this.props.jobs).map(input =>
          <li key={input.id || input}>
            {_.isString(input) ?
              cleanJobPath(input) :
              <Link to={`job/${input.id}`}>{input.name}</Link>}
          </li>
         )}
      </ul>
    );

    var similar = this.props.jobs.filter(j => j.name.indexOf(job.name) != -1);
    similar = similar.filter(j => j.startTime < job.startTime);
    var prev = _.last(_.sortBy(similar, 'startTime'));
    var previous = prev ? <Link to={`job/${prev.id}`}>previous: {secondFormat(prev.duration())}</Link> : null;

    var state = jobState(job);
    if (_.contains(ACTIVE_STATES, job.state)) {
      var killing = this.state.killing;
      state = (
        <span>
          {state} <button onClick={this.showKillModal} className="btn btn-danger kill">
            <span className="label label-danger">{killing ? 'Killing' : 'Kill'}</span>
          </button>
          {this.state.showKillModal ? <KillModal hideModal={this.hideKillModal} killJob={this.kill}/> : null}
          {this.state.killResult ? <code>{this.state.killResult}</code> : null}
        </span>
      );
    } else if (_.contains(FAILED_STATES, job.state)) {
      var link = <Link to={`job/${job.id}/logs`} className="logs-link">view logs</Link>;
      state = <div>{state} {link}</div>;
    }

    var pairs = [
      ['User', job.user],
      ['Name', job.name],
      ['ID', job.id],
      ['Start', timeFormat(job.startTime)],
      ['Duration', previous ? <span>{secondFormat(job.duration())} ({previous})</span> : secondFormat(job.duration())],
      ['State', state],
      ['Input', inputs],
      ['Output', cleanJobPath(job.conf.output)],
    ];

    var stepsStr = job.conf.scaldingSteps;
    if (stepsStr) {
      var lines = stepsStr.split(',').map(val => {
        var trimmed = val.trim();
        var matches = trimmed.match(/[\w.]+:\d+/i);
        return {full: trimmed, short: matches ? matches[0] : trimmed};
      });
      var steps = (
        <ul className="list-unstyled">
          {_.uniq(lines).map(line => <li><span className="scalding-step-description" title={line.full}>{line.short}</span></li>)}
        </ul>
      );
      pairs.push(['Line Numbers', steps]);
    }

    var bytes = {
      hdfs_read: job.counters.get('hdfs.bytes_read').map,
      s3_read: job.counters.get('s3.bytes_read').map || 0,
      file_read: job.counters.get('file.bytes_read').map || 0,
      hdfs_written: job.counters.get('hdfs.bytes_written').total || 0,
      s3_written: job.counters.get('s3.bytes_written').total || 0,
      file_written: job.counters.get('file.bytes_written').total || 0,
      shuffled: job.counters.get('hdfs.bytes_shuffled').reduce || 0,
    };
    bytes.total_read = bytes.hdfs_read + bytes.s3_read + bytes.file_read;
    bytes.total_written = bytes.hdfs_written + bytes.s3_written + bytes.file_written;
    for (var key in bytes) {
      bytes[key] = bytesFormat(bytes[key])
    }
    var bytesReadTitle = "HDFS: " + bytes.hdfs_read + "\nS3: " + bytes.s3_read + "\nFile: " + bytes.file_read
    var bytesWrittenTitle = "HDFS: " + bytes.hdfs_written + "\nS3: " + bytes.s3_written + "\nFile: " + bytes.file_written

    var rv = (
      <div>
        <div className="row">
          <div className="col-md-5">
            <h4>Job Details</h4>
            <table className="table job-details">
              <tbody>
                {pairs.map(d => <tr key={d}><th>{d[0]}</th><td>{d[1]}</td></tr>)}
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
          <RelatedJobs job={job} relatives={this.relatedJobs(job, this.props.jobs)} />
        </div>
      </div>
    );
    console.timeEnd('Render Job');
    return rv;
  }
}

class KillModal extends React.Component {
  render() {
    var hideModal = this.props.hideModal;
    var killJob = this.props.killJob;
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
    var relatives = this.props.relatives;
    if (relatives.length < 2) return null;

    var data = relatives.map(j => { return {start: j.startTime, finish: j.finishTime || new Date, job: j}; });
    data = _.sortBy(data, 'start');

    var fmt = d => secondFormat(d.job.duration()) + ' ' + d.job.name;
    var links = d => '#/job/' + d.job.id;
    return (
      <div>
        <h4>Related Jobs</h4>
        <Waterfall data={data} barHeight={30} lineHeight={40} width={1200} textFormat={fmt} fillStyle="rgb(91, 192, 222)" linkFormat={links} />
      </div>
    );
  }
}


class TaskStats extends React.Component {
  render() {
    var tasks = this.props.tasks.filter(t => !t.bogus);
    var durations = sample(tasks.map(x => x.duration()).sort(), 100, _.identity);
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
  var rv = [];
  var sampleSize = arr.length / limit;
  for (var i = 0; i < arr.length / sampleSize; i++) {
    var vals = arr.slice(i * sampleSize, (i + 1) * sampleSize);
    if (vals.length === 0) continue;
    rv.push(_.max(vals, comparator));
  }
  return rv;
}


class TaskWaterfall extends React.Component {
  render() {
    var tasks = this.props.tasks;
    tasks.maps.forEach(d => d.type = 'map');
    tasks = tasks.maps.concat(tasks.reduces).filter(t => !t.bogus).map(t => {
      return {start: t.startTime, finish: t.finishTime || new Date, type: t.type};
    });
    var data = sample(_.sortBy(tasks, 'start'), 400, d => d.finish - d.start);
    return (
      <div>
        <h4>Map <i className="map-label"></i> / <i className="reduce-label"></i> Reduce Waterfall</h4>
        {data.length ? <Waterfall data={data} /> : null}
      </div>
    );
  }
}


class Waterfall extends React.Component {
  render() {
    return <div></div>;
  }

  shouldComponentUpdate() {
    d3.select(ReactDOM.findDOMNode(this)).selectAll('svg').remove();
    waterfall(this.props.data, ReactDOM.findDOMNode(this), this.props);
    return false;
  }

  componentDidMount() {
    waterfall(this.props.data, ReactDOM.findDOMNode(this), this.props);
  }
}

function waterfall(data, node, opts) {
  var defaults = {
    lineHeight: 1,
    barHeight: 1,
    width: 550,
    textFormat: t => '',
    linkFormat: null,
    fillStyle: d => d.type == 'map' ? 'rgb(91, 192, 222)' : '#E86482',
  };
  opts = _.extend(defaults, opts);

  var margin = {top: 10, right: 20, bottom: 20, left: 20};
  var width = opts.width - margin.left - margin.right;
  var height = Math.max(100, data.length * opts.lineHeight) - margin.top - margin.bottom;

  var chart = d3.waterfall()
      .width(width)
      .height(height)
      .barHeight(opts.barHeight)
      .textFormat(opts.textFormat)
      .linkFormat(opts.linkFormat)
      .barStyle(opts.fillStyle);

  var start = d3.min(_.pluck(data, 'start'));
  var finish = d3.max(_.pluck(data, 'finish'));
  chart.domain([start, finish]);

  if (((finish - start) / 1000) < 180) {
    // Show seconds if the x domain is less than three minutes.
    chart.tickFormat(d3.time.format.utc('%H:%M:%S'));
  }

  var svg = d3.select(node).selectAll('svg')
      .data([data])
    .enter().append('svg')
      .attr('class', 'waterfall')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.bottom + margin.top)
    .append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
      .call(chart);
}


class BoxPlot extends React.Component {
  render() {
    return <div></div>;
  }

  shouldComponentUpdate() {
    d3.select(ReactDOM.findDOMNode(this)).selectAll('svg').remove();
    boxplot(this.props.data, ReactDOM.findDOMNode(this), this.props.tickFormat);
    return false;
  }

  componentDidMount() {
    boxplot(this.props.data, ReactDOM.findDOMNode(this), this.props.tickFormat);
  }
}

function boxplot(data, node, tickFormat) {
  var margin = {top: 10, right: 100, bottom: 20, left: 100},
      width = 220 - margin.left - margin.right,
      height = 400 - margin.top - margin.bottom;

  var chart = d3.box()
      .whiskers(iqr(1.5))
      .width(width)
      .height(height)
      .tickFormat(tickFormat);

  chart.domain(d3.extent(data));

  var svg = d3.select(node).selectAll('svg')
      .data([data])
    .enter().append('svg')
      .attr('class', 'box')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.bottom + margin.top)
    .append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
      .call(chart);

  function iqr(k) {
    return function(d, i) {
      var q1 = d.quartiles[0],
          q3 = d.quartiles[2],
          iqr = (q3 - q1) * k,
          i = -1,
          j = d.length;
      while (d[++i] < q1 - iqr);
      while (d[--j] > q3 + iqr);
      return [i, j];
    };
  }
}


class Summarizer extends React.Component {
  render() {
    var progress = this.props.progress;
    var records = this.props.records;
    var recordsPerSec = records != notAvailable ? Math.floor(records / (progress.totalTime / 1000)) : null;
    var computeTime = recordsPerSec ?
      <span>{humanFormat(progress.totalTime)}<br/>{numFormat(recordsPerSec)} {plural(recordsPerSec, "record")}/sec</span>
      : <span>{humanFormat(progress.totalTime)}</span>;
    var pairs = [
      ['Progress', <ProgressBar value={progress.progress}/>],
      ['Total', numFormat(progress.total)],
      ['Completed', numFormat(progress.completed)],
      ['Running', numFormat(progress.running)],
      ['Pending', numFormat(progress.pending)],
      ['Killed', numFormat(progress.killed)],
      ['Failed', numFormat(progress.failed)],
      ['Records', records != notAvailable ? numFormat(records) : null],
      ['Compute Time', progress.totalTime > 0 ? computeTime : null],
    ];
    return (
      <table className="table">
        <tbody>{pairs.map(t => <tr key={t[0]}><th>{t[0]}</th><td>{t[1]}</td></tr>)}</tbody>
      </table>
    );
  }
}


class MapSummary extends React.Component {
  render() {
    var job = this.props.job;
    var records = this.props.counters.get('task.map_records');
    return <Summarizer progress={job.maps} records={records.map} />;
  }
}


class ReduceSummary extends React.Component {
  render() {
    var job = this.props.job;
    var records = this.props.counters.get('task.reduce_records');
    return <Summarizer progress={job.reduces} records={records.reduce} />;
  }
}
