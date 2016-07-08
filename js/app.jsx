var Router = ReactRouter;
var {Link, Route, Routes} = Router;

var sum = xs => xs.reduce((x, y) => x + y, 0);
var paddedInt = d3.format("02d");
var timeFormat = d3.time.format.utc("%a %H:%M:%S");
var lolhadoop = s => s.replace(/application|job/, '');

// Lifted from react-router.
var isLeftClickEvent = e => e.button === 0;
var isModifiedEvent = e => !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);

var ACTIVE_STATES = ['RUNNING', 'ACCEPTED'];
var FINISHED_STATES = ['SUCCEEDED', 'KILLED', 'FAILED', 'ERROR'];
var FAILED_STATES = ['FAILED', 'KILLED', 'ERROR'];


function numFormat(n) {
  if (!n) n = 0;
  return d3.format(",d")(n);
}

function percentFormat(n) {
  if (_.isNaN(n)) n = 0;
  var style = {width: n + '%'};
  var val = d3.format(".2f")(n) + "%";
  return (
    <div className="pct">
      <div className="bar positive" style={{width: n + '%'}}><span>{val}</span></div>
      <div className="bar negative" style={{width: (100 - n) + '%'}}><span>{val}</span></div>
    </div>
  );
}

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

function secondFormat(n) {
  n = n / 1000;
  var hour = Math.floor(n / 3600);
  var minute = Math.floor(n % 3600 / 60);
  var second = Math.floor(n % 3600 % 60);
  return numFormat(hour) + ":" + paddedInt(minute) + ":" + paddedInt(second);
}

function plural(n, s) {
  return n == 1 ? s : s + "s";
}

function humanFormat(n) {
  n = n / 1000;
  var hour = Math.floor(n / 3600);
  var minute = Math.floor(n % 3600 / 60);
  var second = Math.floor(n % 3600 % 60);
  if (n < 60) {
    return second + plural(second, " second");
  } else if (n < 3600) {
    return minute + plural(minute, " minute");
  }
  return numFormat(hour) + plural(hour, " hour") + " " + minute + plural(minute, " minute");
}

function jobState(job) {
  var state = job.state;
  var label = {
    'accepted': 'success',
    'succeeded': 'success',
    'killed': 'warning',
    'failed': 'danger',
    'error': 'danger',
    'running': 'primary'
  };
  return <span className={'label label-' + label[state.toLowerCase()]}>{state}</span>;
}

function cleanJobName(name) {
  return name.replace(/\[[A-Z0-9\/]+\]\s+/, '').replace(/(\w+\.)+(\w+)/, '$1$2');
}

function cleanJobPath(path) {
  if (!path) return path;
  path = path.replace(/hdfs:\/\/\w+(\.\w+)*:\d+/g, '');
  path = path.replace(/,/, ', ');
  return path;
}


var Store = (function() {
  function Store() {
    this.pipes = {};
  }
  Store.prototype = {
    getJob: function(id) {
      $.getJSON('/jobs/' + id).then(data => {
        this.trigger('job', new MRJob(data));
      }).then(null, error => console.error(error));
    },

    getJobs: function() {
      $.getJSON('/jobs/').then(data => {
        this.trigger('jobs', data.map(d => new MRJob(d)));
      }).then(null, error => console.error(error));
    },

    startSSE: function() {
      var sse = new EventSource('/sse');
      sse.onmessage = e => {
        this.trigger('job', new MRJob(JSON.parse(e.data)));
      };
    },

    trigger: function(key, data) {
      (this.pipes[key] || []).map(f => f(data));
    },

    on: function(key, f) {
      (this.pipes[key] = this.pipes[key] || []).push(f);
    }
  };

  return new Store;
})();


var App = React.createClass({
  getInitialState: function() {
    return {jobs: {}};
  },

  componentDidMount: function() {
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
    this.interval = setInterval(this.flushUpdates, 1000);
  },

  flushUpdates: function() {
    this.setState({jobs: _.extend(this.state.jobs, this.updates)});
    this.updates = {};
  },

  componentWillUnmount: function() {
    clearInterval(this.interval);
  },

  render: function() {
    var jobs = _.values(this.state.jobs);
    return (
      <div>
        <Navbar jobs={jobs} />
        <div id="main" className="container">
          {this.props.activeRouteHandler({jobs: jobs})}
        </div>
      </div>
    );
  }
});


var Navbar = React.createClass({
  render: function() {
    var running = this.props.jobs.filter(j => j.state == 'RUNNING');
    var mappers = sum(running.map(j => j.maps.running));
    var reducers = sum(running.map(j => j.reduces.running));
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
});


var BigData = React.createClass({
  mixins: [Router.Navigation],

  getInitialState: function() {
    return {filter: "", flushed: true};
  },

  componentDidMount: function() {
    // Two reasons for debouncing:
    // 1. Transitioning on every keypress was leading to dropped keys.
    // 2. Debouncing prevents every single keypress from becoming a history item.
    this.updateLocation = _.debounce(() => {
      var q = {filter: this.state.filter};
      this.transitionTo("app", null, _.extend(this.props.query, q));
      this.state.flushed = true;
    }, 250, false);
  },

  componentWillReceiveProps: function(nextProps) {
    // A change to the filter when we're flushed indicates a legit transition, probably via browser
    // back button. If we're not flushed then it's just the location bar catching up to our state.
    if (this.state.flushed && this.state.filter != nextProps.query.filter) {
      this.setState({filter: nextProps.query.filter || ""});
    }
  },

  onFilter: function(e) {
    this.updateLocation();
    this.setState({filter: e.target.value, flushed: false});
  },

  render: function() {
    console.time('BigData')
    var jobs = this.props.jobs;
    var filter = this.state.filter.toLowerCase();
    var rv = (
      <div>
        <RunningJobs jobs={jobs} query={this.props.query} onFilter={this.onFilter} filter={filter} />
        <FinishedJobs jobs={jobs} query={this.props.query} onFilter={this.onFilter} filter={filter} />
      </div>
    );
    console.timeEnd('BigData');
    return rv;
  }
});


var JobTable = {
  sorting: function(sort) {
    var s = sort.split('-');
    return s.length == 1 ? {key: s[0], dir: 1} : {key: s[1], dir: -1};
  },

  sort: function(key) {
    var s = this.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    var n = s.key == key && s.dir == -1 ? key : '-' + key;
    var q = _.object([[this.sortKey, n]]);
    this.transitionTo("app", null, _.extend(this.props.query, q));
  },

  sortedJobs: function() {
    var jobs = this.props.jobs.filter(j => _.contains(this.states, j.state));
    if (this.props.filter) {
      var parts = this.props.filter.split(/\s+/);
      jobs = jobs.filter(job => {
        return parts.every(p => job.searchString.indexOf(p) != -1);
      });
    }
    var sort = this.sorting(this.props.query[this.sortKey] || this.defaultSortKey);
    jobs = _.sortBy(jobs, row => {
      switch (sort.key) {
        case 'user': return row.user;
        case 'name': return row.name;
        case 'started': return row.startTime;
        case 'finished': return row.finishTime;
        case 'duration': return -row.startTime;
        case 'map': return row.maps.progress;
        case 'reduce': return row.reduces.progress;
        case 'state': return row.state;
      }
    });
    if (sort.dir == -1) jobs.reverse();
    return [sort, jobs];
  },

  render: function() {
    document.title = 'Timberlake :: The 20/20 Experience';
    var [sort, jobs] = this.sortedJobs();
    var sortDir = 'sort-' + (sort.dir > 0 ? 'asc' : 'desc');
    var Row = this.rowClass();
    var rows = jobs.slice(0, 150).map(job => <Row key={job.id} job={job} />);
    return (
      <div>
        <h3>
          {this.title} ({jobs.length})
          <input className="form-control" placeholder="Filter by user or text" onChange={this.props.onFilter} value={this.props.filter} autoFocus={this.autoFocus} />
        </h3>
        <table className="table sortable list-view">
          <thead>
            <tr>
              {this.headers.map(h => {
                var cls = sort.key == h ? sortDir : "";
                var click = this.sort.bind(this, h);
                return <th key={h} className={cls} onClick={click}>{h}</th>;
              })}
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    );
  }
};


var FinishedJobs = React.createClass({
  mixins: [ReactRouter.Navigation, JobTable],
  sortKey: 'fsort',
  filterKey: 'filter',
  states: FINISHED_STATES,
  defaultSortKey: '-finished',
  title: 'Finished',
  headers: 'user name started finished duration state'.split(' '),
  rowClass: () => FinishedJobRow,
});


var RunningJobs = React.createClass({
  mixins: [ReactRouter.Navigation, JobTable],
  sortKey: 'rsort',
  filterKey: 'filter',
  states: ACTIVE_STATES,
  defaultSortKey: '-started',
  title: 'Running',
  headers: 'user name started duration map reduce'.split(' '),
  rowClass: () => RunningJobRow,
  autoFocus: true,
});


var JobRow = {
  onClick: function(e) {
    if (isLeftClickEvent(e) && !isModifiedEvent(e)) {
      this.transitionTo("job", {jobId: this.props.job.id});
    }
  },

  render: function() {
    var columns = this.columns();
    return <tr onClick={this.onClick}>{columns.map((d, i) => <td key={i}>{d}</td>)}</tr>;
  }
};


var RunningJobRow = React.createClass({
  mixins: [Router.Navigation, JobRow],

  columns: function() {
    var job = this.props.job;
    return [
      job.user,
      <Link to="job" params={{jobId: job.id}}>{job.name}</Link>,
      timeFormat(job.startTime),
      secondFormat(job.duration()),
      percentFormat(job.maps.progress),
      percentFormat(job.reduces.progress),
    ];
  }
});


var FinishedJobRow = React.createClass({
  mixins: [Router.Navigation, JobRow],

  columns: function() {
    var job = this.props.job;
    return [
      job.user,
      <Link to="job" params={{jobId: job.id}}>{job.name}</Link>,
      timeFormat(job.startTime),
      timeFormat(job.finishTime),
      secondFormat(job.duration()),
      jobState(job),
    ];
  }
});


var KillModal = React.createClass({
  render: function() {
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
});


var Job = React.createClass({
  getInitialState: function() {
    return {};
  },

  componentDidMount: function() {
    Store.getJob(this.props.params.jobId);
  },

  componentWillReceiveProps: function (next) {
    if (this.props.params.jobId != next.params.jobId) {
      Store.getJob(next.params.jobId);
    }
  },

  componentWillUnmount: function() {
    clearInterval(this.interval);
  },

  getJob: function() {
    var jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, d => lolhadoop(d.id) == jobId);
  },

  relatedJobs: function(job, allJobs) {
    return allJobs.filter(d => d.fullName.indexOf(job.taskFamily) == 1);
  },

  inputs: function(job, allJobs) {
    var relatives = this.relatedJobs(job, allJobs);
    var outputs = _.object(_.flatten(relatives.map(j => (j.conf.output || '').split(/,/g).map(o => [o, j])), 1));
    return (job.conf.input || '').split(/,/g).map(input => outputs[input] || input);
  },

  kill: function() {
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
  },

  hideKillModal: function() {
    this.setState({showKillModal: false});
  },

  showKillModal: function() {
    this.setState({showKillModal: true});
  },

  render: function() {
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
              <Link to="job" params={{jobId: input.id}}>{input.name}</Link>}
          </li>
         )}
      </ul>
    );

    var similar = this.props.jobs.filter(j => j.name.indexOf(job.name) != -1);
    similar = similar.filter(j => j.startTime < job.startTime);
    var prev = _.last(_.sortBy(similar, 'startTime'));
    var previous = prev ? <Link to="job" params={{jobId: prev.id}}>previous: {secondFormat(prev.duration())}</Link> : null;

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
      var link = <Link to="logs" params={{jobId: job.id, job: job}} className="logs-link">view logs</Link>;
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
});


var JobLogs = React.createClass({
  componentDidMount: function() {
    Store.getJob(this.props.params.jobId);
  },

  componentWillReceiveProps: function (next) {
    if (this.props.params.jobId != next.params.jobId) {
      Store.getJob(next.params.jobId);
    }
  },

  getJob: function() {
    var jobId = lolhadoop(this.props.params.jobId);
    return _.find(this.props.jobs, d => lolhadoop(d.id) == jobId);
  },

  render: function() {
    var job = this.getJob();
    if (!job) return null;
    var logs = _.sortBy(_.pairs(job.tasks.errors), x => x[1].length).reverse().map(p => {
      var attempts = p[1];
      var errorMessage = p[0].split('\n')[0];
      var errorBody = p[0].split('\n').slice(1).join('\n');
      return (
        <dl>
          <dt>{attempts.length} time{attempts.length == 1 ? '' : 's'}</dt>
          <pre>
            <b>{errorMessage}</b><br/>
            {errorBody}
          </pre>
        </dl>
      );
    });
    return (
      <div>
        <h3><Link to="job" params={{jobId: job.id}}>{job.name}</Link></h3>
        <br/>
        {logs}
      </div>
    );
  }
});


var RelatedJobs = React.createClass({
  render: function() {
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
});


var TaskStats = React.createClass({
  render: function() {
    var tasks = this.props.tasks.filter(t => !t.bogus);
    var durations = sample(tasks.map(x => x.duration()).sort(), 100, _.identity);
    return (
      <div>
        <h4>{this.props.title} Timing</h4>
        {durations.length ? <BoxPlot data={durations} tickFormat={secondFormat} /> : null}
      </div>
    );
  }
});

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


var TaskWaterfall = React.createClass({
  render: function() {
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
});


var Waterfall = React.createClass({
  render: function() {
    return <div></div>;
  },

  shouldComponentUpdate: function() {
    d3.select(this.getDOMNode()).selectAll('svg').remove();
    waterfall(this.props.data, this.getDOMNode(), this.props);
    return false;
  },

  componentDidMount: function() {
    waterfall(this.props.data, this.getDOMNode(), this.props);
  }
});

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


var BoxPlot = React.createClass({
  render: function() {
    return <div></div>;
  },

  shouldComponentUpdate: function() {
    d3.select(this.getDOMNode()).selectAll('svg').remove();
    boxplot(this.props.data, this.getDOMNode(), this.props.tickFormat);
    return false;
  },

  componentDidMount: function() {
    boxplot(this.props.data, this.getDOMNode(), this.props.tickFormat);
  }
});

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


var Summarizer = React.createClass({
  render: function() {
    var progress = this.props.progress;
    var records = this.props.records;
    var recordsPerSec = records != notAvailable ? Math.floor(records / (progress.totalTime / 1000)) : null;
    var computeTime = recordsPerSec ?
      <span>{humanFormat(progress.totalTime)}<br/>{numFormat(recordsPerSec)} {plural(recordsPerSec, "record")}/sec</span>
      : <span>{humanFormat(progress.totalTime)}</span>;
    var pairs = [
      ['Progress', percentFormat(progress.progress)],
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
});


var MapSummary = React.createClass({
  render: function() {
    var job = this.props.job;
    var records = this.props.counters.get('task.map_records');
    return <Summarizer progress={job.maps} records={records.map} />;
  }
});


var ReduceSummary = React.createClass({
  render: function() {
    var job = this.props.job;
    var records = this.props.counters.get('task.reduce_records');
    return <Summarizer progress={job.reduces} records={records.reduce} />;
  }
});


React.render(
  <Routes>
    <Route handler={App}>
      <ReactRouter.Redirect from="/*/" to="/*" />
      <Route name="app" path="/" handler={BigData} ignoreScrollBehavior={true} />
      <Route name="job" path="/job/:jobId" handler={Job} />
      <Route name="logs" path="/job/:jobId/logs" handler={JobLogs} />
    </Route>
  </Routes>
, document.body);


var notAvailable = {};

function MRJob(data) {
  var _m;
  var d = data.details;
  this._data = data;
  this.id = d.id.replace('application_', 'job_');
  this.fullName = d.name;
  this.name = cleanJobName(d.name);
  this.taskFamily = (_m = /^\[(\w+)\/\w+\]/.exec(d.name)) ? _m[1] : undefined;
  this.state = d.state;
  this.startTime = d.startTime ? new Date(d.startTime) : new Date;
  this.startTime.setMilliseconds(0);
  this.finishTime = d.finishTime ? new Date(d.finishTime) : null;
  this.user = d.user;
  this.searchString = (this.name + ' ' + this.user + ' ' + this.id).toLowerCase();
  this.conf = data.conf || {};

  this.maps = {
    progress: d.mapProgress || (d.mapsTotal === 0 ? notAvailable : 100 * (d.mapsCompleted / d.mapsTotal)),
    total: d.mapsTotal,
    completed: d.mapsCompleted,
    pending: d.mapsPending,
    running: d.mapsRunning,
    failed: d.failedMapAttempts,
    killed: d.killedMapAttempts,
    totalTime: d.mapsTotalTime,
  };
  this.reduces = {
    progress: d.reduceProgress || (d.reducesTotal === 0 ? notAvailable : 100 * (d.reducesCompleted / d.reducesTotal)),
    total: d.reducesTotal,
    completed: d.reducesCompleted,
    pending: d.reducesPending,
    running: d.reducesRunning,
    failed: d.failedReduceAttempts,
    killed: d.killedReduceAttempts,
    totalTime: d.reducesTotalTime,
  };

  this.counters = new MRCounters(data.counters);

  var tasks = data.tasks || {};
  this.tasks = {
    maps: (tasks.maps || []).map(d => new MRTask(d)),
    reduces: (tasks.reduces || []).map(d => new MRTask(d)),
    errors: tasks.errors,
  };
}
MRJob.prototype = {
  duration: function() {
    return (this.finishTime || new Date) - this.startTime;
  }
};

function MRCounters(counters) {
  this.data = _.object((counters || []).map(d => [d.name, d]));
}
MRCounters.prototype = {
  get: function(key) {
    return this.data[key] || {};
  }
};

function MRTask(data) {
  var [start, finish] = data;
  this.startTime = start ? new Date(start) : new Date;
  this.startTime.setMilliseconds(0);
  this.finishTime = finish ? new Date(finish) : null;
  this.bogus = start == -1;
}
MRTask.prototype = {
  duration: function() {
    return (this.finishTime || new Date) - this.startTime;
  }
};
