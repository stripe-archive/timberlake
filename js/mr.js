import {
  cleanJobName,
} from './utils';

const {_} = window;

export const notAvailable = {};

export class MRJob {
  constructor(data) {
    this.cluster = data.cluster;
    const d = data.details;
    this.id = d.id.replace('application_', 'job_');
    this.fullName = d.name;
    this.name = cleanJobName(d.name);
    const _m = /^\[(\w+)\/\w+]/.exec(d.name);
    if (_m) {
      [, this.taskFamily] = _m;
    }
    this.state = d.state;
    this.startTime = d.startTime ? new Date(d.startTime) : new Date();
    this.startTime.setMilliseconds(0);
    this.finishTime = d.finishTime ? new Date(d.finishTime) : null;
    this.user = d.user;
    this.searchString = (`${this.name} ${this.user} ${this.id} ${this.cluster}`).toLowerCase();

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

    const tasks = data.tasks || {};
    this.tasks = {
      maps: (tasks.maps || []).map((taskData) => new MRTask(taskData)),
      reduces: (tasks.reduces || []).map((taskData) => new MRTask(taskData)),
      errors: tasks.errors,
    };
  }

  duration() {
    return (this.finishTime || new Date()) - this.startTime;
  }

  compact() {
    this.tasks.maps = [];
    this.tasks.reduces = [];
    this.counters = new MRCounters([]);
  }
}

export class MRCounters {
  constructor(counters) {
    this.data = _.object((counters || []).map((d) => [d.name, d]));
  }

  get(key) {
    return this.data[key] || {};
  }
}

export class MRTask {
  constructor(data) {
    const [start, finish] = data;
    this.startTime = start ? new Date(start) : new Date();
    this.startTime.setMilliseconds(0);
    this.finishTime = finish ? new Date(finish) : null;
    this.bogus = start === -1;
  }

  duration() {
    return (this.finishTime || new Date()) - this.startTime;
  }
}
