import {cleanJobName} from './utils';
import {MRCounters, MRJob} from './mr';

const {$} = window;

class CCStore {
  constructor() {
    this.pipes = {};
    this.jobConfCounters = {};
  }

  getJobConfCounters(jobId) {
    $.getJSON(`/jobs/${jobId}/confcounters`)
      .then((data) => {
        const id = data.id.replace('application_', 'job_');
        const jobConfCounter = {
          id,
          name: cleanJobName(data.name),
          conf: data.conf || {},
          counters: new MRCounters(data.counters),
        };
        this.jobConfCounters[id] = jobConfCounter;
        this.trigger('jobConfCounters', jobConfCounter);
      })
      .then(null, (error) => console.error(error));
  }

  on(key, f) {
    (this.pipes[key] = this.pipes[key] || []).push(f);
  }

  trigger(key, data) {
    (this.pipes[key] || []).map((f) => f(data));
  }
}

export const ConfCountersStore = new CCStore();

class JobStore {
  constructor() {
    this.pipes = {};
    this.lastJob = null;
  }

  getJob(id) {
    this.lastJob = id;
    $.getJSON(`/jobs/${id}`).then((data) => {
      this.trigger('job', new MRJob(data));
    }).then(null, (error) => console.error(error));
  }

  getJobs() {
    $.getJSON('/jobs/').then((data) => {
      this.trigger('jobs', data.map((d) => new MRJob(d)));
    }).then(null, (error) => console.error(error));
  }

  startSSE() {
    const sse = new EventSource('/sse');
    sse.onmessage = (e) => {
      this.trigger('job', new MRJob(JSON.parse(e.data)));
    };
  }

  trigger(key, data) {
    (this.pipes[key] || []).map((f) => f(data));
  }

  on(key, f) {
    (this.pipes[key] = this.pipes[key] || []).push(f);
  }
}

export const Store = new JobStore();
