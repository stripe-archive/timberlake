import {cleanJobName} from './utils';
import {MRJob} from './mr';

const {$} = window;

class JobConfStore {
  constructor() {
    this.pipes = {};
    this.jobConf = {};
  }

  getJobConf(jobId) {
    $.getJSON(`/jobs/${jobId}/conf`)
      .then((data) => {
        const id = data.id.replace('application_', 'job_');
        const jobConf = {
          id,
          name: cleanJobName(data.name),
          conf: data.conf || {},
        };
        this.jobConf[id] = jobConf;
        this.trigger('jobConf', jobConf);
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

export const ConfStore = new JobConfStore();

class JobStore {
  constructor() {
    this.pipes = {};
    this.lastJob = null;
    this.eventLogs = 0;
  }

  getJob(id) {
    this.lastJob = id;
    $.getJSON(`/jobs/${id}`).then((data) => {
      this.trigger('job', new MRJob(data, data.cluster));
    }).then(null, (error) => console.error(error));
  }

  getJobs() {
    $.getJSON('/jobs/').then((data) => {
      data.forEach((datum) => {
        this.trigger('jobs', (datum.jobs || []).map((d) => new MRJob(d, datum.cluster)));
      });
    }).then(null, (error) => console.error(error));
  }

  startSSE() {
    const sse = new EventSource('/sse');
    sse.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (this.eventLogs < 5) {
        debugger; // eslint-disable-line no-debugger
        console.log('GOT EVENT', event, data);
        this.eventLogs += 1;
      }
      this.trigger('job', new MRJob(data, data.cluster));
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
