import {cleanJobName} from './utils';
import {MRJob} from './mr';

const {$} = window;

class JobConfStore {
  constructor() {
    this.pipes = {};
    this.jobConf = {};
  }

  getJobConf(jobId) {
    if (this.jobConf[jobId]) {
      this.trigger('jobConf', this.jobConf[jobId]);
      return;
    }

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
  }

  getNumClusters() {
    $.getJSON('/numClusters/').then((numClusters) => {
      this.trigger('numClusters', numClusters);
    }).then(null, (error) => console.error(error));
  }

  getJob(id) {
    this.lastJob = id;
    return $.getJSON(`/jobs/${id}`)
      .then((data) => {
        const job = new MRJob(data);
        this.trigger('job', job);
        return job;
      })
      .then(null, (error) => console.error(error));
  }

  getRelatedJobs(flowId) {
    return $.getJSON(`/jobIds/${flowId}`)
      .then((jobIds) => {
        const {lastJob} = this;
        const jobDetails = jobIds.map((jobId) => this.getJob(jobId));
        // Hack: we need to keep lastJob the same, so that we don't clear out
        // its counters
        this.lastJob = lastJob;
        const jobConfs = jobIds.map((jobId) => ConfStore.getJobConf(jobId));
        return Promise.all(jobDetails.concat(jobConfs));
      })
      .then(null, (error) => console.error(error));
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
