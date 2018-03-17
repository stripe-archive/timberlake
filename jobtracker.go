package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// How parallel should we be when polling the resource manager?
	runningJobWorkers = 1

	// How parallel should we be when polling the history server? This is
	// primarily useful during the initial data backfill.
	finishedJobWorkers = 3

	// Maximum number of jobs to keep track of per cluster. All data is retained
	// in memory on the server, and the details for each job are sent to the
	// browser.
	jobLimit = 5000

	// How many hours of history should we ask for from the job server?
	jobHistoryDuration = time.Hour * 24 * 7

	// The server will only keep partial data for finished jobs older than this.
	// Keeping partial data reduces server memory usage. If someone tries to
	// fetch the job from the web the full data will be requested again from the
	// history server.
	fullDataDuration = time.Hour * 24

	// How many pairs of start/finish times should we keep around for each job?
	taskLimit = 500
)

// This forces us to be consistent about the keys used in the Jobs map.
type jobID string

func hadoopIDs(id string) (string, jobID) {
	app := strings.Replace(id, "job_", "application_", 1)
	job := strings.Replace(id, "application_", "job_", 1)
	return app, jobID(job)
}

type jobTracker struct {
	jobClient       RecentJobClient
	jobHistoryClient HdfsJobHistoryClient
	clusterName     string
	publicResourceManagerURL string
	publicHistoryServerURL string
	jobs            map[jobID]*job
	jobsLock        sync.Mutex
	rm              string
	hs              string
	ps              string
	namenodeAddress string
	running         chan *job
	finished        chan *job
	backfill        chan *job
	updates         chan *job
}


func newJobTracker(clusterName string, publicResourceManagerURL string, publicHistoryServerURL string, jobClient RecentJobClient, jobHistoryClient HdfsJobHistoryClient) *jobTracker {
	return &jobTracker{
		clusterName: clusterName,
		jobHistoryClient: jobHistoryClient,
		publicResourceManagerURL: publicResourceManagerURL,
		publicHistoryServerURL: publicHistoryServerURL,

		jobClient:   jobClient,
		jobs:        make(map[jobID]*job),
		running:     make(chan *job),
		finished:    make(chan *job),
		backfill:    make(chan *job),
		updates:     make(chan *job),
	}
}

func (jt *jobTracker) Loop() {
	go jt.runningJobLoop()
	go jt.finishedJobLoop()
	go jt.cleanupLoop()
}

func (jt *jobTracker) runningJobLoop() {
	for x := 1; x <= runningJobWorkers; x++ {
		go func() {
			for job := range jt.running {
				err := jt.updateJob(job)
				if err != nil {
					log.Println("An error occurred updating the job", job.Details.ID, err)
					// If a job is brand new we won't be able to fetch details from
					// the RM, so we'll get an error and end up here. If the job
					// doesn't exist in the jobs map then we'll assume that's
					// why we're here and let the mostly-empty job data
					// propagate anyways.
					if jt.hasJob(job.Details.ID) {
						continue
					}
				}

				jt.saveJob(job)
				jt.updates <- job
			}
		}()
	}

	for range time.Tick(*pollInterval) {
		log.Printf("Listing running jobs in cluster %s on resource manager %s\n", jt.clusterName, jt.rm)
		running, err := jt.jobClient.listJobs()
		if err != nil {
			log.Println("Error listing running jobs:", err)
			continue
		}

		log.Printf("Running jobs in cluster %s: %d\n", jt.clusterName, len(running.Apps.App))
		log.Println("Jobs in cache:", len(jt.jobs))
		log.Println("Goroutines:", runtime.NumGoroutine())

		// We rely on jobs moving from the RM to the History Server when they
		// stop running. This doesn't always happen. If we detect a job that's
		// disappeared, mark it as GONE and forget about it. The frontend
		// doesn't know what GONE means so it ignores the job.
		for jobID, job := range jt.jobs {
			if job.running && time.Now().Sub(job.updated).Seconds() > 30*pollInterval.Seconds() {
				log.Printf("%s in cluster %s has not been updated in thirty ticks. Removing.\n", jobID, jt.clusterName)
				job.Details.State = "GONE"
				jt.updates <- job
				jt.deleteJob(job.Details.ID)
			}
		}

		for i := range running.Apps.App {
			job := &job{Details: running.Apps.App[i], running: true, updated: time.Now()}
			jt.running <- job
		}
	}

}

func (jt *jobTracker) finishedJobLoop() {
	for x := 1; x <= finishedJobWorkers; x++ {
		go func() {
			for {
				var job *job

				select {
				case job = <-jt.finished:
				case job = <-jt.backfill:
				}

				full := job.Details.FinishTime/1000 > time.Now().Add(-fullDataDuration).Unix()
				err := jt.jobHistoryClient.updateFromHistoryFile(jt, job, full)
				if err != nil {
					log.Println("An error occurred updating from history file", job.Details.ID, err)
					continue
				}

				job.updated = time.Now()
				jt.saveJob(job)
				jt.updates <- job
			}
		}()
	}

	go func() {
		var backfill *jobsResp
		var err error
		for {
			backfill, err = jt.jobClient.listFinishedJobs(time.Now().Add(-jobHistoryDuration))
			if err != nil {
				log.Println("Error listing backfill jobs:", err)
				time.Sleep(time.Second * 5)
			} else {
				break
			}
		}

		// This sorts in reverse order of start time so that we'll fill in the
		// details for the newest jobs first.
		sort.Sort(sort.Reverse(jobDetails(backfill.Jobs.Job)))
		total := len(backfill.Jobs.Job)
		log.Println("Jobs to backfill:", total)
		for i := range backfill.Jobs.Job {
			if i > jobLimit {
				break
			}

			if i%100 == 0 {
				log.Printf("Backfilled %d/%d jobs", i, total)
			}

			jt.backfill <- &job{Details: backfill.Jobs.Job[i], running: false}
		}

		log.Println("Finished backfilling jobs.")
	}()

	for range time.Tick(*pollInterval) {
		dur := 1 * time.Minute
		if dur < (*pollInterval * 2) {
			dur = *pollInterval * 2
		}

		finished, err := jt.jobClient.listFinishedJobs(time.Now().Add(-dur))
		if err != nil {
			log.Println("Error loading finished jobs from the history server:", err)
			continue
		}

		log.Println("Finished jobs:", len(finished.Jobs.Job))

		for i, details := range finished.Jobs.Job {
			// Only fill in details for jobs we don't already have.
			if jt.hasJob(details.ID) {
				j := jt.getJob(details.ID)
				if j.Details.State == details.State {
					continue
				}
			}

			jt.finished <- &job{Details: finished.Jobs.Job[i], running: false}
		}
	}
}

func (jt *jobTracker) cleanupLoop() {
	for range time.Tick(time.Second * 60) {
		jt.jobsLock.Lock()

		details := make(jobDetails, 0)
		for _, job := range jt.jobs {
			if job.Details.State == "SUCCEEDED" {
				details = append(details, job.Details)
			}
		}

		// Enforce the job limit.
		before := len(jt.jobs)
		if len(details) > jobLimit {
			sort.Sort(sort.Reverse(details))
			for _, d := range details[jobLimit:] {
				_, jobID := hadoopIDs(d.ID)
				delete(jt.jobs, jobID)
			}
		}

		log.Printf("Forgot about %d jobs to stay near the limit.\n", len(jt.jobs)-before)

		// Drop tasks and counters for old jobs since those are only visible in
		// details pages (and unlikely to be viewed).

		counter := 0
		for jobID, j := range jt.jobs {
			if j.running || j.partial {
				continue
			}

			cutoff := time.Now().Add(-fullDataDuration).Unix()
			if j.Details.FinishTime/1000 < cutoff {
				cleaned := &job{Details: j.Details, running: j.running, partial: true}
				jt.jobs[jobID] = cleaned
				counter++
			}
		}

		jt.jobsLock.Unlock()
		log.Printf("Dropped full data for %d older jobs.\n", counter)
	}
}

func (jt *jobTracker) hasJob(id string) bool {
	_, jobID := hadoopIDs(id)

	jt.jobsLock.Lock()
	defer jt.jobsLock.Unlock()

	_, exists := jt.jobs[jobID]
	return exists
}

func (jt *jobTracker) getJob(id string) *job {
	_, jobID := hadoopIDs(id)

	jt.jobsLock.Lock()
	defer jt.jobsLock.Unlock()

	return jt.jobs[jobID]
}

func (jt *jobTracker) reifyJob(job *job) {
	if !job.running && job.partial {
		err := jt.jobHistoryClient.updateFromHistoryFile(jt, job, true)
		if err != nil {
			log.Println("Error loading full details for job:", err)
		}
	}

	appID, _jobID := hadoopIDs(job.Details.ID)

	job.Cluster = jt.clusterName
	job.ResourceManagerURL = fmt.Sprintf("%s/cluster/app/%s", jt.publicResourceManagerURL, appID)
	job.JobHistoryURL = fmt.Sprintf("%s/jobhistory/job/%s", jt.publicHistoryServerURL, _jobID)
}

func (jt *jobTracker) deleteJob(id string) {
	_, jobID := hadoopIDs(id)

	jt.jobsLock.Lock()
	defer jt.jobsLock.Unlock()

	delete(jt.jobs, jobID)
}

func (jt *jobTracker) saveJob(job *job) {
	_, jobID := hadoopIDs(job.Details.ID)

	jt.jobsLock.Lock()
	defer jt.jobsLock.Unlock()

	jt.jobs[jobID] = job
}

// updateJob reads the latest state from the resourcemanager.
func (jt *jobTracker) updateJob(job *job) error {
	details, err := jt.jobClient.fetchJobDetails(job.Details.ID)
	if err != nil {
		log.Println("An error occurred fetching job details", job.Details.ID, err)
		return err
	}
	job.Details = details

	conf, err := jt.jobClient.fetchConf(job.Details.ID)
	if err != nil {
		log.Println("An error occurred fetching job conf", job.Details.ID, err)
		return err
	}
	job.conf.update(conf)

	// This is a hack because Brushfire isn't setting the job name properly.
	if strings.Index(job.Details.Name, "null/") != -1 && job.conf.name != "" {
		job.Details.Name = strings.Replace(job.Details.Name, "null/", job.conf.name+"/", 1)
	}

	counters, err := jt.jobClient.listCounters(job.Details.ID)
	if err != nil {
		log.Println("An error occurred fetching job counters", job.Details.ID, err)
		return err
	}
	job.Counters = counters

	tasks, err := jt.jobClient.fetchTasks(job.Details.ID)
	if err != nil {
		log.Println("An error occurred fetching job tasks", job.Details.ID, err)
		return err
	}
	job.Details.MapsTotalTime = sumTimes(tasks.Map)
	job.Details.ReducesTotalTime = sumTimes(tasks.Reduce)
	job.Tasks.Map = trimTasks(tasks.Map)
	job.Tasks.Reduce = trimTasks(tasks.Reduce)

	return nil
}

func (jt *jobTracker) sendUpdates(sse *sse) {
	for job := range jt.updates {
		jt.reifyJob(job)
		jsonBytes, err := json.Marshal(job)
		if err != nil {
			log.Println("json error: ", err)
		} else {
			sse.events <- jsonBytes
		}
	}
}
