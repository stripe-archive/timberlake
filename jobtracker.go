package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
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

	// Maximum number of jobs to keep track of. All data is retained in memory
	// on the server, and the details for each job are sent to the browser.
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

// Redirects are banned for two reasons:
// 1. When a job is in the accepted state the JSON API redirects to an HTML page.
// 2. When a job is finished The RM redirects to the history server over an
//    address we may not be able to follow.
var httpClient = http.Client{
	Timeout: *httpTimeout,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return fmt.Errorf("no redirects allowed. Blocked redirect to %s", req.URL)
	},
}

var redirectRegexp, _ = regexp.Compile("This is standby RM. Redirecting to the current active RM: (https?://[^/]*)")

// if response is not valid JSON, the response string will be returned along
// with the error
func getJSON(url string, data interface{}) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	req.Close = true
	resp, err := httpClient.Do(req)
	if err != nil {
		if strings.Index(err.Error(), "use of closed network connection") != -1 {
		}
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed getJSON: %v (%v)", url, resp.Status)
	}

	defer resp.Body.Close()
	jsonBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	err = json.Unmarshal(jsonBytes, data)
	if err != nil {
		var responseStr = string(jsonBytes[:])
		log.Printf("Response is not valid JSON:`\n`%s\n", responseStr)
		return responseStr, err
	}
	return "", err
}

type jobTracker struct {
	jobs     map[jobID]*job
	jobsLock sync.Mutex
	rm       string
	hs       string
	ps       string
	namenodeAddress string
	running  chan *job
	finished chan *job
	backfill chan *job
	updates  chan *job
}

func newJobTracker(rmHost string, historyHost string, proxyHost string, namenodeAddress string) *jobTracker {
	return &jobTracker{
		jobs:     make(map[jobID]*job),
		rm:       rmHost,
		hs:       historyHost,
		ps:       proxyHost,
		namenodeAddress: namenodeAddress,
		running:  make(chan *job),
		finished: make(chan *job),
		backfill: make(chan *job),
		updates:  make(chan *job),
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
		log.Printf("Listing running jobs on resource manager %s\n", jt.rm)
		running, err := jt.listJobs()
		if err != nil {
			log.Println("Error listing running jobs:", err)
			continue
		}

		log.Println("Running jobs:", len(running.Apps.App))
		log.Println("Jobs in cache:", len(jt.jobs))
		log.Println("Goroutines:", runtime.NumGoroutine())

		// We rely on jobs moving from the RM to the History Server when they
		// stop running. This doesn't always happen. If we detect a job that's
		// disappeared, mark it as GONE and forget about it. The frontend
		// doesn't know what GONE means so it ignores the job.
		for jobID, job := range jt.jobs {
			if job.running && time.Now().Sub(job.updated).Seconds() > 30*pollInterval.Seconds() {
				log.Printf("%s has not been updated in thirty ticks. Removing.", jobID)
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
				err := jt.updateFromHistoryFile(job, full)
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
			backfill, err = jt.listFinishedJobs(time.Now().Add(-jobHistoryDuration))
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

		finished, err := jt.listFinishedJobs(time.Now().Add(-dur))
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

func (jt *jobTracker) reifyJob(id string) *job {
	job := jt.getJob(id)

	if !job.running && job.partial {
		err := jt.updateFromHistoryFile(job, true)
		if err != nil {
			log.Println("Error loading full details for job:", err)
		}
	}

	return job
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
	details, err := jt.fetchJobDetails(job.Details.ID)
	if err != nil {
		return err
	}
	job.Details = details

	conf, err := jt.fetchConf(job.Details.ID)
	if err != nil {
		return err
	}
	job.conf.update(conf)

	// This is a hack because Brushfire isn't setting the job name properly.
	if strings.Index(job.Details.Name, "null/") != -1 && job.conf.name != "" {
		job.Details.Name = strings.Replace(job.Details.Name, "null/", job.conf.name+"/", 1)
	}

	counters, err := jt.fetchCounters(job.Details.ID)
	if err != nil {
		return err
	}
	job.counters = counters

	tasks, err := jt.fetchTasks(job.Details.ID)
	if err != nil {
		return err
	}
	job.Details.MapsTotalTime = sumTimes(tasks.Map)
	job.Details.ReducesTotalTime = sumTimes(tasks.Reduce)
	job.Tasks.Map = trimTasks(tasks.Map)
	job.Tasks.Reduce = trimTasks(tasks.Reduce)

	return nil
}

func (jt *jobTracker) listJobs() (*appsResp, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/?states=running,submitted,accepted,new", jt.rm)
	log.Printf("RM URL: %s\n", url)
	resp := &appsResp{}
	responseStr, err := getJSON(url, resp)
	var submatch = redirectRegexp.FindStringSubmatch(responseStr)
	if len(submatch) == 2 {
		log.Printf("Response indicated a redirect but it was not followed: `%s`\n", responseStr)
		log.Printf("Updating jt.rm from %s to %s\n", jt.rm, submatch[1])
		jt.rm = submatch[1]
	}
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (jt *jobTracker) listFinishedJobs(since time.Time) (*jobsResp, error) {
	url := fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs?finishedTimeBegin=%d000", jt.hs, since.Unix())
	resp := &jobsResp{}
	_, err := getJSON(url, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (jt *jobTracker) fetchJobDetails(id string) (jobDetail, error) {
	appID, _ := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs", jt.ps, appID)

	jobs := &jobsResp{}
	responseStr, err := getJSON(url, jobs)
	var submatch = redirectRegexp.FindStringSubmatch(responseStr)
	if len(submatch) == 2 {
		log.Printf("Response indicated a redirect but it was not followed: `%s`\n", responseStr)
		log.Printf("Updating jt.ps from %s to %s\n", jt.ps, submatch[1])
		jt.ps = submatch[1]
	}
	if err != nil {
		return jobDetail{}, err
	}

	return jobs.Jobs.Job[0], nil
}

func (jt *jobTracker) fetchTasks(id string) (tasks, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks", jt.ps, appID, jobID)

	taskResp := &tasksResp{}
	if _, err := getJSON(url, taskResp); err != nil {
		return tasks{}, err
	}

	tasks := tasks{Map: make([][]int64, 0), Reduce: make([][]int64, 0)}

	for _, task := range taskResp.Tasks.Task {
		// The API reports the start time of scheduled tasks as the start time
		// of the job. They haven't actually started though.
		startTime := task.StartTime
		if task.State == "SCHEDULED" {
			startTime = -1
		}

		if task.Type == "MAP" {
			tasks.Map = append(tasks.Map, []int64{startTime, task.FinishTime})
		} else if task.Type == "REDUCE" {
			tasks.Reduce = append(tasks.Reduce, []int64{startTime, task.FinishTime})
		}
	}

	return tasks, nil
}

func (jt *jobTracker) fetchCounters(id string) ([]counter, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/counters", jt.ps, appID, jobID)

	counterResp := &countersResp{}
	if _, err := getJSON(url, counterResp); err != nil {
		return nil, err
	}

	var counters []counter

	for _, group := range counterResp.JobCounters.CounterGroups {
		splits := strings.Split(group.Name, ".")
		groupName := splits[len(splits)-1]
		for _, c := range group.Counters {
			counters = append(counters, counter{
				Name:   groupName + "." + c.Name,
				Total:  c.Total,
				Map:    c.Map,
				Reduce: c.Reduce,
			})
		}
	}

	return counters, nil
}
