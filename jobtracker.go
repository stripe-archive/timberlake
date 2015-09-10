package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/colinmarc/hdfs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// How parallel should we be when polling the resource manager?
	runningJobWorkers = 3

	// How parallel should we be when polling the history server? This is
	// primarily useful during the inital data backfill.
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

var countersToKeep = map[string]string{
	"FileSystemCounter.HDFS_BYTES_READ":    "bytes_read",
	"FileSystemCounter.S3_BYTES_READ":      "bytes_read",
	"FileSystemCounter.FILE_BYTES_READ":	"bytes_read",
	"FileSystemCounter.HDFS_BYTES_WRITTEN": "bytes_written",
	"FileSystemCounter.S3_BYTES_WRITTEN":   "bytes_written",
	"FileSystemCounter.FILE_BYTES_WRITTEN": "bytes_written",
	"TaskCounter.REDUCE_SHUFFLE_BYTES":     "hdfs.bytes_shuffled",
	"TaskCounter.MAP_INPUT_RECORDS":        "task.map_records",
	"TaskCounter.REDUCE_INPUT_RECORDS":     "task.reduce_records",
}

var configToKeep = map[string]string{
	"mapreduce.input.fileinputformat.inputdir":    "input",
	"mapreduce.output.fileoutputformat.outputdir": "output",
	"cascading.app.name":                          "cascading.app.name",
}

// This forces us to be consistent about the keys used in the Jobs map.
type jobID string

func hadoopIDs(id string) (string, jobID) {
	app := strings.Replace(id, "job_", "application_", 1)
	job := strings.Replace(id, "application_", "job_", 1)
	return app, jobID(job)
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	// Redirects are banned for two reasons:
	// 1. When a job is in the accepted state the JSON API redirects to an HTML page.
	// 2. When a job is finished The RM redirects to the history server over an
	//    address we may not be able to follow.
	return fmt.Errorf("No redirects allowed. Blocked redirect to %s.", req.URL)
}

// This is cheesy. We're getting intermittent "use of closed network connection"
// errors when talking to the RM and history server. Whenever that happens we'll
// create a new http client and move on with our lives.
var httpClient http.Client
var httpClientMutex sync.Mutex

func init() {
	httpClientMutex = sync.Mutex{}
	generateNewHTTPClient()
}

func generateNewHTTPClient() {
	httpClientMutex.Lock()
	httpClient = http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   *httpTimeout,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		CheckRedirect: checkRedirect,
		Timeout:       *httpTimeout,
	}
	httpClientMutex.Unlock()
}

func getJSON(url string, data interface{}) error {
	resp, err := httpClient.Get(url)
	if err != nil {
		if strings.Index(err.Error(), "use of closed network connection") != -1 {
			generateNewHTTPClient()
		}
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Failed getJSON: %v (%v)", url, resp.Status)
	}

	defer resp.Body.Close()
	jsonBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(jsonBytes, data)
}

type jobTracker struct {
	Jobs     map[jobID]*job
	rm       string
	hs       string
	ps       string
	running  chan *job
	finished chan *job
	backfill chan *job
	updates  chan *job
}

func newJobTracker(rmHost string, historyHost string, proxyHost string) jobTracker {
	generateNewHTTPClient()
	jt := jobTracker{
		Jobs:     make(map[jobID]*job),
		rm:       rmHost,
		hs:       historyHost,
		ps:       proxyHost,
		running:  make(chan *job, 100),
		finished: make(chan *job, 100),
		backfill: make(chan *job, 100),
		updates:  make(chan *job, 100),
	}
	return jt
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
				_, jobID := hadoopIDs(job.Details.ID)
				err := jt.UpdateJob(job, true)
				if err != nil {
					log.Println(err)
					// If a job is brand new we won't be able to fetch details from
					// the RM, so we'll get an error and end up here. If the job
					// doesn't exist in the jobs map then we'll assume that's
					// why we're here and let the mostly-empty job data
					// propagate anyways.
					if _, exists := jt.Jobs[jobID]; exists {
						continue
					}
				}
				jt.Jobs[jobID] = job
				jt.updates <- job
			}
		}()
	}

	for _ = range time.Tick(*pollInterval) {
		running := &appsResp{}
		if err := jt.ListJobs(jt.rm, running); err != nil {
			log.Println("Error listing running jobs:", err)
			continue
		}

		log.Println("Running jobs:", len(running.Apps.App))
		log.Println("Jobs in cache:", len(jt.Jobs))
		log.Println("Goroutines:", runtime.NumGoroutine())

		// We rely on jobs moving from the RM to the History Server when they
		// stop running. This doesn't always happen. If we detect a job that's
		// disappeared, mark it as GONE and forget about it. The frontend
		// doesn't know what GONE means so it ignores the job.
		for jobID, job := range jt.Jobs {
			if job.host == jt.rm && time.Now().Sub(job.updated).Seconds() > 30*pollInterval.Seconds() {
				log.Printf("%s has not been updated in thirty ticks. Removing.", jobID)
				job.Details.State = "GONE"
				jt.updates <- job
				delete(jt.Jobs, jobID)
			}
		}

		for i := range running.Apps.App {
			job := &job{Details: &running.Apps.App[i], host: jt.rm, updated: time.Now()}
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

				full := job.Details.FinishTime/1000 > time.Now().Unix()-24*60*60
				err := jt.UpdateJob(job, full)
				if err != nil {
					log.Println(err)
					continue
				}
				_, jobID := hadoopIDs(job.Details.ID)
				jt.Jobs[jobID] = job
				jt.updates <- job
			}
		}()
	}

	go func() {
		finished := &jobResp{}
		for {
			if err := jt.FinishedJobs(jt.hs, finished); err != nil {
				log.Println("Error listing backfill jobs:", err)
				time.Sleep(time.Second * 5)
			} else {
				break
			}
		}

		// This sorts in reverse order of start time so that we'll fill in the
		// details for the newest jobs first.
		sort.Sort(sort.Reverse(jobDetails(finished.Jobs.Job)))
		log.Println("Backfill jobs:", len(finished.Jobs.Job))
		for i := range finished.Jobs.Job {
			if i > jobLimit {
				break
			}
			jt.finished <- &job{Details: &finished.Jobs.Job[i], host: jt.hs}
		}
	}()

	for _ = range time.Tick(*pollInterval) {
		finished := &jobResp{}
		if err := jt.ListJobs(jt.hs, finished); err != nil {
			log.Println("Error listing finished jobs:", err)
			continue
		}

		log.Println("Finished jobs:", len(finished.Jobs.Job))
		for i, details := range finished.Jobs.Job {
			// Only fill in details for jobs we don't already have.
			_, jobID := hadoopIDs(details.ID)
			j, exists := jt.Jobs[jobID]
			if exists && j.Details.State == details.State {
				continue
			}
			jt.finished <- &job{Details: &finished.Jobs.Job[i], host: jt.hs}
		}
	}
}

func (jt *jobTracker) cleanupLoop() {
	for _ = range time.Tick(time.Second * 60) {
		details := make(jobDetails, 0)
		for _, job := range jt.Jobs {
			if job.Details.State == "SUCCEEDED" {
				details = append(details, *job.Details)
			}
		}

		before := len(jt.Jobs)

		// Enforce the job limit.
		if len(details) > jobLimit {
			sort.Sort(sort.Reverse(details))
			for _, d := range details[jobLimit:] {
				_, jobID := hadoopIDs(d.ID)
				delete(jt.Jobs, jobID)
			}
		}
		log.Printf("Forgot about %d jobs to stay near the limit.\n", len(jt.Jobs)-before)

		// Drop tasks and counters for old jobs since those are only visible in
		// details pages (and unlikely to be viewed).
		counter := 0
		for _, job := range jt.Jobs {
			cutoff := time.Now().Add(-fullDataDuration).Unix()
			if job.Details.FinishTime/1000 < cutoff && job.complete {
				job.complete = false
				job.Tasks = nil
				job.Counters = nil
				counter++
			}
		}
		log.Printf("Dropped full data for %d older jobs.\n", counter)
	}
}

func (jt *jobTracker) HasJob(id string) bool {
	_, jobID := hadoopIDs(id)
	_, exists := jt.Jobs[jobID]
	return exists
}

func (jt *jobTracker) GetJob(id string) *job {
	_, jobID := hadoopIDs(id)
	job := jt.Jobs[jobID]
	if !job.complete {
		if err := jt.UpdatePartialJob(job); err != nil {
			log.Println(err)
		}
	}
	return job
}

func (jt *jobTracker) UpdateJob(job *job, full bool) error {
	details, err := jt.FetchJobDetails(job.Details.ID, job.host)
	if err != nil {
		return err
	}
	job.Details = details

	conf, err := jt.FetchConf(job.Details.ID, job.host)
	if err != nil {
		return err
	}
	job.Conf = conf

	// This is a hack because Brushfire isn't setting the job name properly.
	if strings.Index(job.Details.Name, "null/") != -1 && job.Conf.name != "" {
		job.Details.Name = strings.Replace(job.Details.Name, "null/", job.Conf.name+"/", 1)
	}

	if !full {
		return nil
	}
	return jt.UpdatePartialJob(job)
}

func sumTimes(pairs [][]int64) int64 {
	var sum int64
	now := time.Now().Unix() * 1000
	for _, pair := range pairs {
		if pair[0] == -1 {
			continue
		}
		if pair[1] == 0 {
			sum += now - pair[0]
		} else {
			sum += pair[1] - pair[0]
		}
	}
	return sum
}

func (jt *jobTracker) UpdatePartialJob(job *job) error {
	counters, err := jt.FetchCounters(job.Details.ID, job.host)
	if err != nil {
		return err
	}
	job.Counters = counters

	tasks, err := jt.FetchTasks(job.Details.ID, job.host)
	if err != nil {
		return err
	}
	job.Tasks = tasks
	job.Details.MapsTotalTime = sumTimes(tasks.Map)
	job.Details.ReducesTotalTime = sumTimes(tasks.Reduce)
	job.Tasks.Map = trimTasks(tasks.Map)
	job.Tasks.Reduce = trimTasks(tasks.Reduce)

	job.complete = true

	return nil
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

// Sort the tasks by startTime, split them into (taskLimit) windows, and take
// the longest task in each window. This gives us a good representation of how
// the task flowed without retaining lots of data for each mapper/reducer.
func trimTasks(pairs [][]int64) [][]int64 {
	if len(pairs) < taskLimit {
		return pairs
	}

	var trimmed [][]int64

	sort.Sort(taskListByStartTime(pairs))

	sampleSize := int(len(pairs) / taskLimit)
	for i := 0; i < len(pairs)/sampleSize; i++ {
		window := pairs[i*sampleSize : min((i+1)*sampleSize, len(pairs))]
		sort.Sort(sort.Reverse(taskListByDuration(window)))
		trimmed = append(trimmed, window[0])
	}
	return trimmed
}

func (jt *jobTracker) ListJobs(host string, data interface{}) error {
	var url string
	if host == jt.rm {
		url = fmt.Sprintf("%s/ws/v1/cluster/apps/?states=running,submitted,accepted,new", host)
	} else {
		t := time.Now().Unix() - 60*60
		url = fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs?finishedTimeBegin=%d000", host, t)
	}

	return getJSON(url, data)
}

func (jt *jobTracker) FinishedJobs(host string, data *jobResp) error {
	t := time.Now().Add(-jobHistoryDuration).Unix()
	url := fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs?finishedTimeBegin=%d000", host, t)
	return getJSON(url, data)
}

func (jt *jobTracker) FetchJobDetails(id string, host string) (*jobDetail, error) {
	var url string
	appID, jobID := hadoopIDs(id)
	if host == jt.rm {
		url = fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs", jt.ps, appID)
	} else {
		url = fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs/%s", host, jobID)
	}

	jobs := &jobResp{}
	if err := getJSON(url, jobs); err != nil {
		return nil, err
	}

	if len(jobs.Jobs.Job) > 0 {
		// ResourceManager does it like this.
		return &jobs.Jobs.Job[0], nil
	}
	// HistoryServer does it like this.
	return &jobs.Job, nil
}

func (jt *jobTracker) FetchTasks(id string, host string) (*tasks, error) {
	var url string
	appID, jobID := hadoopIDs(id)
	if host == jt.rm {
		url = fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks", jt.ps, appID, jobID)
	} else {
		url = fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs/%s/tasks", host, jobID)
	}

	taskResp := &tasksResp{}
	if err := getJSON(url, taskResp); err != nil {
		return nil, err
	}

	tasks := &tasks{Map: make([][]int64, 0), Reduce: make([][]int64, 0)}

	for _, task := range taskResp.Tasks.Task {
		// SCHEDULED tasks are just noise.
		if task.State == "SCHEDULED" {
			continue
		}
		if task.Type == "MAP" {
			tasks.Map = append(tasks.Map, []int64{task.StartTime, task.FinishTime})
		} else if task.Type == "REDUCE" {
			tasks.Reduce = append(tasks.Reduce, []int64{task.StartTime, task.FinishTime})
		}
	}

	return tasks, nil
}

func (jt *jobTracker) FetchConf(id string, host string) (conf, error) {
	var url string
	appID, jobID := hadoopIDs(id)
	if host == jt.rm {
		url = fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/conf", jt.ps, appID, jobID)
	} else {
		url = fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs/%s/conf", host, jobID)
	}

	confResp := &confResp{}
	if err := getJSON(url, confResp); err != nil {
		return conf{}, err
	}

	cc := make(map[string]string, 0)

	for _, property := range confResp.Conf.Property {
		if alias, exists := configToKeep[property.Name]; exists {
			cc[alias] = property.Value
		}
	}

	conf := conf{
		Input:  cc["input"],
		Output: cc["output"],
		name:   cc["cascading.app.name"],
	}

	return conf, nil
}

func (jt *jobTracker) FetchCounters(id string, host string) ([]counter, error) {
	var url string
	appID, jobID := hadoopIDs(id)
	if host == jt.rm {
		url = fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/counters", jt.ps, appID, jobID)
	} else {
		url = fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs/%s/counters", host, jobID)
	}

	counterResp := &countersResp{}
	if err := getJSON(url, counterResp); err != nil {
		return nil, err
	}

	var counters []counter
	countersMap := make(map[string]counter)

	for _, group := range counterResp.JobCounters.CounterGroups {
		splits := strings.Split(group.Name, ".")
		groupName := splits[len(splits)-1]
		for _, c := range group.Counters {
			counterName := groupName + "." + c.Name
			if alias, exists := countersToKeep[counterName]; exists {
				if prevCounter, exists := countersMap[alias]; exists {
					countersMap[alias] = counter{
						Name:   alias,
						Total:  c.Total + prevCounter.Total,
						Map:    c.Map + prevCounter.Map,
						Reduce: c.Reduce + prevCounter.Reduce,
					}
				} else {
					countersMap[alias] = counter{
						Name:   alias,
						Total:  c.Total,
						Map:    c.Map,
						Reduce: c.Reduce,
					}
				}
			}
		}
	}

	for _, value := range countersMap {
		counters = append(counters, value)
	}

	return counters, nil
}

func (jt *jobTracker) TestLogsDir() error {
	client, err := hdfs.New(*namenodeAddress)
	if err != nil {
		return err
	}

	_, err = client.ReadDir(*yarnLogDir)
	return err
}

func (jt *jobTracker) FetchLogs(id string) ([]string, error) {
	client, err := hdfs.New(*namenodeAddress)
	if err != nil {
		return nil, err
	}

	appID, jobID := hadoopIDs(id)
	user := jt.Jobs[jobID].Details.User
	dir := fmt.Sprintf("%s/%s/logs/%s/", *yarnLogDir, user, appID)

	files, err := client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	filenames := make(chan string, len(files))
	results := make(chan string, len(files))

	var wg sync.WaitGroup

	for x := 1; x <= 5; x++ {
		go func() {
			for name := range filenames {
				result, err := findStacktrace(client, name)
				if err != nil {
					log.Println("Error reading", name, err)
				} else if result != "" {
					results <- result
				}
				wg.Done()
			}
		}()
	}

	var logs []string

	wg.Add(len(files))
	for _, file := range files {
		filenames <- dir + file.Name()
	}
	close(filenames)

	wg.Wait()
	close(results)

	for result := range results {
		logs = append(logs, result)
	}

	return logs, nil
}

// These are all the tokens that look like noise when I look at logs.
var logsToSkip = [][]byte{
	[]byte("INFO"),
	[]byte("WARN"),
	[]byte("SLF4J"),
	[]byte("log4j:ERROR"),
	[]byte("Container killed on request"),
	[]byte("Container exited with"),
	[]byte("RMCommunicator Allocator"),
	[]byte("RMContainerAllocator"),
	[]byte("TaskAttemptListenerImpl"),
	[]byte("AM com"),
	[]byte("PM com"),
}

func findStacktrace(client *hdfs.Client, name string) (string, error) {
	log.Println("Reading", name)
	file, err := client.Open(name)
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	var logs [][]byte

	lines := bytes.SplitAfter(data, []byte("\n"))

	for _, line := range lines {
		matched := false
		for _, token := range logsToSkip {
			if bytes.Contains(line, token) {
				matched = true
				break
			}
		}
		if !matched {
			logs = append(logs, line)
		}
	}
	log.Println("Finished", name)

	return string(bytes.Join(logs, nil)), nil
}

func (jt *jobTracker) KillJob(id string) error {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/%s/state", jt.rm, id)
	payload := strings.NewReader(`{"state":"KILLED"}`) //cheating
	req, err := http.NewRequest("PUT", url, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	log.Println("Killing", id, req)

	res, err := http.DefaultClient.Do(req)
	log.Println("Kill status:", res.Status)

	if res.StatusCode == 202 {
		// We have to do this ourselves because the job doesn't make it to the
		// history server using this API. :\
		log.Println("Setting", id, "to KILLED")
		_, jobID := hadoopIDs(id)
		job := jt.Jobs[jobID]

		killTime := time.Now().Unix() * 1000
		job.Details.State = "KILLED"
		job.Details.FinishTime = killTime
		job.Details.MapsKilled += job.Details.MapsRunning
		job.Details.MapsRunning = 0
		job.Details.ReducesKilled += job.Details.ReducesRunning
		job.Details.ReducesRunning = 0

		if job.Tasks != nil {
			for _, task := range job.Tasks.Map {
				if task[1] == 0 {
					task[1] = killTime
				}
			}
			for _, task := range job.Tasks.Reduce {
				if task[1] == 0 {
					task[1] = killTime
				}
			}
		}
		jt.updates <- job
	}
	return err
}
