package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)


type JobFetchClient interface {
  listJobs() (*appsResp, error)
  listFinishedJobs(since time.Time) (*jobsResp, error)
  fetchJobDetails(id string) (jobDetail, error)
  fetchTasks(id string) (tasks, error)
  fetchCounters(id string) ([]counter, error)
  fetchConf(id string) (map[string]string, error)
  getNamenodeAddress() string
}

type jobFetchClient struct {
  resourceManagerHost string
  jobHistoryHost string
  proxyHost string
  namenodeAddresses string
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


func newJobFetchClient(resourceManagerHost string, jobHistoryHost string, proxyHost string, namenodeAddresses string) JobFetchClient {
    return &jobFetchClient{
      resourceManagerHost: resourceManagerHost,
      jobHistoryHost: jobHistoryHost,
      proxyHost: proxyHost,
      namenodeAddresses: namenodeAddresses,
    }
}

// if response is not valid JSON, the response string will be returned along
// with the error
func getJSON(url string, data interface{}) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	req.Close = true
	resp, err := httpClient.Do(req)
	if err != nil {
		if strings.Index(err.Error(), "use of closed network connection") != -1 {
			log.Println("Could not get JSON due to closed network connection.")
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


func (jt *jobFetchClient) getNamenodeAddress() string {
  return jt.namenodeAddresses
}

func (jt *jobFetchClient) listJobs() (*appsResp, error) {
	url := fmt.Sprintf("%s/ws/v1/cluster/apps/?states=running,submitted,accepted,new", jt.resourceManagerHost)
	log.Printf("RM URL: %s\n", url)
	resp := &appsResp{}
	responseStr, err := getJSON(url, resp)
	var submatch = redirectRegexp.FindStringSubmatch(responseStr)
	if len(submatch) == 2 {
		log.Printf("Response indicated a redirect but it was not followed: `%s`\n", responseStr)
		log.Printf("Updating jt.resourceManagerHost from %s to %s\n", jt.resourceManagerHost, submatch[1])
		jt.resourceManagerHost = submatch[1]
	}
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (jt *jobFetchClient) listFinishedJobs(since time.Time) (*jobsResp, error) {
	url := fmt.Sprintf("%s/ws/v1/history/mapreduce/jobs?finishedTimeBegin=%d000", jt.jobHistoryHost, since.Unix())
	resp := &jobsResp{}
	_, err := getJSON(url, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (jt *jobFetchClient) fetchJobDetails(id string) (jobDetail, error) {
	appID, _ := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs", jt.proxyHost, appID)

	jobs := &jobsResp{}
	responseStr, err := getJSON(url, jobs)
	var submatch = redirectRegexp.FindStringSubmatch(responseStr)
	if len(submatch) == 2 {
		log.Printf("Response indicated a redirect but it was not followed: `%s`\n", responseStr)
		log.Printf("Updating jt.proxyHost from %s to %s\n", jt.proxyHost, submatch[1])
		jt.proxyHost = submatch[1]
	}
	if err != nil {
		return jobDetail{}, err
	}

	return jobs.Jobs.Job[0], nil
}

func (jt *jobFetchClient) fetchTasks(id string) (tasks, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/tasks", jt.proxyHost, appID, jobID)

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

func (jt *jobFetchClient) fetchCounters(id string) ([]counter, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/counters", jt.proxyHost, appID, jobID)

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

// fetchConf pulls a job's hadoop conf from the RM.
func (jt *jobFetchClient) fetchConf(id string) (map[string]string, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/conf", jt.resourceManagerHost, appID, jobID)
	confResp := &confResp{}
	if _, err := getJSON(url, confResp); err != nil {
		return nil, err
	}

	conf := make(map[string]string, len(confResp.Conf.Property))
	for _, property := range confResp.Conf.Property {
		conf[property.Name] = property.Value
	}

	return conf, nil
}
