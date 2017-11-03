package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
)

var jhistHeader = []byte("Avro-Json")

type histEvent struct {
	Type  string          `json:"type"`
	Event json.RawMessage `json:"event"`
}

type jobSubmittedEvent struct {
	Ev struct {
		ID   string `json:"jobid"`
		Name string `json:"jobName"`
		User string `json:"userName"`
	} `json:"org.apache.hadoop.mapreduce.jobhistory.JobSubmitted"`
}

type jobInitedEvent struct {
	Ev struct {
		ID           string `json:"jobid"`
		LaunchTime   int64  `json:"launchTime"`
		TotalMaps    int    `json:"totalMaps"`
		TotalReduces int    `json:"totalReduces"`
	} `json:"org.apache.hadoop.mapreduce.jobhistory.JobInited"`
}

type jobFinishedEvent struct {
	Ev struct {
		ID         string `json:"jobid"`
		FinishTime int64  `json:"finishTime"`
	} `json:"org.apache.hadoop.mapreduce.jobhistory.JobFinished"`
}

type jobFailedEvent struct {
	Ev struct {
		ID         string `json:"jobid"`
		FinishTime int64  `json:"finishTime"`
		Status     string `json:"jobStatus"`
	} `json:"org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletion"`
}

type attemptStartedEvent struct {
	Ev attemptEvent `json:"org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStarted"`
}

type mapFinishedEvent struct {
	Ev attemptEvent `json:"org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished"`
}

type reduceFinishedEvent struct {
	Ev attemptEvent `json:"org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinished"`
}

type taskFailedEvent struct {
	Ev attemptEvent `json:"org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion"`
}

type attemptEvent struct {
	ID         string `json:"attemptId"`
	Type       string `json:"taskType"`
	StartTime  int64  `json:"startTime"`
	FinishTime int64  `json:"finishTime"`
	Error      string `json:"error"`
	Hostname   string `json:"hostname"`
	Status     string `json:"status"`
	Counters   struct {
		Groups []struct {
			Name   string `json:"name"`
			Counts []struct {
				Name  string `json:"name"`
				Value int    `json:"value"`
			}
		} `json:"groups"`
	} `json:"counters"`
}

type jhistParser struct {
	job  *job
	full bool

	scanner  *bufio.Scanner
	attempts map[string]attemptEvent
}

// loadHistFile streams through the jhist file represented by r, and updates
// the given job's details.
func loadHistFile(r io.Reader, job *job, full bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Scan()
	if scanner.Err() != nil {
		return scanner.Err()
	}

	if !bytes.Equal(scanner.Bytes(), jhistHeader) {
		return errors.New("invalid Avro-Json header")
	}

	parser := &jhistParser{
		job:      job,
		full:     full,
		scanner:  scanner,
		attempts: make(map[string]attemptEvent),
	}

	return parser.parse()
}

func (jp *jhistParser) parse() error {
	// Reset these so we can count the events.
	jp.job.Details.MapsCompleted = 0
	jp.job.Details.MapsFailed = 0
	jp.job.Details.MapsKilled = 0
	jp.job.Details.ReducesCompleted = 0
	jp.job.Details.ReducesFailed = 0
	jp.job.Details.ReducesKilled = 0

	lineNumber := 1
	for jp.scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(jp.scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		wrapper := histEvent{}
		err := json.Unmarshal(line, &wrapper)
		if err != nil {
			return fmt.Errorf("line %d: %s", lineNumber, err)
		}

		switch wrapper.Type {
		case "JOB_SUBMITTED":
			jp.parseJobSubmitted(wrapper.Event)
		case "JOB_INITED":
			jp.parseJobInited(wrapper.Event)
		case "JOB_FINISHED":
			jp.parseJobFinished(wrapper.Event)
		case "JOB_FAILED":
			jp.parseJobFailed(wrapper.Event)
		case "MAP_ATTEMPT_STARTED":
			jp.parseTaskStarted(wrapper.Event)
		case "MAP_ATTEMPT_FINISHED":
			jp.job.Details.MapsCompleted++
			jp.parseMapFinished(wrapper.Event)
		case "MAP_ATTEMPT_FAILED":
			jp.job.Details.MapsFailed++
			jp.parseTaskFailed(wrapper.Event)
		case "MAP_ATTEMPT_KILLED":
			jp.job.Details.MapsKilled++
			jp.parseTaskFailed(wrapper.Event)
		case "REDUCE_ATTEMPT_STARTED":
			jp.parseTaskStarted(wrapper.Event)
		case "REDUCE_ATTEMPT_FINISHED":
			jp.job.Details.ReducesCompleted++
			jp.parseReduceFinished(wrapper.Event)
		case "REDUCE_ATTEMPT_FAILED":
			jp.job.Details.ReducesFailed++
			jp.parseTaskFailed(wrapper.Event)
		case "REDUCE_ATTEMPT_KILLED":
			jp.job.Details.ReducesKilled++
			jp.parseTaskFailed(wrapper.Event)
		}

		lineNumber++
	}

	if jp.scanner.Err() != nil {
		return jp.scanner.Err()
	}

	if !jp.full {
		return nil
	}

	// Consolidate tasks and counters. We kinda lazily combine the attempts into
	// tasks here when trimTasks runs over them. We can't just look at the task
	// events, because the historyserver does the same misdirection - it sets
	// startTime for the task to the startTime of the first attempt, for example.
	tasks := tasks{
		Map:    make([][]int64, 0),
		Reduce: make([][]int64, 0),
		Errors: make(map[string][]taskAttempt),
	}
	counters := make(map[string]counter)
	for _, attempt := range jp.attempts {
		// Save the task times.
		if attempt.Type == "MAP" {
			tasks.Map = append(tasks.Map, []int64{attempt.StartTime, attempt.FinishTime})
		} else if attempt.Type == "REDUCE" {
			tasks.Reduce = append(tasks.Reduce, []int64{attempt.StartTime, attempt.FinishTime})
		}

		if attempt.Status == "FAILED" && attempt.Error != "" {
			if _, exists := tasks.Errors[attempt.Error]; !exists {
				tasks.Errors[attempt.Error] = make([]taskAttempt, 0)
			}
			tasks.Errors[attempt.Error] = append(tasks.Errors[attempt.Error], taskAttempt{
				ID:       attempt.ID,
				Hostname: attempt.Hostname,
				Type:     attempt.Type,
			})
		}

		// Update any counters from the attempt.
		for _, group := range attempt.Counters.Groups {
			groupName := group.Name[strings.LastIndex(group.Name, ".")+1:]
			for _, count := range group.Counts {
				counterName := fmt.Sprintf("%s.%s", groupName, count.Name)
				counter := counters[counterName]
				counter.Name = counterName
				counter.Total += count.Value

				if attempt.Type == "MAP" {
					counter.Map += count.Value
				} else if attempt.Type == "REDUCE" {
					counter.Reduce += count.Value
				}

				counters[counterName] = counter
			}
		}
	}

	jp.job.Details.MapsTotalTime = sumTimes(tasks.Map)
	jp.job.Details.ReducesTotalTime = sumTimes(tasks.Reduce)
	jp.job.Tasks.Map = trimTasks(tasks.Map)
	jp.job.Tasks.Reduce = trimTasks(tasks.Reduce)
	jp.job.Tasks.Errors = tasks.Errors
	for _, counter := range counters {
		jp.job.Counters = append(jp.job.Counters, counter)
	}

	return nil
}

func (jp *jhistParser) parseJobSubmitted(b []byte) {
	ev := jobSubmittedEvent{}
	json.Unmarshal(b, &ev)

	jp.job.Details.ID = ev.Ev.ID
	jp.job.Details.Name = ev.Ev.Name
	jp.job.Details.User = ev.Ev.User
}

func (jp *jhistParser) parseJobInited(b []byte) {
	ev := jobInitedEvent{}
	json.Unmarshal(b, &ev)

	jp.job.Details.ID = ev.Ev.ID
	jp.job.Details.StartTime = ev.Ev.LaunchTime
	jp.job.Details.MapsTotal = ev.Ev.TotalMaps
	jp.job.Details.ReducesTotal = ev.Ev.TotalReduces
}

func (jp *jhistParser) parseJobFinished(b []byte) {
	ev := jobFinishedEvent{}
	json.Unmarshal(b, &ev)

	jp.job.Details.ID = ev.Ev.ID
	jp.job.Details.FinishTime = ev.Ev.FinishTime
	jp.job.Details.State = "SUCCEEDED"
}

func (jp *jhistParser) parseJobFailed(b []byte) {
	ev := jobFailedEvent{}
	json.Unmarshal(b, &ev)

	jp.job.Details.ID = ev.Ev.ID
	jp.job.Details.FinishTime = ev.Ev.FinishTime
	jp.job.Details.State = ev.Ev.Status
}

func (jp *jhistParser) parseTaskStarted(b []byte) {
	ev := attemptStartedEvent{}
	json.Unmarshal(b, &ev)

	jp.attempts[ev.Ev.ID] = ev.Ev
}

func (jp *jhistParser) parseMapFinished(b []byte) {
	ev := mapFinishedEvent{}
	json.Unmarshal(b, &ev)

	startTime := jp.attempts[ev.Ev.ID].StartTime
	ev.Ev.StartTime = startTime
	jp.attempts[ev.Ev.ID] = ev.Ev
}

func (jp *jhistParser) parseReduceFinished(b []byte) {
	ev := reduceFinishedEvent{}
	json.Unmarshal(b, &ev)

	startTime := jp.attempts[ev.Ev.ID].StartTime
	ev.Ev.StartTime = startTime
	jp.attempts[ev.Ev.ID] = ev.Ev
}

func (jp *jhistParser) parseTaskFailed(b []byte) {
	ev := taskFailedEvent{}
	json.Unmarshal(b, &ev)

	startTime := jp.attempts[ev.Ev.ID].StartTime
	ev.Ev.StartTime = startTime
	jp.attempts[ev.Ev.ID] = ev.Ev
}

// findHistoryAndConfFiles locates and returns the .jhist and _conf.xml files for the given
// job.
func findHistoryAndConfFiles(client *hdfs.Client, jobID jobID, finishTime int64) (string, string, error) {
	parts := strings.Split(string(jobID), "_")
	sort, _ := strconv.ParseInt(parts[len(parts)-1], 10, 0)
	t := time.Unix(finishTime/1000, 0)
	histPath := fmt.Sprintf("%s/%04d/%02d/%02d/%06d",
		*yarnHistoryDir, t.Year(), t.Month(), t.Day(), sort/1000)

	infos, err := client.ReadDir(histPath)
	if err != nil {
		return "", "", err
	}

	var confFile, histFile string
	for _, info := range infos {
		if strings.HasPrefix(info.Name(), string(jobID)) {
			p := path.Join(histPath, info.Name())
			if strings.HasSuffix(p, "conf.xml") {
				confFile = p
			} else if strings.HasSuffix(p, ".jhist") {
				histFile = p
			}

			if confFile != "" && histFile != "" {
				return confFile, histFile, nil
			}
		}
	}

	return "", "", fmt.Errorf("no matching files found at %s", histPath)
}

// updateFromHistoryFile updates a job's details by loading its saved 'jhist'
// file stored in hdfs, along with the stored jobconf xml file.
func (jt *jobTracker) updateFromHistoryFile(job *job, full bool) error {
	now := time.Now()

	client, err := hdfs.New(jt.namenodeAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	_, jobID := hadoopIDs(job.Details.ID)
	confFile, histFile, err := findHistoryAndConfFiles(client, jobID, job.Details.FinishTime)
	if err != nil {
		return fmt.Errorf("couldn't find history file for %s: %s", jobID, err)
	}

	histFileReader, err := client.Open(histFile)
	if err != nil {
		return fmt.Errorf("couldn't open history file at %s: %s", histFile, err)
	}

	err = loadHistFile(histFileReader, job, full)
	if err != nil {
		return fmt.Errorf("couldn't read history file at %s: %s", histFile, err)
	}

	confFileReader, err := client.Open(confFile)
	if err != nil {
		return fmt.Errorf("couldn't open jobconf at %s: %s", confFile, err)
	}

	conf, err := loadConf(confFileReader)
	if err != nil {
		return fmt.Errorf("couldn't read jobconf at %s: %s", confFile, err)
	}

	log.Println("Read jobConf and history file for", jobID, "in", time.Now().Sub(now))

	job.Conf.update(conf)
	if full {
		job.partial = false
	}

	return nil
}
