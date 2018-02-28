package main

import "strings"

// S3JobDetail represents our stored job format, which is a little different from
// what we get from job history server
type S3JobDetail struct {
	ID         string            `json:"job_id"`
	Name       string            `json:"job_name"`
	User       string            `json:"user"`
	StartTime  int64             `json:"submit_date"`
	FinishTime int64             `json:"finish_date"`
	State      string            `json:"outcome"`
	Conf       map[string]string `json:"job_properties"`

	MapTasks []task `json:"map_tasks"`
	ReduceTasks []task `json:"reduce_tasks"`

	MapCounters    map[string]int `json:"map_counters"`
	ReduceCounters map[string]int `json:"reduce_counters"`

	MapsTotal    int `json:"total_maps"`
	ReducesTotal int `json:"total_reduces"`
}

type task struct {
	StartTime int64 `json:"launch_date"`
	EndTime   int64 `json:"finish_date"`
	Status    string `json:"task_status"`
}

// s3responseToJob translates the s3 response data to a job object
func s3responseToJob(data *S3JobDetail) *job {
	flowID, _ := data.Conf["cascading.flow.id"]
	return &job{
		Details:  s3jobdetailToJobDetail(data),
		conf:     s3responseToJobConf(data),
		Tasks:    s3responseToTasks(data),
		Counters: s3responseToCounters(data),
		FlowID:   &flowID,
	}
}

/**
 * Translates counter names
 */
func getCounterName(s3name string) string {
	if strings.Contains(s3name, "BYTES_READ") || strings.Contains(s3name, "BYTES_WRITTEN") {
		return "FileSystemCounter." + s3name
	} else if s3name == "REDUCE_SHUFFLE_BYTES" || strings.Contains(s3name, "PUT_RECORDS") {
		return "TaskCounter." + s3name
	}
	return s3name
}

func s3responseToCounters(s *S3JobDetail) []counter {
	counters := make([]counter, 0)

	for key := range s.ReduceCounters {
		counters = append(counters, counter{
			Name:   getCounterName(key),
			Total:  s.MapCounters[key] + s.ReduceCounters[key],
			Map:    s.MapCounters[key],
			Reduce: s.ReduceCounters[key],
		})
	}

	for key := range s.MapCounters {
		counters = append(counters, counter{
			Name:   getCounterName(key),
			Total:  s.MapCounters[key] + s.ReduceCounters[key],
			Map:    s.MapCounters[key],
			Reduce: s.ReduceCounters[key],
		})
	}

	return counters
}

func s3responseToTasks(s *S3JobDetail) tasks {
	tasks := tasks{Map: make([][]int64, len(s.MapTasks)), Reduce: make([][]int64, len(s.ReduceTasks))}

	for i, task := range s.MapTasks {
		tasks.Map[i] = []int64{task.StartTime, task.EndTime}
	}
	for i, task := range s.ReduceTasks {
		tasks.Reduce[i] = []int64{task.StartTime, task.EndTime}
	}

	return tasks
}

func s3responseToJobConf(s *S3JobDetail) conf {
	return conf{
		Flags:  s.Conf,
		Input:  s.Conf["mapreduce.input.fileinputformat.inputdir"],
		Output: s.Conf["mapreduce.output.fileoutputformat.outputdir"],
	}
}

func filter(vs []task, f func(task) bool) []task {
    vsf := make([]task, 0)
    for _, v := range vs {
        if f(v) {
            vsf = append(vsf, v)
        }
    }
    return vsf
}

func s3jobdetailToJobDetail(s *S3JobDetail) jobDetail {
	state := s.State
	if state == "SUCCESS" {
		state = "SUCCEEDED" // for consistency with job history server
	}

	return jobDetail{
		ID:         s.ID,
		Name:       s.Name,
		User:       s.User,
		State:      state,
		StartTime:  s.StartTime,
		FinishTime: s.FinishTime,

		MapsTotal:     s.MapsTotal,
		MapProgress:   100,
		MapsPending: 0,
		MapsRunning: 0,
		MapsCompleted: len(filter(s.MapTasks, func (t task) bool { return t.Status == "SUCCESS" })),
		MapsFailed: len(filter(s.MapTasks, func (t task) bool { return t.Status == "FAILED" })),
		MapsKilled: len(filter(s.MapTasks, func (t task) bool { return t.Status == "KILLED" })),
		MapsTotalTime: int64(s.MapCounters["CPU_MILLISECONDS"]),

		ReducesTotal:     s.ReducesTotal,
		ReduceProgress:   100,
		ReducesPending: 0,
		ReducesRunning: 0,
		ReducesCompleted: len(filter(s.ReduceTasks, func (t task) bool { return t.Status == "SUCCESS" })),
		ReducesFailed: len(filter(s.ReduceTasks, func (t task) bool { return t.Status == "FAILED" })),
		ReducesKilled: len(filter(s.ReduceTasks, func (t task) bool { return t.Status == "KILLED" })),
		ReducesTotalTime: int64(s.ReduceCounters["CPU_MILLISECONDS"]),
	}
}
