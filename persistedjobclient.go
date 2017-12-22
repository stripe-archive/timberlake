package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// PersistedJobClient fetches retired jobs from persistent storage (e.g. S3) that
// stores job indefinitely (or a very long time)
type PersistedJobClient interface {
	FetchJob(id string) (*job, error)
	FetchFlowJobIds(flowID string) ([]string, error)
}

/**
 * We expect the jobs to be stored in the bucket with the following structure:
 *
 * <s3bucket>/
 * 		<jobsPrefix>/
 *    	<jobid>.json
 * 			<otherjobid>.json
 * 		<flowPrefix>/
 * 			<flowid>/
 *				<jobid>.json
 *			<otherflowid>/
 *				<otherjobid>.json
 */
type s3JobClient struct {
	bucketName string
	jobsPrefix string
	flowPrefix string
	s3Client   *s3.S3
}

/**
 * The stored job format is a little different from what we get from the
 * job history server
 */
type s3JobDetail struct {
	ID         string            `json:"job_id"`
	Name       string            `json:"job_name"`
	User       string            `json:"user"`
	StartTime  int64             `json:"submit_date"`
	FinishTime int64             `json:"finish_date"`
	State      string            `json:"outcome"`
	Conf       map[string]string `json:"job_properties"`

	MapTasks []struct {
		StartTime int64 `json:"launch_date"`
		EndTime   int64 `json:"finish_date"`
	} `json:"map_tasks"`
	ReduceTasks []struct {
		StartTime int64 `json:"launch_date"`
		EndTime   int64 `json:"finish_date"`
	} `json:"reduce_tasks"`

	MapCounters    map[string]int `json:"map_counters"`
	ReduceCounters map[string]int `json:"reduce_counters"`

	MapsTotal    int `json:"total_maps"`
	ReducesTotal int `json:"total_reduces"`
}

// NewS3JobClient creates a storage client
func NewS3JobClient(awsRegion string, bucketName string, jobsPrefix string, flowPrefix string) PersistedJobClient {
	config := &aws.Config{
		Region: aws.String(awsRegion),
	}
	return &s3JobClient{
		bucketName: bucketName,
		jobsPrefix: jobsPrefix,
		flowPrefix: flowPrefix,
		s3Client:   s3.New(session.Must(session.NewSession(config))),
	}
}

/**
 * Expects a key like "<folder>/job_123.json"
 *
 * returns the job id
 */
func parseJobIDFromKey(key string) string {
	idStartIdx := strings.LastIndex(key, "/") + 1
	endIdx := len(key) - len(".json")
	return key[idStartIdx:endIdx]
}

func (client *s3JobClient) FetchFlowJobIds(flowID string) ([]string, error) {
	s3Key := fmt.Sprintf("%s/%s", client.flowPrefix, flowID)
	input := &s3.ListObjectsInput{
		Bucket: aws.String(client.bucketName),
		Prefix: aws.String(s3Key),
	}

	// fetch objects from s3
	result, err := client.s3Client.ListObjects(input)
	if err != nil {
		log.Printf("Failed to fetch from S3: `%s`\n", err.Error())
		return nil, err
	}

	// get just the keys
	relatedJobKeys := make([]string, len(result.Contents))
	for i, obj := range result.Contents {
		relatedJobKeys[i] = parseJobIDFromKey(*obj.Key)
	}

	return relatedJobKeys, nil
}

func (client *s3JobClient) FetchJob(id string) (*job, error) {
	s3Key := fmt.Sprintf("%s/%s.json", client.jobsPrefix, id)
	input := &s3.GetObjectInput{
		Bucket: aws.String(client.bucketName),
		Key:    aws.String(s3Key),
	}

	// fetch from S3
	result, err := client.s3Client.GetObject(input)
	if err != nil {
		log.Printf("Failed to fetch from S3: `%s`\n", err.Error())
		return nil, err
	}

	// read response body
	defer result.Body.Close()
	jsonBytes, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	// deserialize JSON
	data := &s3JobDetail{}
	err = json.Unmarshal(jsonBytes, data)
	if err != nil {
		return nil, err
	}

	flowID, _ := data.Conf["cascading.flow.id"]

	// handle the translating to be consistent with job history server
	return &job{
		Details:  s3jobdetailToJobDetail(data),
		conf:     s3responseToJobConf(data),
		Tasks:    s3responseToTasks(data),
		Counters: s3responseToCounters(data),
		FlowID:   &flowID,
	}, nil
}

/**
 * Translates counter names
 */
func getCounterName(s3name string) string {
	if strings.Contains(s3name, "BYTES_READ") || strings.Contains(s3name, "BYTES_WRITTEN") {
		return "FileSystemCounter." + s3name
	} else if s3name == "REDUCE_SHUFFLE_BYTES" {
		return "TaskCounter." + s3name
	}
	return s3name
}

func s3responseToCounters(s *s3JobDetail) []counter {
	counters := make([]counter, 0)

	for key := range s.ReduceCounters {
		counters = append(counters, counter{
			Name:   getCounterName(key),
			Total:  s.MapCounters[key] + s.ReduceCounters[key],
			Map:    s.MapCounters[key],
			Reduce: s.ReduceCounters[key],
		})
	}

	return counters
}

func s3responseToTasks(s *s3JobDetail) tasks {
	tasks := tasks{Map: make([][]int64, len(s.MapTasks)), Reduce: make([][]int64, len(s.ReduceTasks))}

	for i, task := range s.MapTasks {
		tasks.Map[i] = []int64{task.StartTime, task.EndTime}
	}
	for i, task := range s.ReduceTasks {
		tasks.Reduce[i] = []int64{task.StartTime, task.EndTime}
	}

	return tasks
}

func s3responseToJobConf(s *s3JobDetail) conf {
	return conf{
		Flags:  s.Conf,
		Input:  s.Conf["mapreduce.input.fileinputformat.inputdir"],
		Output: s.Conf["mapreduce.output.fileoutputformat.outputdir"],
	}
}

func s3jobdetailToJobDetail(s *s3JobDetail) jobDetail {
	state := s.State
	if state == "success" {
		state = "succeeded" // for consistency with job history server
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
		MapsTotalTime: int64(s.MapCounters["CPU_MILLISECONDS"]),

		ReducesTotal:     s.ReducesTotal,
		ReduceProgress:   100,
		ReducesTotalTime: int64(s.ReduceCounters["CPU_MILLISECONDS"]),
	}
}
