package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockJobClient struct {
	jobFetchClient
	mock.Mock
}

/**
 * sets a jobTracker to main.jts
 */
func setJobTracker(client JobFetchClient) *jobTracker {
	jts = make(map[string]*jobTracker)
	var jt = newJobTracker( "foo", client)
	jts["testCluster"] = jt
	return jt
}


func TestGetNonExistentJob(t *testing.T) {
	jc := new(mockJobClient)
	jc.On("fetchJobDetails", "idontexist").Return(jobDetail{}, fmt.Errorf("bad!"))
	setJobTracker(jc)

	res, err := getJob("idontexist")

	assert.Nil(t, err)
	assert.Nil(t, res)
}

func TestGetJobFromMemory(t *testing.T) {
	var jobId = "job123"
	var job = &job{}
	var jt = setJobTracker(new(mockJobClient))
	jt.jobs[jobID(jobId)] = job

	res, err := getJob(jobId)

	assert.Nil(t, err)
	assert.Equal(t, res, job)
}
