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

func (m *mockJobClient) fetchJobDetails(foo string) (jobDetail, error) {
  args := m.Called(foo)
  return args.Get(0).(jobDetail), args.Error(1)
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

func TestGetJobFromServer(t *testing.T) {
	var jobId = "imajobid"
	var jobDetailsFromServer = jobDetail{}
	jc := new(mockJobClient)
	jc.On("fetchJobDetails", jobId).Return(jobDetailsFromServer, nil)
	setJobTracker(jc)

	res, err := getJob(jobId)

	jc.AssertExpectations(t)
	assert.Equal(t, res.Details, jobDetailsFromServer)
	assert.Nil(t, err)
}
