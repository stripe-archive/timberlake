package main

import (
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestJobTrackerJobsMapRace(t *testing.T) {
	mockClient := new(mockJobClient)
	mockHistoryClient := new(mockHdfsJobHistoryClient)

	jobresp := jobsResp{
		Jobs: jobsDetailList{
			Job: []jobDetail{jobDetail{}},
		},
	}
	mockClient.On("listFinishedJobs", mock.AnythingOfType("time.Time")).Return(&jobresp, nil)

	appresp := appsResp{
		Apps: appsDetailList{
			App: []jobDetail{jobDetail{}},
		},
	}
	mockClient.On("listJobs").Return(&appresp, nil)
	jt := newJobTracker("foo", "", "", mockClient, mockHistoryClient)

	jt.Loop()
	time.Sleep(time.Second * 5)
}
