package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadHistory(t *testing.T) {
	f, err := os.Open("test/sleepjob.jhist")
	require.NoError(t, err, "test jhist file should load")

	job := job{}
	err = loadHistFile(f, &job, true)
	require.NoError(t, err, "loading from a hist file should work")

	assert.Equal(t, "job_1329348432655_0001", job.Details.ID, "the job id should be correct")
	assert.Equal(t, "Sleep job", job.Details.Name, "the job name should be correct")
	assert.Equal(t, "user", job.Details.User, "the user should be correct")
	assert.Equal(t, "SUCCEEDED", job.Details.State, "the job should be marked successful")
	assert.Equal(t, int64(1329348448308), job.Details.StartTime, "the start time should be correct")
	assert.Equal(t, int64(1329348468601), job.Details.FinishTime, "the finish time should be correct")

	assert.Equal(t, 10, job.Details.MapsTotal, "the number of map tasks should be correct")
	assert.Equal(t, 10, job.Details.MapsCompleted, "the number of completed map attempts should be correct")
	assert.Equal(t, 1, job.Details.MapsFailed, "the number of failed map attempts should be correct")
	assert.Equal(t, 0, job.Details.MapsKilled, "the number of killed map attempts should be correct")
	assert.Equal(t, int64(101610), job.Details.MapsTotalTime, "the total time spent in mappers should be correct")

	assert.Equal(t, 1, job.Details.ReducesTotal, "the number of reducer tasks should be correct")
	assert.Equal(t, 1, job.Details.ReducesCompleted, "the number of completed reducer attempts should be correct")
	assert.Equal(t, 0, job.Details.ReducesFailed, "the number of failed reducer attempts should be correct")
	assert.Equal(t, 0, job.Details.ReducesKilled, "the number of killed reducer attempts should be correct")
	assert.Equal(t, int64(3605), job.Details.ReducesTotalTime, "the total time spent in reducers should be correct")

	assert.Equal(t, 11, len(job.Tasks.Map), "the list of map task times should be the right length")
	assert.Equal(t, 1, len(job.Tasks.Reduce), "the list of reduce task times should be the right length")

	assert.Equal(t, 1, len(job.Tasks.Errors), "the list of errors should be the right length")
	attempts := []taskAttempt{taskAttempt{ID: "attempt_1457998088753_7918_m_000014_0", Hostname: "bigdata33", Type: "MAP"}}
	assert.Equal(t, attempts, job.Tasks.Errors["This is an error."], "the error attempts are correct")

	counters := make(map[string]counter)
	for _, c := range job.Counters {
		counters[c.Name] = c
	}

	assert.Equal(t, "hdfs.bytes_read", counters["hdfs.bytes_read"].Name, "the hdfs.bytes_read counter should be set")
	assert.Equal(t, 480, counters["hdfs.bytes_read"].Total, "the hdfs.bytes_read counter total should be correct")
	assert.Equal(t, 480, counters["hdfs.bytes_read"].Map, "the hdfs.bytes_read counter for maps should be correct")
	assert.Equal(t, 0, counters["hdfs.bytes_read"].Reduce, "the hdfs.bytes_read counter for reduces should be correct")
}
