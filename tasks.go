package main

import (
	"sort"
	"time"
)

type tasks struct {
	Map    [][]int64                `json:"maps"`
	Reduce [][]int64                `json:"reduces"`
	Errors map[string][]taskAttempt `json:"errors"`
}

type taskAttempt struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
	Type     string `json:"type"`
}

type taskListByStartTime [][]int64

func (ts taskListByStartTime) Len() int {
	return len(ts)
}

func (ts taskListByStartTime) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

func (ts taskListByStartTime) Less(i, j int) bool {
	return ts[i][0] < ts[j][0]
}

type taskListByDuration [][]int64

func (ts taskListByDuration) Len() int {
	return len(ts)
}

func (ts taskListByDuration) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

func (ts taskListByDuration) Less(i, j int) bool {
	return (ts[i][1] - ts[i][0]) < (ts[j][1] - ts[j][0])
}

// trimTasks sorts the tasks by startTime, splits them into (taskLimit) windows,
// and takes the longest task in each window. This gives us a good
// representation of how the task flowed without retaining lots of data for each
// mapper/reducer.
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

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
