package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/colinmarc/hdfs"
)

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

func (jt *jobTracker) testLogsDir() error {
	client, err := hdfs.New(*namenodeAddress)
	if err != nil {
		return err
	}

	_, err = client.ReadDir(*yarnLogDir)
	return err
}

func (jt *jobTracker) fetchLogs(id string) ([]string, error) {
	client, err := hdfs.New(*namenodeAddress)
	if err != nil {
		return nil, err
	}

	appID, jobID := hadoopIDs(id)

	jt.jobsLock.Lock()
	user := jt.jobs[jobID].Details.User
	jt.jobsLock.Unlock()

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
