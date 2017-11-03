package main

import (
	"github.com/colinmarc/hdfs"
)

func (jt *jobTracker) testLogsDir() error {
	client, err := hdfs.New(jt.namenodeAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.ReadDir(*yarnLogDir)
	return err
}
