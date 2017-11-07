package main

import (
	"encoding/xml"
	"fmt"
	"io"
)

type conf struct {
	Flags         map[string]string `json:"flags"`
	Input         string            `json:"input"`
	Output        string            `json:"output"`
	ScaldingSteps string            `json:"scaldingSteps"`
	name          string
}

type confProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type jobConf struct {
	XMLName    xml.Name       `xml:"configuration"`
	Properties []confProperty `xml:"property"`
}

// update applies known configuration properties to the job object.
func (conf *conf) update(c map[string]string) {
	if conf.Flags == nil {
		conf.Flags = make(map[string]string)
	}
	for key, value := range c {
		conf.Flags[key] = value
		switch key {
		case "mapreduce.input.fileinputformat.inputdir":
			conf.Input = value
		case "mapreduce.output.fileoutputformat.outputdir":
			conf.Output = value
		case "scalding.step.descriptions":
			conf.ScaldingSteps = value
		case "cascading.app.name":
			conf.name = value
		}
	}
}

// loadConf loads a job's hadoop conf from an xml file represented by r.
func loadConf(r io.Reader) (map[string]string, error) {
	decoder := xml.NewDecoder(r)

	parsed := jobConf{}
	err := decoder.Decode(&parsed)
	if err != nil {
		return nil, err
	}

	conf := make(map[string]string, len(parsed.Properties))
	for _, prop := range parsed.Properties {
		conf[prop.Name] = prop.Value
	}

	return conf, nil
}

// fetchConf pulls a job's hadoop conf from the RM.
func (jt *jobTracker) fetchConf(id string) (map[string]string, error) {
	appID, jobID := hadoopIDs(id)
	url := fmt.Sprintf("%s/proxy/%s/ws/v1/mapreduce/jobs/%s/conf", jt.rm, appID, jobID)
	confResp := &confResp{}
	if _, err := getJSON(url, confResp); err != nil {
		return nil, err
	}

	conf := make(map[string]string, len(confResp.Conf.Property))
	for _, property := range confResp.Conf.Property {
		conf[property.Name] = property.Value
	}

	return conf, nil
}
