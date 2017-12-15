package main

import (
	"encoding/xml"
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

type parsedJobConf struct {
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

	parsed := parsedJobConf{}
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
