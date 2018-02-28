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
	data := &S3JobDetail{}
	err = json.Unmarshal(jsonBytes, data)
	if err != nil {
		return nil, err
	}

	// handle the translating to be consistent with job history server
	return s3responseToJob(data), nil
}
