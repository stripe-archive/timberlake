package main

import (
	"encoding/json"
	"flag"
	"github.com/zenazn/goji/bind"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

var clusterNames = flag.String("cluster-name", "default", "The user-visible names for the clusters")
var resourceManagerURL = flag.String("resource-manager-url", "http://localhost:8088", "The HTTP URL to access the resource manager.")
var historyServerURL = flag.String("history-server-url", "http://localhost:19888", "The HTTP URL to access the history server.")
var proxyServerURL = flag.String("proxy-server-url", "", "The HTTP URL to access the proxy server, if separate from the resource manager.")
var namenodeAddress = flag.String("namenode-address", "localhost:9000", "The host:port to access the Namenode metadata service.")
var yarnLogDir = flag.String("yarn-logs-dir", "/tmp/logs", "The HDFS path where YARN stores logs. This is the controlled by the hadoop property yarn.nodemanager.remote-app-log-dir.")
var yarnHistoryDir = flag.String("yarn-history-dir", "/tmp/staging/history/done", "The HDFS path where YARN stores finished job history files. This is the controlled by the hadoop property mapreduce.jobhistory.done-dir.")
var httpTimeout = flag.Duration("http-timeout", time.Second*2, "The timeout used for connecting to YARN API. Pass values like: 2s")
var pollInterval = flag.Duration("poll-interval", time.Second*5, "How often should we poll the job APIs. Pass values like: 2s")
var enableDebug = flag.Bool("pprof", false, "Enable pprof debugging tools at /debug.")
var s3BucketName = flag.String("s3-bucket", "", "S3 bucket to fetch old jobs from")
var s3Region = flag.String("s3-region", "", "AWS region for the job storage S3 bucket")
var s3JobsPrefix = flag.String("s3-jobs-prefix", "", "S3 key prefix (\"folder\") where jobs are stored")
var s3FlowPrefix = flag.String("s3-flow-prefix", "", "S3 key prefix (\"folder\") where cascading flows are stored")

var jts map[string]*jobTracker
var persistedJobClient PersistedJobClient

var rootPath, staticPath string

var mux *web.Mux

func init() {
	bind.WithFlag()
	mux = web.New()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.Logger)
	mux.Use(middleware.Recoverer)
	mux.Use(middleware.AutomaticOptions)
}

func index(c web.C, w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(rootPath, "index.html"))
}

func getJobs(c web.C, w http.ResponseWriter, r *http.Request) {
	// We only need the details for listing pages.
	var jobs []*job
	for clusterName, tracker := range jts {
		for _, j := range tracker.jobs {
			jobs = append(jobs, &job{
				Cluster: tracker.clusterName,
				Details: j.Details,
				conf: conf{
					Input:         j.conf.Input,
					Output:        j.conf.Output,
					ScaldingSteps: j.conf.ScaldingSteps,
					name:          j.conf.name,
				},
			})
		}
		log.Printf("Appending %d jobs for Cluster %s: %s %s\n", len(jobs), clusterName, tracker.hs, tracker.rm)
	}

	jsonBytes, err := json.Marshal(jobs)
	if err != nil {
		log.Println("getJobs error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getNumClusters(c web.C, w http.ResponseWriter, r *http.Request) {
	jsonBytes, err := json.Marshal(len(jts))
	if err != nil {
		log.Println("getNumbClusters error:", err)
		w.WriteHeader(500)
		return
	}
	w.Write(jsonBytes)
}

func getConf(c web.C, w http.ResponseWriter, r *http.Request) {
	id := c.URLParams["id"]
	log.Printf("Getting job conf for %s", id)

	job := getJob(id)
	if job == nil {
		w.WriteHeader(404)
		return
	}

	jsonBytes, err := json.Marshal(jobConf{
		Conf: job.conf,
		ID:   job.Details.ID,
		Name: job.Details.Name,
	})
	if err != nil {
		log.Println("could not marshal:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getJob(rawJobID string) *job {
	// check if we have it in memory
	for clusterName, jt := range jts {
		if _, ok := jt.jobs[jobID(rawJobID)]; ok {
			job := jt.reifyJob(rawJobID)
			job.Cluster = clusterName
			return job
		}
	}

	// check if we have it in long-term storage
	job, _ := persistedJobClient.FetchJob(rawJobID)

	return job
}

func getJobAPIHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	job := getJob(c.URLParams["id"])

	if job == nil {
		w.WriteHeader(404)
		return
	}

	jsonBytes, err := json.Marshal(job)
	if err != nil {
		log.Println("error serializing job:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getJobIdsAPIHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	jobIds, err := persistedJobClient.FetchFlowJobIds(c.URLParams["flowID"])
	if err != nil {
		log.Println("FetchFlowJobIds error:", err)
		w.WriteHeader(500)
		return
	}

	jsonBytes, err := json.Marshal(jobIds)
	if err != nil {
		log.Println("JSON marshal error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func killJob(c web.C, w http.ResponseWriter, r *http.Request) {
	id := c.URLParams["id"]
	jobID := jobID(id)

	for _, jt := range jts {
		if _, ok := jt.jobs[jobID]; ok {
			err := jt.killJob(id)
			if err != nil {
				log.Println("killJob error: ", err)
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(204)
			return
		}
	}

	w.WriteHeader(404)
}

func init() {
	binPath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	rootPath = filepath.Join(binPath, "..")
	staticPath = filepath.Join(rootPath, "static")
}

func main() {
	flag.Parse()

	var clusterNames = strings.Split(*clusterNames, ",")
	var resourceManagerURLs = strings.Split(*resourceManagerURL, ",")
	var historyServerURLs = strings.Split(*historyServerURL, ",")
	var proxyServerURLs = strings.Split(*proxyServerURL, ",")
	var namenodeAddresses = strings.Split(*namenodeAddress, ",")

	if len(resourceManagerURLs) != len(historyServerURLs) {
		log.Fatal("resource-manager-url and history-server-url are not 1:1")
	}
	if !reflect.DeepEqual(proxyServerURLs, []string{""}) && len(proxyServerURLs) != len(resourceManagerURLs) {
		log.Fatal("proxy-server-url exists and is not 1:1 with resource-manager-url")
	}
	if len(resourceManagerURLs) != len(namenodeAddresses) {
		log.Fatal("resource-manager-url and namenode-address are not 1:1")
	}
	if len(resourceManagerURLs) != len(clusterNames) {
		log.Fatal("cluster-names and resource-manager-url are not 1:1")
	}

	persistedJobClient = NewS3JobClient(*s3Region, *s3BucketName, *s3JobsPrefix, *s3FlowPrefix)
	jts = make(map[string]*jobTracker)
	for i := range resourceManagerURLs {
		var proxyServerURL string
		if reflect.DeepEqual(proxyServerURLs, []string{""}) {
			proxyServerURL = resourceManagerURLs[i]
		} else {
			proxyServerURL = proxyServerURLs[i]
		}
		log.Printf("Creating new JT [%d]: %s %s %s\n", i, resourceManagerURLs[i], historyServerURLs[i], proxyServerURL)
		jts[clusterNames[i]] = newJobTracker(
			clusterNames[i],
			newRecentJobClient(
				resourceManagerURLs[i],
				historyServerURLs[i],
				proxyServerURL,
				namenodeAddresses[i],
			),
		)
	}

	log.Println("initiating JT loop")

	for clusterName, jt := range jts {
		go jt.Loop()
		if err := jt.testLogsDir(); err != nil {
			log.Printf("WARNING: Could not read yarn logs directory for cluster %s. Error message: `%s`\n", clusterName, err)
			log.Println("\tYou can change the path with --yarn-logs-dir=HDFS_PATH.")
			log.Println("\tTo talk to HDFS, Timberlake needs to be able to access the namenode (--namenode-address) and datanodes.")
		}
	}

	sse := newSSE()
	go sse.Loop()

	static := http.StripPrefix("/static/", http.FileServer(http.Dir(staticPath)))
	log.Println("serving static files from", staticPath)

	mux.Get("/static/*", static)
	mux.Get("/", index)
	mux.Get("/jobs/", getJobs)
	mux.Get("/numClusters/", getNumClusters)
	mux.Get("/sse", sse)
	mux.Get("/jobIds/:flowID", getJobIdsAPIHandler)
	mux.Get("/jobs/:id", getJobAPIHandler)
	mux.Get("/jobs/:id/conf", getConf)
	mux.Post("/jobs/:id/kill", killJob)

	if *enableDebug {
		mux.Get("/debug/pprof/*", pprof.Index)
		mux.Get("/debug/pprof/cmdline", pprof.Cmdline)
		mux.Get("/debug/pprof/profile", pprof.Profile)
		mux.Get("/debug/pprof/symbol", pprof.Symbol)
		mux.Get("/debug/pprof/trace", pprof.Trace)
	}

	for _, jt := range jts {
		go jt.sendUpdates(sse)
	}

	http.Serve(bind.Default(), mux)
}
