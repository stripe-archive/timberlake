package main

import (
	"encoding/json"
	"flag"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var resourceManagerURL = flag.String("resource-manager-url", "http://localhost:8088", "The HTTP URL to access the resource manager.")
var historyServerURL = flag.String("history-server-url", "http://localhost:19888", "The HTTP URL to access the history server.")
var proxyServerURL = flag.String("proxy-server-url", "", "The HTTP URL to access the proxy server, if separate from the resource manager.")
var namenodeAddress = flag.String("namenode-address", "localhost:9000", "The host:port to access the Namenode metadata service.")
var yarnLogDir = flag.String("yarn-logs-dir", "/tmp/logs", "The HDFS path where YARN stores logs. This is the controlled by the hadoop property yarn.nodemanager.remote-app-log-dir.")
var yarnHistoryDir = flag.String("yarn-history-dir", "/tmp/staging/history/done", "The HDFS path where YARN stores finished job history files. This is the controlled by the hadoop property mapreduce.jobhistory.done-dir.")
var httpTimeout = flag.Duration("http-timeout", time.Second*2, "The timeout used for connecting to YARN API. Pass values like: 2s")
var pollInterval = flag.Duration("poll-interval", time.Second*2, "How often should we poll the job APIs. Pass values like: 2s")

var jt *jobTracker

var rootPath, staticPath string

func index(c web.C, w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(rootPath, "index.html"))
}

func getJobs(c web.C, w http.ResponseWriter, r *http.Request) {
	// We only need the details for listing pages.
	var jobs []*job
	for _, j := range jt.jobs {
		jobs = append(jobs, &job{Details: j.Details, Conf: j.Conf})
	}

	jsonBytes, err := json.Marshal(jobs)
	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getJob(c web.C, w http.ResponseWriter, r *http.Request) {
	if !jt.hasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	job := jt.reifyJob(c.URLParams["id"])

	jsonBytes, err := json.Marshal(job)
	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getLogs(c web.C, w http.ResponseWriter, r *http.Request) {
	if !jt.hasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	lines, err := jt.fetchLogs(c.URLParams["id"])

	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(500)
		return
	}

	jsonBytes, err := json.Marshal(lines)
	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func killJob(c web.C, w http.ResponseWriter, r *http.Request) {
	if !jt.hasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	id := c.URLParams["id"]
	err := jt.killJob(id)
	if err != nil {
		log.Println("error: ", err)
		w.WriteHeader(500)
	}
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

	if *proxyServerURL == "" {
		proxyServerURL = resourceManagerURL
	}

	jt = newJobTracker(*resourceManagerURL, *historyServerURL, *proxyServerURL)
	go jt.Loop()

	if err := jt.testLogsDir(); err != nil {
		log.Printf("WARNING: Could not read yarn logs directory. Error message: `%s`\n", err)
		log.Println("\tYou can change the path with --yarn-logs-dir=HDFS_PATH.")
		log.Println("\tTo talk to HDFS, Timberlake needs to be able to access the namenode (--namenode-address) and datanodes.")
	}

	sse := newSSE()
	go sse.Loop()

	static := http.StripPrefix("/static/", http.FileServer(http.Dir(staticPath)))
	log.Println("serving static files from", staticPath)

	goji.Get("/static/*", static)
	goji.Get("/", index)
	goji.Get("/jobs/", getJobs)
	goji.Get("/sse", sse)
	goji.Get("/jobs/:id", getJob)
	goji.Get("/jobs/:id/logs", getLogs)
	goji.Post("/jobs/:id/kill", killJob)

	go func() {
		for job := range jt.updates {
			jsonBytes, err := json.Marshal(job)
			if err != nil {
				log.Println("json error: ", err)
			} else {
				sse.events <- jsonBytes
			}
		}
	}()

	goji.Serve()
}
