package main

import (
	"encoding/json"
	"flag"
	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
	"log"
	"net/http"
)

var resourceManagerURL = flag.String("resource-manager-url", "http://localhost:8088", "The HTTP URL to access the resource manager.")
var historyServerURL = flag.String("history-server-url", "http://localhost:19888", "The HTTP URL to access the history server.")
var namenodeAddress = flag.String("namenode-address", "localhost:9000", "The host:port to access the Namenode metadata service.")
var rootLogDir = flag.String("root-log-dir", "/tmp/logs", "The HDFS path where YARN stores logs. This is the controlled by the hadoop property yarn.nodemanager.remote-app-log-dir.")

var jt jobTracker

func index(c web.C, w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func getJobs(c web.C, w http.ResponseWriter, r *http.Request) {
	// We only need the details for listing pages.
	var jobs []*job
	for _, j := range jt.Jobs {
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
	if !jt.HasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	job := jt.GetJob(c.URLParams["id"])

	jsonBytes, err := json.Marshal(job)
	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(500)
		return
	}

	w.Write(jsonBytes)
}

func getLogs(c web.C, w http.ResponseWriter, r *http.Request) {
	if !jt.HasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	lines, err := jt.FetchLogs(c.URLParams["id"])

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
	if !jt.HasJob(c.URLParams["id"]) {
		w.WriteHeader(404)
		return
	}

	id := c.URLParams["id"]
	err := jt.KillJob(id)
	if err != nil {
		log.Println("error: ", err)
		w.WriteHeader(500)
	}
}

func main() {
	flag.Parse()

	jt = newJobTracker(*resourceManagerURL, *historyServerURL)
	go jt.Loop()

	sse := newSSE()
	go sse.Loop()

	static := http.StripPrefix("/static/", http.FileServer(http.Dir("build")))
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
