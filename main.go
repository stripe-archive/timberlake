package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/zenazn/goji/bind"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

var resourceManagerURL = flag.String("resource-manager-url", "http://localhost:8088", "The HTTP URL to access the resource manager.")
var historyServerURL = flag.String("history-server-url", "http://localhost:19888", "The HTTP URL to access the history server.")
var proxyServerURL = flag.String("proxy-server-url", "", "The HTTP URL to access the proxy server, if separate from the resource manager.")
var namenodeAddress = flag.String("namenode-address", "localhost:9000", "The host:port to access the Namenode metadata service.")
var yarnLogDir = flag.String("yarn-logs-dir", "/tmp/logs", "The HDFS path where YARN stores logs. This is the controlled by the hadoop property yarn.nodemanager.remote-app-log-dir.")
var yarnHistoryDir = flag.String("yarn-history-dir", "/tmp/staging/history/done", "The HDFS path where YARN stores finished job history files. This is the controlled by the hadoop property mapreduce.jobhistory.done-dir.")
var httpTimeout = flag.Duration("http-timeout", time.Second*2, "The timeout used for connecting to YARN API. Pass values like: 2s")
var pollInterval = flag.Duration("poll-interval", time.Second*30, "How often should we poll the job APIs. Pass values like: 2s")
var enableDebug = flag.Bool("pprof", false, "Enable pprof debugging tools at /debug.")

var jt *jobTracker

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
	for _, j := range jt.jobs {
		jobs = append(jobs, &job{Details: j.Details, Conf: conf{
			Input:         j.Conf.Input,
			Output:        j.Conf.Output,
			ScaldingSteps: j.Conf.ScaldingSteps,
			name:          j.Conf.name,
		}})
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

	jt = newJobTracker(*resourceManagerURL, *historyServerURL, *proxyServerURL, *namenodeAddress)
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

	mux.Get("/static/*", static)
	mux.Get("/", index)
	mux.Get("/jobs/", getJobs)
	mux.Get("/sse", sse)
	mux.Get("/jobs/:id", getJob)
	mux.Post("/jobs/:id/kill", killJob)

	if *enableDebug {
		mux.Get("/debug/pprof/*", pprof.Index)
		mux.Get("/debug/pprof/cmdline", pprof.Cmdline)
		mux.Get("/debug/pprof/profile", pprof.Profile)
		mux.Get("/debug/pprof/symbol", pprof.Symbol)
		mux.Get("/debug/pprof/trace", pprof.Trace)
	}

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

	http.Serve(bind.Default(), mux)
}
