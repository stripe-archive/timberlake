package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var internalURL = flag.String("internal-timberlake-url", "http://localhost:8000", "The internal HTTP URL the bot should use to access timberlake.")
var timberlakeURL = flag.String("external-timberlake-url", "https://timberlake.example.com", "The external URL the bot should display in messages.")
var slackURL = flag.String("slack-url", "https://hooks.slack.com/services/UNICORN", "The Slack URL to POST webhooks.")

type Job struct {
	Details struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		User       string `json:"user"`
		State      string `json:"state"`
		StartTime  int64  `json:"startTime"`
		FinishTime int64  `json:"finishTime"`
	} `json:"details"`
}

type SlackMessage struct {
	Text      string `json:"text"`
	Emoji     string `json:"icon_emoji"`
	LinkNames bool   `json:"link_names"`
	Username  string `json:"username"`
}

var running = make(map[string]bool, 0)
var finished = make(map[string]bool, 0)

var finishedStates = []string{"SUCCEEDED", "FAILED", "KILLED"}

func listen(events chan<- Job) {
	defer close(events)
	backoff := -2
	for {
		backoff = int(math.Min(float64(backoff+2), 10))
		if backoff > 0 {
			log.Println("Backing off. Will retry after", backoff, "seconds.")
			time.Sleep(time.Second * time.Duration(backoff))
		}

		url := strings.TrimRight(*internalURL, "/") + "/sse"
		log.Println("Getting", url)
		resp, err := http.Get(url)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Got a", resp.Status)
		log.Println("Scanning for events.")
		backoff = 0

		var buf string

		prefix := "data: "
		reader := bufio.NewReader(resp.Body)
		readline, err := reader.ReadString('\n')
		for err == nil {
			line := string(readline)
			if strings.Index(line, prefix) == 0 {
				buf += line[len(prefix):]
			} else {
				job := &Job{}
				err := json.Unmarshal([]byte(strings.Trim(buf, "\n")), job)
				if err != nil {
					log.Println(err)
				}
				buf = ""
				events <- *job
			}
			readline, err = reader.ReadString('\n')
		}
		fmt.Println("Scanner closed unexpectedly:", err)
		resp.Body.Close()
	}
}

func listener(events <-chan Job) {
	for job := range events {
		id := job.Details.ID
		if job.Details.User == "root" {
			continue
		}
		if !isFinished(job) {
			if !running[id] {
				running[id] = true
				// If it started in the last 30s, notify someone.
				if job.Details.StartTime/1000 > time.Now().Unix()-30 {
					go alert(job)
				} else {
					s := job.Details.StartTime
					t := time.Unix(s/1000, 0)
					log.Println("new-to-me running job, but not recent", job.Details.Name, s, t)
				}
			}
		} else {
			if !finished[id] {
				finished[id] = true
				// If it finished in the last 30s, notify someone.
				if job.Details.FinishTime/1000 > time.Now().Unix()-30 {
					go alert(job)
				} else {
					s := job.Details.FinishTime
					t := time.Unix(s/1000, 0)
					log.Println("new-to-me finished job, but not recent", job.Details.Name, s, t)
				}
			}
		}
	}
}

var emoji = map[string]string{
	"RUNNING":   "rocket zap bangbang bowtie triumph pray dancer sunny hatching_chick fireworks construction steam_locomotive birthday lollipop tophat ribbon surfer snowboarder postal_horn satellite octopus penguin panda_face pig koala frog bear muscle v",
	"SUCCEEDED": "+1 tada smile sunglasses star star2 sparkles dizzy ok_hand clap metal chart_with_upwards_trend crown ribbon rainbow stars checkered_flag",
	"KILLED":    "hocho skull poop no_good speak_no_evil ghost gun confounded scream_cat",
	"FAILED":    "boom fire disappointed -1 poop see_no_evil bomb no_good pouting_cat whale chart_with_downwards_trend",
}

func alert(job Job) {
	state := job.Details.State
	var msg string
	switch state {
	case "RUNNING":
		msg = "is *running*"
	case "SUCCEEDED":
		msg = "finished *successfully*"
	case "KILLED":
		msg = "was *killed*"
	case "FAILED":
		msg = "*crashed*"
	default:
		log.Println("Don't know how to deal with state:", state)
		return
	}
	url := strings.TrimRight(*timberlakeURL, "/") + "#/job/" + job.Details.ID
	name := cleanName(job.Details.Name) // TODO: strip bogus chars
	emojis := strings.Split(emoji[state], " ")
	e := emojis[rand.Int()%len(emojis)]
	text := fmt.Sprintf("@%s <%s|%s> %s :%s:", job.Details.User, url, name, msg, e)
	log.Println(text)

	slack := SlackMessage{
		Text:      text,
		Emoji:     ":elephant:",
		Username:  "timberlake",
		LinkNames: true,
	}

	jsonBytes, err := json.Marshal(slack)
	if err != nil {
		fmt.Println(err)
		return
	}

	resp, err := http.Post(*slackURL, "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(resp)
}

func cleanName(name string) string {
	re := regexp.MustCompile(`\[[A-Z0-9/]+\]\s+`)
	name = re.ReplaceAllString(name, "")

	re = regexp.MustCompile(`(\w+\.)+(\w+)`)
	name = re.ReplaceAllString(name, "$1$2")

	name = strings.Replace(name, ">", "&gt;", -1)
	name = strings.Replace(name, "<", "&lt;", -1)

	return name
}

func isFinished(job Job) bool {
	for _, state := range finishedStates {
		if state == job.Details.State {
			return true
		}
	}
	return false
}

func main() {
	flag.Parse()
	events := make(chan Job, 0)
	go listen(events)
	listener(events)
}
