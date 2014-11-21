# Timberlake is a Job Tracker for Hadoop.

* [Intro](#intro)
* [Screenshots](#screenshots)
* [Installation](#installation)
* [Limitations](#limitations)

## Intro

Timberlake is a Go server paired with a React.js frontend. It improves on
existing Hadoop job trackers by providing a lightweight realtime view of your
running and finished MapReduce jobs. Timberlake exposes the counters and
configuration that we use the most at Stripe. We can get a quick overview of the
whole cluster and we can dig into the performance and behavior of a single job.

Timberlake visualizes your job's map and reduce tasks as a waterfall and as
boxplots. These visualizations help us see why a job is slow. Is it launching
too many mappers and overloading the cluster? Are reducers launching too early
and starving the mappers? Does the job have reducer skew? We use the
counters of bytes written, shuffled, and read to understand the network and I/O
behavior of our jobs. And when jobs fail, Timberlake will show the key
parts of the logs that will help us debug the job.

Timberlake pairs well with Scalding and Cascading. It uses extra data from the
Cascading planner to show the relationships between steps, and to clarify which
jobs' outputs are used as inputs to other jobs in the flow. Visualizing that
flow makes it much easier to figure out which steps are causing bottlenecks.

We've also included a Slackbot that has significantly improved our Hadooping
lives. The bot can notify you when your jobs start and finish, and provides
links back to Timberlake.


## Screenshots

#### Job Details
![Job Details](https://cloud.githubusercontent.com/assets/57258/5138257/b65377fe-7100-11e4-89b9-13fbacf411b1.png)

#### List of Jobs
![List of Jobs](https://cloud.githubusercontent.com/assets/57258/5137476/f755b92c-70ee-11e4-8d6f-6819e5035529.png)


## Installation

1. Clone the source.

        git clone https://github.com/stripe/timberlake.git

2. Run make.

        make

The Makefile assumes that you've set up your
[`$GOPATH`](http://golang.org/doc/code.html) and will try to install
[`godep`](https://github.com/tools/godep) if it doesn't already exist.

3. Start the server.

        ./timberlake \
            --bind :8000 \
            --resource-manager-url http://bigdata:8088 \
            --history-server-url http://bigdata:19888 \
            --namenode-address http://bigdata:9000 \
            --root-log-dir /tmp/logs

4. Start the Slackbot (optional).

        ./slack \
            --internal-timberlake-url http://localhost:8000 \
            --external-timberlake-url https://timberlake.example.com \
            --slack-url https://hooks.slack.com/services/...

You'll need to create a new [Incoming Webhook](https://slack.com/services) to
generate the Slack URL for your bot.


# Limitations

Timberlake only works with the [YARN Resource
Manager API](https://hadoop.apache.org/docs/r2.5.2/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html). It's been tested on v2.4.x and v2.5.x, but the Kill Job feature uses an endpoint that's only avaiable in 2.5.x+.

Our cluster has 10-40 jobs running simultaneously and about 2,000 jobs running
per day. Timberlake's performance has not been tested outside these bounds.
