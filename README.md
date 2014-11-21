# Timberlake is a Job Tracker for Hadoop.

* [Intro](#intro)
* [Screenshots](#screenshots)
* [Installation](#installation)

## Intro

Timberlake is a Go server paired with a React.js frontend. It improves on
existing Hadoop job trackers by providing a lightweight realtime view of your
running and finished MapReduce jobs. Timberlake exposes the counters and
configuration that we use the most at Stripe: we can get a quick overview of the
whole cluster and we can dig into the performance and behavior of a single job.

Timberlake visualizes your job's map and reduce tasks as a waterfall and as
boxplots. These visualizations help us see why a job is slow: is it launching
too many mappers and overloading the cluster? Are reducers launching too early
and starving the mappers? Does the job have reducer skew? We also use the
counters of bytes written, shuffled, and read to understand the network and I/O
behavior of our jobs. When your jobs fail, Timberlake will show you the key
parts of the logs that will help you debug your job.

Timberlake pairs well with Scalding and Cascading. It uses extra data from the
Cascading planner to show the relationships between flow steps: it's clear which
jobs are used as input to other jobs and how the whole flow performed.
Visualizing the flow has helped us to find which steps in the flow are causing
bottlenecks.

We've also included a Slackbot that has significantly improved our Hadooping
lives. The bot can notify you when your jobs start and finish, and provides
links back to timberlake.


## Screenshots

#### Job Details
![Job Details](https://cloud.githubusercontent.com/assets/57258/5137475/f4972946-70ee-11e4-9040-08a905ce8842.png)

#### List of Jobs
![List of Jobs](https://cloud.githubusercontent.com/assets/57258/5137476/f755b92c-70ee-11e4-8d6f-6819e5035529.png)


## Installation

1. Clone the source.

    git clone https://github.com/stripe/timberlake.git

2. Run make.

    make

3. Start the server.

    ./timberlake --bind :8000 \
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
