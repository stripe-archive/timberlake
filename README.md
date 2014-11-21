# Timberlake is a Job Tracker for Hadoop.

* [Intro](#intro)
* [Screenshots](#screenshots)
* [Installation](#installation)
* [Limitations](#limitations)

## Intro

Timberlake is a Go server paired with a React.js frontend. It improves on
existing Hadoop job trackers by providing a lightweight realtime view of your
running and finished MapReduce jobs. Timberlake exposes the counters and
configuration that are the most useful, so you can get a quick overview of
the whole cluster or dig into the performance and behavior of a single job.

It also provides waterfall and boxplot visualizations for jobs. We've found that
these  visualizations can be really helpful for figuring out why a job is too
slow. Is it launching too many mappers and overloading the cluster? Are reducers
launching too early and starving the mappers? Does the job have reducer skew?
You can the counters of bytes written, shuffled, and read to understand the
network and I/O behavior of our jobs. And when jobs fail, Timberlake will show
you tracebacks from the logs, to will help you debug the job.

Timberlake pairs well with Scalding and Cascading. It uses extra data from the
Cascading planner to show the relationships between steps, and to clarify which
jobs' outputs are used as inputs to other jobs in the flow. Visualizing that
flow makes it much easier to figure out which steps are causing bottlenecks.

Finally, we've also included a Slackbot that has significantly improved our
Hadooping lives. The bot can notify you when your jobs start and finish, and
provides links back to Timberlake.


## Screenshots

#### Job Details
![Job Details](https://cloud.githubusercontent.com/assets/57258/5138257/b65377fe-7100-11e4-89b9-13fbacf411b1.png)

#### List of Jobs
![List of Jobs](https://cloud.githubusercontent.com/assets/57258/5137476/f755b92c-70ee-11e4-8d6f-6819e5035529.png)


## Installation

The best way to install is with tarballs, which are available on the [release page](https://github.com/stripe/timberlake/releases).

Download it somewhere on your server, and then untar it:

    $ tar zxvf timberlake-v0.1.0-linux-amd64.tar.gz
    $ mv -T timberlake-v0.1.0-linux-amd64 /opt/timberlake

Now you can start the server:


    $ /opt/timberlake/bin/timberlake \
        --bind :8000 \
        --resource-manager-url http://resourcemanager:8088 \
        --history-server-url http://resourcemanager:19888 \
        --namenode-address namenode:9000

And optionally, start the Slackbot:

    $ /opt/timberlake/bin/slack \
        --internal-timberlake-url http://localhost:8000 \
        --external-timberlake-url https://timberlake.example.com \
        --slack-url https://hooks.slack.com/services/...

You'll need to create a new [Incoming Webhook](https://slack.com/services)
to generate the Slack URL for your bot.

## Building from Source

You'll need `npm` and `go` on your path.

    $ git clone https://github.com/stripe/timberlake.git
    $ cd timberlake
    $ make

## Limitations

Timberlake only works with the [YARN Resource
Manager API](https://hadoop.apache.org/docs/r2.5.2/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html). It's been tested on v2.4.x and v2.5.x, but the Kill Job feature uses an endpoint that's only avaiable in 2.5.x+.

Our cluster has 10-40 jobs running simultaneously and about 2,000 jobs running
per day. Timberlake's performance has not been tested outside these bounds.
