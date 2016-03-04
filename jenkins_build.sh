#!/bin/sh

set -eux

cd /go/src/github.com/stripe/timberlake
make

bin/timberlake -help 2>&1 | grep Usage && echo 'binary looks good'
bin/timberlake-slackbot -help 2>&1 | grep Usage && echo 'binary looks good'

mkdir -p /build
cp -r bin static index.html /build/
echo "DONE"
