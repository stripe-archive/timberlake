FROM golang:1.6

ADD . /go/src/github.com/stripe/timberlake
RUN mkdir -p /build/
WORKDIR /go/src/github.com/stripe/timberlake
CMD /go/src/github.com/stripe/timberlake/jenkins_build.sh
