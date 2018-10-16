FROM golang:1.9

# Install node
RUN if [ -e /usr/stripe/bin/docker/stripe-install-node ]; then /usr/stripe/bin/docker/stripe-install-node 6.9.2; else \
  curl --silent --location https://nodejs.org/download/release/v6.9.2/node-v6.9.2-linux-x64.tar.gz > /tmp/node.tar.gz \
  && echo "cbf6a35b035c56f991c2e6a4aedbcd9f09555234ac0dd5b2c15128e2b5f4eb50 ?/tmp/node.tar.gz" | shasum -p -a 256 -c \
  && tar --directory=/usr/local/ --strip-components=1 -xzf /tmp/node.tar.gz; fi

ADD . /go/src/github.com/stripe/timberlake
RUN mkdir -p /build/
RUN go get -v golang.org/x/lint/golint
WORKDIR /go/src/github.com/stripe/timberlake
CMD /go/src/github.com/stripe/timberlake/jenkins_build.sh
