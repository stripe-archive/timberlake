FROM golang:1.6

# Install node
RUN if [ -e /usr/stripe/bin/docker/stripe-install-node ]; then /usr/stripe/bin/docker/stripe-install-node 4.3.1; else \
  curl --silent --location https://nodejs.org/download/release/v4.3.1/node-v4.3.1-linux-x64.tar.gz > /tmp/node.tar.gz \
  && echo "b3af1ed18a9150af42754e9a0385ecc4b4e9b493fcf32bf6ca0d7239d636254b ?/tmp/node.tar.gz" | shasum -p -a 256 -c \
  && tar --directory=/usr/local/ --strip-components=1 -xzf /tmp/node.tar.gz; fi

ADD . /go/src/github.com/stripe/timberlake
RUN mkdir -p /build/
WORKDIR /go/src/github.com/stripe/timberlake
CMD /go/src/github.com/stripe/timberlake/jenkins_build.sh
