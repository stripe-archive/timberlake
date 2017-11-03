TRAVIS_TAG ?= $(shell git describe --tags HEAD)
TIMBERLAKE_VERSION ?= $(TRAVIS_TAG)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = timberlake-$(TIMBERLAKE_VERSION)

all: test build

test: node_modules
	npm run test
	golint -set_exit_status
	go test -race

build: bin/timberlake bin/timberlake-slackbot static

release: clean test build
	mkdir -p $(RELEASE_NAME)
	cp -r bin static index.html README.md LICENSE $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

bin/timberlake:
	go build -o bin/timberlake .

bin/timberlake-slackbot:
	go build -o bin/timberlake-slackbot bots/slack.go

static: node_modules
	node_modules/.bin/gulp build

node_modules:
	npm install

clean:
	rm -f timberlake timberlake-*.tar.gz
	rm -rf static bin node_modules
	rm -rf $(RELEASE_NAME)

.PHONY: clean test build release
