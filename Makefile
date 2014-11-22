TRAVIS_TAG ?= $(shell git describe --tags HEAD)
TIMBERLAKE_VERSION ?= $(TRAVIS_TAG)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = timberlake-$(TIMBERLAKE_VERSION)

all: build

build: bin/timberlake bin/slack static

release: clean build
	mkdir -p $(RELEASE_NAME)
	cp -r bin static README.md LICENSE $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

bin/timberlake:
	go get -v github.com/zenazn/goji
	go get -v github.com/colinmarc/hdfs
	go build -o bin/timberlake .

bin/slack:
	go build -o bin/slack bots/slack.go


static: node_modules
	node_modules/.bin/gulp build

node_modules:
	npm install

clean:
	rm -f timberlake timberlake-*.tar.gz
	rm -rf static/{js,css,img} bin node_modules
	rm -rf $(RELEASE_NAME)

.PHONY: clean build release
