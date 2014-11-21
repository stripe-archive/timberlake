TRAVIS_TAG ?= $(shell git describe --tags HEAD)
TIMBERLAKE_VERSION ?= $(TRAVIS_TAG)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = timberlake-$(TIMBERLAKE_VERSION)

all: build

build: timberlake static

release: clean build
	mkdir -p $(RELEASE_NAME)
	cp -r timberlake static README.md LICENSE $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

timberlake:
	go get -v github.com/zenazn/goji
	go get -v github.com/colinmarc/hdfs
	go build -v

static: node_modules
	node_modules/.bin/gulp build

node_modules:
	npm install

clean:
	rm -f timberlake timberlake-*.tar.gz
	rm -rf static
	rm -rf $(RELEASE_NAME)

.PHONY: clean build release
