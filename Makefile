TRAVIS_TAG ?= $(shell git describe --tags HEAD)
TIMBERLAKE_VERSION ?= $(TRAVIS_TAG)-$(shell go env GOOS)-$(shell go env GOARCH)
RELEASE_NAME = timberlake-$(TIMBERLAKE_VERSION)

all: build

build: timberlake static

release: clean build
	mkdir -p $(RELEASE_NAME)
	cp -r timberlake static README.md LICENSE.txt $(RELEASE_NAME)/
	tar -cvzf $(RELEASE_NAME).tar.gz $(RELEASE_NAME)

timberlake:
	go get -v github.com/tools/godep
	$(GOPATH)/bin/godep go build -v

node_modules:
	npm install

static: node_modules
	node_modules/.bin/gulp build

clean:
	rm -f timberlake timberlake-*.tar.gz
	rm -rf static
	rm -rf node_modules
	rm -rf $(RELEASE_NAME)

.PHONY: clean release
