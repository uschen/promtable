PREFIX=/usr/local
# VERSION=$(shell git describe)-$(shell date -u +%Y%m%d.%H%M%S)
BUILD_DIR = $(shell pwd)
BUILD_GOPATH=${BUILD_DIR}/_build
PKG=github.com/uschen/promtable
REPO_PATH=$(PKG)

VERSION ?= $(shell ./scripts/git-version.sh)

LD_FLAGS="-extldflags -static -s -w -X $(REPO_PATH)/version.Version=$(VERSION)"


ci-env:
	$(eval GOPATH=${BUILD_GOPATH})

${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

ci-build: ci-clean-build $(BUILD_DIR)/_build ci-env
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a --installsuffix cgo --ldflags $(LD_FLAGS) -o ${BUILD_DIR}/nuorder ${BUILD_DIR}/_build/src/${PKG}/main.go
	chmod +x ${BUILD_DIR}/nuorder

ci-cloud-pre:
    go get -u github.com/kisielk/errcheck
    go get -u golang.org/x/lint/golint
    go get -u github.com/golang/dep/cmd/dep
    dep ensure

.PHONY: ci-test
ci-test: ci-clean-build ci-cloud-pre $(BUILD_DIR)/_build ci-env
	go fmt $$(go list ${PKG}/... | grep -v vendor/) | awk '{ print } END { if (NR > 0) { print "Please run go fmt"; exit 1 } }'
	golint $$(go list ${PKG}/... | grep -v vendor/)
	errcheck $$(go list ${PKG}/... | grep -v vendor/) | grep -v "defer " | grep -v "fmt.Fprint" | awk '{ print } END { if (NR > 0) { print "Please run errcheck"; exit 1 } }'
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test --installsuffix cgo --ldflags $(LD_FLAGS) -v $$(go list ${PKG}/... | grep -v vendor/)
	@echo SUCCESS

.PHONY:	ci-clean-build
ci-clean-build:
	rm -rf ${BUILD_DIR}/_build
