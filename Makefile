# MDMP Makefile

GOCMD = GO111MODULE=on go

VERSION := $(shell grep "const Version " pkg/version/version.go | sed -E 's/.*"(.+)"$$/\1/')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)
BUILD_DATE=$(shell date '+%Y-%m-%d-%H:%M:%S')
GORUN = $(GOCMD) run -ldflags "-X git.internal.yunify.com/MDMP2/metadata/pkg/version.GitCommit=${GIT_COMMIT}${GIT_DIRTY} -X git.internal.yunify.com/MDMP2/metadata/pkg/version.BuildDate=${BUILD_DATE}"
GOBUILD = $(GOCMD) build -ldflags "-X git.internal.yunify.com/MDMP2/metadata/pkg/version.GitCommit=${GIT_COMMIT}${GIT_DIRTY} -X git.internal.yunify.com/MDMP2/metadata/pkg/version.BuildDate=${BUILD_DATE}"
GOTEST = $(GOCMD) test
BINNAME = metadata
BINNAMES = metadata

DOCKERTAG?=tkeelio/metadata:dev

run:
	@echo "---------------------------"
	@echo "-         Run             -"
	@echo "---------------------------"
	@$(GORUN) . serve  --conf conf/metadata-example.toml

build:
	@rm -rf bin/
	@mkdir bin/
	@echo "---------------------------"
	@echo "-        build...         -"
	@$(GOBUILD)    -o bin/metadata
	@echo "-     build(linux)...     -"
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64  $(GOBUILD) -o bin/linux/metadata
	@echo "-    builds completed!    -"
	@echo "---------------------------"
	@bin/metadata version
	@echo "-----------Done------------"

test:
	@$(GOTEST) git.internal.yunify.com/MDMP2/metadata/pkg/...
	@$(GOTEST) git.internal.yunify.com/MDMP2/metadata/internal/...

docker-build:
	docker build -t $(DOCKERTAG) .
docker-push:
	docker push $(DOCKERTAG)
docker-auto:
	docker build -t $(DOCKERTAG) .
	docker push $(DOCKERTAG)


.PHONY: install generate

-include .dev/*.makefile
