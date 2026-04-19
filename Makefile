CONTROLLER_GEN ?= $(shell which controller-gen)
GOPATH ?= $(shell go env GOPATH)

.PHONY: all build test lint vet generate manifests

all: build

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

generate:
	$(CONTROLLER_GEN) object:headerFile="" paths="./operator/api/..."

manifests:
	$(CONTROLLER_GEN) crd paths="./operator/api/..." output:crd:artifacts:config=deploy/crds
