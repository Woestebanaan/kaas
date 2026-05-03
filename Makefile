CONTROLLER_GEN ?= $(shell which controller-gen)
BUF ?= $(shell which buf)
GOPATH ?= $(shell go env GOPATH)

.PHONY: all build test lint vet generate manifests helm-crds proto proto-tools

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

# manifests rebuilds CRD YAMLs from the operator/api/ Go types and
# mirrors them into the Helm chart's crds/ directory so a fresh
# `helm install` ships every CRD the operator + broker reference.
# Without the helm-crds step, KafkaClusterAssignments (Phase 1) and
# any new CRD will silently fail to install.
manifests: helm-crds-source
	$(MAKE) helm-crds

helm-crds-source:
	$(CONTROLLER_GEN) crd paths="./operator/api/..." output:crd:artifacts:config=deploy/crds

helm-crds:
	cp deploy/crds/skafka.io_*.yaml deploy/helm/skafka/crds/

# proto regenerates the gRPC stubs in pkg/heartbeatpb/ from proto/heartbeat.proto.
# Requires `buf` on PATH — install via `make proto-tools`.
# Phase 4 work; Phase 1 ships only the .proto schema.
proto:
	cd proto && $(BUF) generate

proto-tools:
	go install github.com/bufbuild/buf/cmd/buf@latest
