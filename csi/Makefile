# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DEPLOY_FOLDER = ./deploy
CMDS=f3plugin
#PKG = github.com/kubernetes-csi/csi-driver-f3
GINKGO_FLAGS = -ginkgo.v
GO111MODULE = on
GOPATH ?= $(shell go env GOPATH)
GOBIN ?= $(GOPATH)/bin
DOCKER_CLI_EXPERIMENTAL = enabled
export GOPATH GOBIN GO111MODULE DOCKER_CLI_EXPERIMENTAL

REV=$(shell git describe --long --tags --match='v*' --dirty 2>/dev/null || git rev-list -n1 HEAD)
IMPORTPATH_LDFLAGS = -X main.version=$(REV)
EXT_LDFLAGS = -extldflags "-static"
LDFLAGS =
FULL_LDFLAGS = $(LDFLAGS) $(IMPORTPATH_LDFLAGS) $(EXT_LDFLAGS)
GOFLAGS_VENDOR =

#include release-tools/build.make

GIT_COMMIT = $(shell git rev-parse HEAD)
BUILD_DATE = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
IMAGE_VERSION = v3.0.0
#LDFLAGS = -X ${PKG}/pkg/f3.driverVersion=${IMAGE_VERSION} -X ${PKG}/pkg/f3.gitCommit=${GIT_COMMIT} -X ${PKG}/pkg/f3.buildDate=${BUILD_DATE}
#EXT_LDFLAGS = -s -w -extldflags "-static"

# Use a custom version for E2E tests if we are testing in CI
ifdef CI
ifndef PUBLISH
override IMAGE_VERSION := e2e-$(GIT_COMMIT)
endif
endif

IMAGENAME ?= f3plugin
REGISTRY ?= kubes1:5000/f3
IMAGE_TAG = $(REGISTRY)/$(IMAGENAME):$(IMAGE_VERSION)
IMAGE_TAG_LATEST = $(REGISTRY)/$(IMAGENAME):latest
LOCAL_USER ?= kubes1:5000/f3

PLUGIN_BIN := ./bin/f3plugin
PLUGIN_CMD := ./cmd/f3plugin
SOURCES := pkg/f3/*.go $(PLUGIN_CMD)/main.go

all: f3

.PHONY: local-build-push
local-build-push: f3
	sudo docker build -t $(LOCAL_USER)/f3plugin:latest .
	sudo docker push $(LOCAL_USER)/f3plugin

.PHONY: local-k8s-install
local-k8s-install:
	echo "Instlling locally"
	kubectl apply -f $(DEPLOY_FOLDER)/rbac-csi-f3-controller.yaml
	kubectl apply -f $(DEPLOY_FOLDER)/csi-f3-driverinfo.yaml
	kubectl apply -f $(DEPLOY_FOLDER)/csi-f3-controller.yaml
	kubectl apply -f $(DEPLOY_FOLDER)/csi-f3-node.yaml
	kubectl apply -f $(DEPLOY_FOLDER)/f3-sc.yaml
	echo "Successfully installed"

.PHONY: local-k8s-uninstall
local-k8s-uninstall:
	echo "Uninstalling driver"
	kubectl delete -f $(DEPLOY_FOLDER)/csi-f3-controller.yaml --ignore-not-found
	kubectl delete -f $(DEPLOY_FOLDER)/csi-f3-node.yaml --ignore-not-found
	kubectl delete -f $(DEPLOY_FOLDER)/csi-f3-driverinfo.yaml --ignore-not-found
	kubectl delete -f $(DEPLOY_FOLDER)/rbac-csi-f3-controller.yaml --ignore-not-found
	kubectl delete -f $(DEPLOY_FOLDER)/f3-sc.yaml --ignore-not-found
	echo "Uninstalled F3 driver"

.PHONY: f3
f3: $(PLUGIN_BIN)
$(PLUGIN_BIN): $(SOURCES)
	CGO_ENABLED=0 GOOS= GOARCH= go build $(GOFLAGS_VENDOR) -a -ldflags '$(FULL_LDFLAGS)' -o $(PLUGIN_BIN) $(PLUGIN_CMD)

.PHONY: container
container: f3
	sudo docker build --no-cache -t $(IMAGE_TAG) .

.PHONY: push
push:
	sudo docker push $(IMAGE_TAG)

.PHONY: push-latest
push-latest:
	docker tag $(IMAGE_TAG) $(IMAGE_TAG_LATEST)
	docker push $(IMAGE_TAG_LATEST)

.PHONY: install-f3-server
install-f3-server:
	kubectl apply -f ./deploy/example/f3-provisioner/f3-server.yaml
