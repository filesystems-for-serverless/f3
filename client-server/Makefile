IMAGENAME ?= transfer-client-server
REGISTRY ?= kubes1:5000/f3
IMAGE_LATEST ?= $(REGISTRY)/$(IMAGENAME):latest
#image: kubes1:5000/f3/transfer-client-server:latest

all: client
	go build src/server/server.go

.PHONY: client
client:
	go build src/client/client.go
	kubectl cp client dev-pod-1:/client
	kubectl cp client dev-pod-2:/client

.PHONY: container
container:
	sudo docker build --no-cache

do-containerized-build:

containerized-build:
	sudo docker run --rm -v$(CURDIR):/workdir golang:1.16.5-alpine3.14 /bin/sh /workdir/do-containerized-build.sh
