ID_TRANSFER_IMAGENAME ?= transfer-client-server
F3_CSI_IMAGENAME ?= f3-csi
REGISTRY ?= kubes1:5000/f3
F3_CSI_IMAGE_LATEST ?= $(REGISTRY)/$(F3_CSI_IMAGENAME):latest
ID_TRANSFER_IMAGE_LATEST ?= $(REGISTRY)/$(ID_TRANSFER_IMAGENAME):latest

CSI_DEPLOYMENT_NAME ?= csi-f3-controller
CSI_DAEMONSET_NAME ?= csi-f3-node

CSI_DRIVER_BIN = csi/bin/f3plugin
FUSE_DRIVER_BIN = fuse/f3-fuse-driver
BINS = $(CSI_DRIVER_BIN) $(FUSE_DRIVER_BIN)

.PHONY: deploy-csi
deploy-csi:
	make -C csi local-k8s-install

.PHONY: update-drivers
update-drivers:
	kubectl rollout restart deployment $(CSI_DEPLOYMENT_NAME)
	kubectl rollout restart daemonset $(CSI_DAEMONSET_NAME)

.PHONY: csi-container
csi-container: build-timestamp

build-timestamp: $(CSI_DRIVER_BIN) $(FUSE_DRIVER_BIN)
	sudo docker build --no-cache -t $(F3_CSI_IMAGE_LATEST) .
	sudo docker push $(F3_CSI_IMAGE_LATEST)
	make update-drivers
	date +%s > $@

rebuild-csi-container: fuse-driver csi-driver

.PHONY: id-transfer-container
id-transfer-container:
	sudo docker build --no-cache -t $(ID_TRANSFER_IMAGE_LATEST) client-server
	sudo docker push $(ID_TRANSFER_IMAGE_LATEST)

$(CSI_DRIVER_BIN): csi-driver
.PHONY: csi-driver
csi-driver:
	make -C csi

$(FUSE_DRIVER_BIN): fuse-driver
.PHONY: fuse-driver
fuse-driver:
	make -C fuse

.PHONY: ow
ow:
	make -C openwhisk
