# Copyright 2020 The Kubernetes Authors.
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

ARG ARCH=amd64

FROM ubuntu AS builder
COPY fuse/libfuse/include /libfuse/include
COPY fuse/cxxopts.hpp fuse/f3.cc fuse/uds_client.cc fuse/libfuse/build/lib/libfuse3.a /libfuse/
RUN apt update && apt install -y build-essential
RUN g++ -Wall /libfuse/f3.cc /libfuse/uds_client.cc /libfuse/libfuse3.a -I/libfuse/faas/ -I/libfuse/include -o /libfuse/f3-fuse-driver -pthread -ldl -DHAVE_SETXATTR -g

#FROM k8s.gcr.io/build-image/debian-base-${ARCH}:v2.1.3
FROM ubuntu

# Copy f3plugin from build _output directory
COPY csi/bin/f3plugin /f3plugin
COPY --from=builder /libfuse/f3-fuse-driver /f3-fuse-driver

RUN apt update && apt install ca-certificates mount -y || true

ENTRYPOINT ["/f3plugin"]
