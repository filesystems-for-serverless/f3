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
COPY libfuse/example /libfuse/example
COPY libfuse/build/lib/libfuse3.a /libfuse/libfuse3.a
COPY libfuse/include /libfuse/include
COPY libfuse/faas /libfuse/faas
RUN apt update && apt install -y build-essential
#RUN g++ /libfuse/example/passthrough_hp.cc /libfuse/libfuse3.a -o /libfuse/fusetest \
#	-I/libfuse/include -Wall -static -lrt -ldl -pthread \
#	-Wl,--whole-archive -lpthread -Wl,--no-whole-archive
#RUN gcc -Wall /libfuse/faas/f3.c /libfuse/faas/uds_client.cc /libfuse/libfuse3.a -I/libfuse/faas/ -I/libfuse/include -o /libfuse/fusetest -pthread -ldl
RUN g++ -Wall /libfuse/faas/f3.cc /libfuse/faas/uds_client.cc /libfuse/libfuse3.a -I/libfuse/faas/ -I/libfuse/include -o /libfuse/fusetest -pthread -ldl

#FROM k8s.gcr.io/build-image/debian-base-${ARCH}:v2.1.3
FROM ubuntu

# Copy f3plugin from build _output directory
COPY bin/f3plugin /f3plugin
COPY --from=builder /libfuse/fusetest /fusetest

RUN apt update && apt install ca-certificates mount -y || true

ENTRYPOINT ["/f3plugin"]
