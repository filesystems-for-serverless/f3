SOURCES := f3.cc uds_client.cc

GCC_VERSION := $(shell g++ -dumpversion)
# Don't know the version we actually needs but at least 7 is definitely ok
GCC_VERSION_OK := $(shell expr "$(GCC_VERSION)" ">=" "7.0.0")

ifeq "$(GCC_VERSION_OK)" "1"
	CC := g++
else
	CC := scl enable devtoolset-8 -- g++
endif

all: f3-fuse-driver

f3-fuse-driver: $(SOURCES)
	$(CC) $(SOURCES) libfuse/build/lib/libfuse3.a -Wall -Ilibfuse/include -o f3-fuse-driver -pthread -ldl -DHAVE_SETXATTR -g
	#$(CC) $(SOURCES) libfuse/build/lib/libfuse3.a -Wall -Ilibfuse/include -o f3-fuse-driver -pthread -ldl

clean:
	rm f3-fuse-driver

copy-to-dev: f3-fuse-driver
	kubectl cp f3-fuse-driver dev-pod-1:/fuse/f3-fuse-driver
	kubectl cp f3-fuse-driver dev-pod-2:/fuse/f3-fuse-driver

.PHONY: all clean
