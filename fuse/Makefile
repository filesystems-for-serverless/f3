all:
	g++ -Wall f3.cc uds_client.cc libfuse/build/lib/libfuse3.a -Ilibfuse/include -o f3-fuse-driver -pthread -ldl

clean:
	rm f3-fuse-driver
