#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// C includes
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

// C++ includes
/*
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <mutex>
#include <fstream>
#include <thread>
#include <iomanip>

using namespace std;
*/


#define SOCK_PATH "/tmp/fuse-client.sock"

int download_file(const char *uds_path, char *path, char *servers) {
	struct sockaddr_un addr;
	int fd, ret;

	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		return -errno;
	}

	memset(&addr, 0, sizeof(addr));

	addr.sun_family = AF_UNIX;

	//strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path)-1);
	strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path)-1);

	printf("download_file: connecting\n");
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
		return -errno;
	}

	char buf[100];
	bzero(buf, 100);
	printf("servers: %s path: %s\n", servers, path);
	int len = snprintf(buf, sizeof(buf), "%s,%s\n", path, servers);
	if (len < 0) {
		return -errno;
	}

	ret = write(fd, buf, len);
	if (ret < 0) {
		return -errno;
	} else if (ret < len) {
		fprintf(stderr, "partial write %d %d\n", ret, len);
	}
	printf("Wrote >>>\n%s\n<<< (%d bytes)\n", buf, ret);
	memset(buf, 0, sizeof(buf));

	ret = read(fd, buf, sizeof(buf));
	if (ret < 0) {
		return -errno;
	}

	// TODO: check for ACK response
	printf("read %s (%d bytes)\n", buf, ret);

	return 0;
}

#ifdef CLIENT_ONLY
int main(int argc, char **argv) {
	if (argc < 3) {
		printf("usage: %s path servers", argv[0]);
		return 1;
	}

	download_file(argv[1], argv[2]);
	
	return 0;
}
#endif
