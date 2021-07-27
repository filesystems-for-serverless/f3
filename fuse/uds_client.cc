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

int setup_conn(const char *uds_path) {
	struct sockaddr_un addr;
	int fd;

	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		return -errno;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;

	strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path)-1);

	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
		return -errno;
	}

    printf("Connected to %s\n", uds_path);

    return fd;
}

long int download_file(int fd, char *path, char *servers, size_t end_byte) {
    int ret;

	char buf[100];
	bzero(buf, 100);
	//printf("servers: %s path: %s\n", servers, path);
	int len = snprintf(buf, sizeof(buf), "%s,%lu,%s\n", path, end_byte, servers);
	if (len < 0) {
		return -errno;
	}

	ret = write(fd, buf, len);
	if (ret < 0) {
		return -errno;
	} else if (ret < len) {
		fprintf(stderr, "partial write %d %d\n", ret, len);
	}
	//printf("Wrote >>>\n%s\n<<< (%d bytes)\n", buf, ret);

    bzero(buf, sizeof(buf));
    ret = read(fd, buf, sizeof(buf));
    if (ret < 0)
        return -errno;

    //printf("Got buf %s\n", buf);

    // NAK
    if (buf[0] == 'N') {
        printf("Got NAK, buf: %s\n", buf);
        return -1;
    }

    long int new_pos = strtol(buf+2, NULL, 10);
    //printf("ID client read up to %ld\n", new_pos);

	// TODO: check for ACK response
	//printf("read %s (%d bytes)\n", buf, ret);

	return new_pos;
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
