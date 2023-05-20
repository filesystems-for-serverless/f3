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
#include <fcntl.h>
#include <pthread.h>
#include <limits.h>

struct download_info {
    int fd;
    char *path;
    char *servers;
    size_t end_byte;
    int *download_done;
};

struct move_info {
    char *client_uds_path;
    char *old_path;
    char *new_path;
    char *servers;
};

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

int send_fname_created(int fd, char *fname) {
    int ret;
    char buf[PATH_MAX];
    int len = snprintf(buf, sizeof(buf), "XXX%s\n", fname);
    printf("Sending fname created %s\n", buf);
    ret = write(fd, buf, len);
    if (ret < 0) {
        perror("write");
        return -1;
    }
    printf("Wrote %d bytes\n", ret);

    return 0;
}

int send_fname_done(int fd, char *fname) {
    int ret;
    char buf[PATH_MAX];
    int len = snprintf(buf, sizeof(buf), "%s\n", fname);
    printf("Sending fname %s\n", buf);
    ret = write(fd, buf, len);
    if (ret < 0) {
        perror("write");
        return -1;
    }
    printf("Wrote %d bytes\n", ret);

    return 0;
}

long int download_file(int fd, char *path, char *servers, size_t end_byte) {
    int ret;

    char *buf = (char *)malloc(500);
    bzero(buf, 500);
    printf("downloading file!!!  servers: %s path: %s\n", servers, path);
    fflush(stdout);
    int len = snprintf(buf, 500, "download,%s,%lu,%s\n", path, end_byte, servers);
    if (len < 0) {
        perror("AAA snprintf");
        fflush(stderr);
        free(buf);
        return -errno;
    }

    printf("writing %d %s\n", len, buf);
    fflush(stdout);
    ret = write(fd, buf, len);
    if (ret < 0) {
        perror("AAA write");
        free(buf);
        return -errno;
    } else if (ret < len) {
        fprintf(stderr, "partial write %d %d\n", ret, len);
    }
    printf("Wrote >>>\n%s\n<<< (%d bytes)\n", buf, ret);

    bzero(buf, 500);
    ret = read(fd, buf, 500);
    if (ret < 0) {
        perror("AAA read");
        free(buf);
        return -errno;
    }

    printf("Got buf %s\n", buf);

    // NAK
    if (buf[0] == 'N') {
        printf("Got NAK, buf: %s\n", buf);
        free(buf);
        return -1;
    }

    long int new_pos = strtol(buf+2, NULL, 10);
    //printf("ID client read up to %ld\n", new_pos);

    // TODO: check for ACK response
    //printf("read %s (%d bytes)\n", buf, ret);

    free(buf);

    return new_pos;
}

void *download_file_thread(void *ptr) {
    struct download_info *dl_info = (struct download_info *)ptr;
    long int ret = download_file(dl_info->fd, dl_info->path, dl_info->servers, dl_info->end_byte);
    printf("Downloaded %ld bytes\n", ret);
    *(dl_info->download_done) = 1;

    free(dl_info->path);
    free(dl_info->servers);
    free(dl_info);

    return NULL;
}

long int inform_moved_file(int fd, char *old_path, char *new_path, char *servers) {
    int ret;

    char *buf = (char *)malloc(500);
    bzero(buf, 500);
    int len = snprintf(buf, 500, "move,%s,%s,%s\n", old_path, new_path, servers);
    if (len < 0) {
        perror("AAA snprintf");
        fflush(stderr);
        free(buf);
        return -errno;
    }

    printf("writing %d %s\n", len, buf);
    fflush(stdout);
    ret = write(fd, buf, len);
    if (ret < 0) {
        perror("AAA write");
        free(buf);
        return -errno;
    } else if (ret < len) {
        fprintf(stderr, "partial write %d %d\n", ret, len);
    }
    printf("Wrote >>>\n%s\n<<< (%d bytes)\n", buf, ret);

    bzero(buf, 500);
    ret = read(fd, buf, 500);
    if (ret < 0) {
        perror("AAA read");
        free(buf);
        return -errno;
    }

    printf("Got buf %s\n", buf);

    // NAK
    if (buf[0] == 'N') {
        printf("Got NAK, buf: %s\n", buf);
        free(buf);
        return -1;
    }

    long int new_pos = strtol(buf+2, NULL, 10);
    //printf("ID client read up to %ld\n", new_pos);

    // TODO: check for ACK response
    //printf("read %s (%d bytes)\n", buf, ret);

    free(buf);

    return new_pos;
}

void *move_file_thread(void *ptr) {
    struct move_info *mv_info = (struct move_info *)ptr;

    int fd = setup_conn(mv_info->client_uds_path);
    if (fd < 0) {
        perror("setup_conn");
    }

    long int ret = inform_moved_file(fd, mv_info->old_path, mv_info->new_path, mv_info->servers);
    printf("After move, downloaded %ld bytes\n", ret);

    free(mv_info->client_uds_path);
    free(mv_info->old_path);
    free(mv_info->new_path);
    free(mv_info);

    return NULL;
}

#ifdef CLIENT_ONLY
int main(int argc, char **argv) {
    //if (argc < 3) {
    if (argc < 4) {
        printf("usage: %s path servers", argv[0]);
        return 1;
    }

    int fd = setup_conn(argv[1]);
    download_file(fd, argv[2], argv[4], 1000);

    pthread_t thread;
    int done = 0;
    struct download_info *dl_info = (struct download_info *)malloc(sizeof(struct download_info));
    dl_info->fd = fd;
    dl_info->path = strdup(argv[2]);
    dl_info->servers = strdup(argv[3]);
    dl_info->end_byte = 0;
    dl_info->download_done = &done;
    auto ret = pthread_create(&thread, NULL, download_file_thread, (void *)dl_info);
    if (ret < 0) {
        perror("pthread_create?");
    }

    while (!done) {
        sleep(1);
        printf(".");
        fflush(stdout);
    }

    printf("\nDone\n");

    return 0;
}
#endif
