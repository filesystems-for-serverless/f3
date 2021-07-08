/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2017       Nikolaus Rath <Nikolaus@rath.org>
  Copyright (C) 2018       Valve, Inc

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * This is a "high-performance" version of passthrough_ll.c. While
 * passthrough_ll.c is designed to be as simple as possible, this
 * example intended to be as efficient and correct as possible.
 *
 * passthrough_hp.cc mirrors a specified "source" directory under a
 * specified the mountpoint with as much fidelity and performance as
 * possible.
 *
 * If --nocache is specified, the source directory may be changed
 * directly even while mounted and the filesystem will continue
 * to work correctly.
 *
 * Without --nocache, the source directory is assumed to be modified
 * only through the passthrough filesystem. This enables much better
 * performance, but if changes are made directly to the source, they
 * may not be immediately visible under the mountpoint and further
 * access to the mountpoint may result in incorrect behavior,
 * including data-loss.
 *
 * On its own, this filesystem fulfills no practical purpose. It is
 * intended as a template upon which additional functionality can be
 * built.
 *
 * Unless --nocache is specified, is only possible to write to files
 * for which the mounting user has read permissions. This is because
 * the writeback cache requires the kernel to be able to issue read
 * requests for all files (which the passthrough filesystem cannot
 * satisfy if it can't read the file in the underlying filesystem).
 *
 * ## Source code ##
 * \include passthrough_hp.cc
 */

#define FUSE_USE_VERSION 35

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// C includes
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <ftw.h>
#include <fuse_lowlevel.h>
#include <inttypes.h>
#include <string.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/xattr.h>
#include <sys/sendfile.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <limits.h>

// C++ includes
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <list>
#include "cxxopts.hpp"
#include <mutex>
#include <fstream>
#include <thread>
#include <iomanip>

using namespace std;

#define F3_LOG(fmt, ...) do { fprintf(stderr, "F3: line %d: " fmt "\n", __LINE__, ##__VA_ARGS__); }while(0)
#define F3_REPLY_ERR(req, err) do { if (err == 0) { fuse_reply_err(req, err); } else { fprintf(stderr, "ERROR: line %d (%d)\n", __LINE__, err); fuse_reply_err(req, err); } } while(0)
#define INODE(i) (i.is_id ? i.id_fd : i.fd)

static std::string uds_path;
int download_file(const char *uds_path, char *path, char *servers);

/* We are re-using pointers to our `struct sfs_inode` and `struct
   sfs_dirp` elements as inodes and file handles. This means that we
   must be able to store pointer a pointer in both a fuse_ino_t
   variable and a uint64_t variable (used for file handles). */
static_assert(sizeof(fuse_ino_t) >= sizeof(void*),
              "void* must fit into fuse_ino_t");
static_assert(sizeof(fuse_ino_t) >= sizeof(uint64_t),
              "fuse_ino_t must be at least 64 bits");


/* Forward declarations */
struct Inode;
static Inode& get_inode(fuse_ino_t ino);
static void forget_one(fuse_ino_t ino, uint64_t n);

// Uniquely identifies a file in the source directory tree. This could
// be simplified to just ino_t since we require the source directory
// not to contain any mountpoints. This hasn't been done yet in case
// we need to reconsider this constraint (but relaxing this would have
// the drawback that we can no longer re-use inode numbers, and thus
// readdir() would need to do a full lookup() in order to report the
// right inode number).
typedef std::pair<ino_t, dev_t> SrcId;

// Define a hash function for SrcId
namespace std {
    template<>
    struct hash<SrcId> {
        size_t operator()(const SrcId& id) const {
            return hash<ino_t>{}(id.first) ^ hash<dev_t>{}(id.second);
        }
    };
}

// Maps files in the source directory tree to inodes
typedef std::unordered_map<SrcId, Inode> InodeMap;

struct Inode {
    int fd {-1};
    int id_fd {-1};
    dev_t src_dev {0};
    ino_t src_ino {0};
    int generation {0};
    uint64_t nopen {0};
    uint64_t nlookup {0};
    bool is_id {false};
    bool needs_download{false};
    std::mutex m;

    // Delete copy constructor and assignments. We could implement
    // move if we need it.
    Inode() = default;
    Inode(const Inode&) = delete;
    Inode(Inode&& inode) = delete;
    Inode& operator=(Inode&& inode) = delete;
    Inode& operator=(const Inode&) = delete;

    ~Inode() {
        if(fd > 0)
            close(fd);
    }
};

struct Fs {
    // Must be acquired *after* any Inode.m locks.
    std::mutex mutex;
    InodeMap inodes; // protected by mutex
    Inode root;
    double timeout;
    bool debug;
    std::string source;
    std::string idroot;
    size_t blocksize;
    dev_t src_dev;
    bool nosplice;
    bool nocache;
    std::string address;
};
static Fs fs{};


#define FUSE_BUF_COPY_FLAGS                      \
        (fs.nosplice ?                           \
            FUSE_BUF_NO_SPLICE :                 \
            static_cast<fuse_buf_copy_flags>(0))


static Inode& get_inode(fuse_ino_t ino) {
    if (ino == FUSE_ROOT_ID)
        return fs.root;

    Inode* inode = reinterpret_cast<Inode*>(ino);
    if(inode->fd == -1) {
        cerr << "INTERNAL ERROR: Unknown inode " << ino << endl;
        abort();
    }
    return *inode;
}


static bool f3_is_id_ext(const char *name) {
    const char *ext = strrchr(name, '.');
    return ext && !strncmp(ext+1, "id", sizeof("id"));
}


// Gets the path of file relative to source directory
static int f3_get_rel_path(int fd, char *full_path) {
    char proc_path[PATH_MAX];
    sprintf(proc_path, "/proc/self/fd/%i", fd);
    int len = readlink(proc_path, full_path, PATH_MAX);
    if (len < 0)
        return -1;

    return 0;
}

static int f3_get_full_fd(int fd, mode_t mode) {
    char buf[64];
    sprintf(buf, "/proc/self/fd/%i", fd);
    return open(buf, mode);
}

static int f3_mark_as_id(int fd, int root_dir_len) {
    int res = 0;
    auto saveerr = ENOMEM;

    auto newfd = f3_get_full_fd(fd, O_RDONLY);
    if (newfd == -1) {
        return 0;
    }

    const char *istempfile = "true";
    res = fsetxattr(newfd, "user.f3.id", istempfile, sizeof("true"), 0);
    if (res == -1) {
        saveerr = errno;
        goto out;
    }

    res = fsetxattr(newfd, "user.f3.servers", fs.address.c_str(), strlen(fs.address.c_str()), 0);
    if (res == -1) {
        saveerr = errno;
        goto out;
    }

    // Want the file path relative to the Ceph mountpoint
    char full_path[PATH_MAX];
    memset(full_path, 0, PATH_MAX);
    res = f3_get_rel_path(newfd, full_path);
    if (res < 0) {
        saveerr = errno;
        goto out;
    }
    //cerr << "F3: full path: " << full_path << " rel path: " << full_path + fs.source.length() << endl;
    F3_LOG("full path: %s rel path: %s", full_path, full_path + root_dir_len);
    res = fsetxattr(fd, "user.f3.filepath", full_path + root_dir_len, strlen(full_path + root_dir_len), 0);
    if (res == -1) {
        saveerr = errno;
        goto out;
    }

out:
    close(newfd);
    errno = saveerr;
    return res;
}

static int f3_is_id(int fd) {
    char attr[10];
    int ret = 1;
    auto saveerr = ENOMEM;

    // fd might have been opened in a mode that doesn't allow using
    // fgetxattr (e.g. O_PATH)
    auto newfd = f3_get_full_fd(fd, O_RDONLY);
    if (newfd == -1) {
        return 0;
    }

    ret = fgetxattr(newfd, "user.f3.id", attr, sizeof(attr));
    if (ret < 0) {
        if (errno == ENODATA) {
            ret = 0;
            errno = 0;
        } else {
            perror("uh6");
        }
        ret = 0;
        goto out;
    }

    // Don't actually care what the attr is, just that it exists
    //printf("Got attr %s\n", attr);

out:
    close(newfd);
    errno = saveerr;
    return ret;
}

static bool f3_is_new_id(fuse_ino_t parent, const char *name) {
    Inode& inode_p = get_inode(parent);
    return f3_is_id(inode_p.fd) > 0 || f3_is_id_ext(name);
}

static int f3_get_servers(int fd, char *servers, size_t size) {
    int ret = 0;

    auto newfd = f3_get_full_fd(fd, O_RDONLY);
    if (newfd == -1) {
        return 0;
    }

    ret = fgetxattr(newfd, "user.f3.servers", servers, size);
    if (ret < 0) {
        perror("uh5");
    }

    close(newfd);

    return ret;
}


static int f3_get_filepath(int fd, char *filepath, size_t size) {
    int ret = 0;

    auto newfd = f3_get_full_fd(fd, O_RDONLY);
    if (newfd == -1) {
        return 0;
    }

    ret = fgetxattr(newfd, "user.f3.filepath", filepath, size);
    if (ret < 0) {
        perror("uh50");
    }

    close(newfd);

    return ret;
}

static int f3_make_id_fs(fuse_ino_t ino) {
    Inode& inode = get_inode(ino);
    auto fs_fd = f3_get_full_fd(inode.fd, O_RDWR);
    auto id_fd = f3_get_full_fd(inode.id_fd, O_RDWR);
    int saverr = 0;
    int ret = 0;
    off_t offset = 0;

    while ((ret = sendfile(fs_fd, id_fd, &offset, 4096)) > 0);
    if (ret == -1) {
        saverr = errno;
        F3_LOG("!!! %d", saverr);
        goto out;
    }

    if (ftruncate(id_fd, 0) < 0) {
        saverr = errno;
        F3_LOG("!!! %d", saverr);
    }

    inode.is_id = false;

out:
    close(fs_fd);
    close(id_fd);

    return saverr;
}


static int get_fs_fd(fuse_ino_t ino) {
    int fd = get_inode(ino).fd;
    return fd;
}

static int get_fs_id_fd(fuse_ino_t ino) {
    int fd = get_inode(ino).id_fd;
    return fd;
}

static void sfs_init(void *userdata, fuse_conn_info *conn) {
    (void)userdata;
    if (conn->capable & FUSE_CAP_EXPORT_SUPPORT)
        conn->want |= FUSE_CAP_EXPORT_SUPPORT;

    if (fs.timeout && conn->capable & FUSE_CAP_WRITEBACK_CACHE)
        conn->want |= FUSE_CAP_WRITEBACK_CACHE;

    if (conn->capable & FUSE_CAP_FLOCK_LOCKS)
        conn->want |= FUSE_CAP_FLOCK_LOCKS;

    // Use splicing if supported. Since we are using writeback caching
    // and readahead, individual requests should have a decent size so
    // that splicing between fd's is well worth it.
    if (conn->capable & FUSE_CAP_SPLICE_WRITE && !fs.nosplice)
        conn->want |= FUSE_CAP_SPLICE_WRITE;
    if (conn->capable & FUSE_CAP_SPLICE_READ && !fs.nosplice)
        conn->want |= FUSE_CAP_SPLICE_READ;
}


static void sfs_getattr(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    (void)fi;
    Inode& inode = get_inode(ino);
    //F3_LOG("%s: %lu", __func__, (long unsigned int)ino);

    struct stat attr;
    auto res = fstatat(INODE(inode), "", &attr,
                   AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
    if (res == -1) {
        F3_REPLY_ERR(req, errno);
        return;
    }
    //F3_LOG("%s: size: %lu", __func__, attr.st_size);
    fuse_reply_attr(req, &attr, fs.timeout);
}


static void do_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                       int valid, struct fuse_file_info* fi) {
    Inode& inode = get_inode(ino);
    int ifd = INODE(inode);
    int res;

    if (valid & FUSE_SET_ATTR_MODE) {
        if (fi) {
            res = fchmod(fi->fh, attr->st_mode);
        } else {
            char procname[64];
            sprintf(procname, "/proc/self/fd/%i", ifd);
            res = chmod(procname, attr->st_mode);
        }
        if (res == -1)
            goto out_err;
    }
    if (valid & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
        uid_t uid = (valid & FUSE_SET_ATTR_UID) ? attr->st_uid : static_cast<uid_t>(-1);
        gid_t gid = (valid & FUSE_SET_ATTR_GID) ? attr->st_gid : static_cast<gid_t>(-1);

        res = fchownat(ifd, "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        if (res == -1)
            goto out_err;
    }
    if (valid & FUSE_SET_ATTR_SIZE) {
        if (fi) {
            res = ftruncate(fi->fh, attr->st_size);
        } else {
            char procname[64];
            sprintf(procname, "/proc/self/fd/%i", ifd);
            res = truncate(procname, attr->st_size);
        }
        if (res == -1)
            goto out_err;
    }
    if (valid & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {
        struct timespec tv[2];

        tv[0].tv_sec = 0;
        tv[1].tv_sec = 0;
        tv[0].tv_nsec = UTIME_OMIT;
        tv[1].tv_nsec = UTIME_OMIT;

        if (valid & FUSE_SET_ATTR_ATIME_NOW)
            tv[0].tv_nsec = UTIME_NOW;
        else if (valid & FUSE_SET_ATTR_ATIME)
            tv[0] = attr->st_atim;

        if (valid & FUSE_SET_ATTR_MTIME_NOW)
            tv[1].tv_nsec = UTIME_NOW;
        else if (valid & FUSE_SET_ATTR_MTIME)
            tv[1] = attr->st_mtim;

        if (fi)
            res = futimens(fi->fh, tv);
        else {
#ifdef HAVE_UTIMENSAT
            char procname[64];
            sprintf(procname, "/proc/self/fd/%i", ifd);
            res = utimensat(AT_FDCWD, procname, tv, 0);
#else
            res = -1;
            errno = EOPNOTSUPP;
#endif
        }
        if (res == -1)
            goto out_err;
    }
    return sfs_getattr(req, ino, fi);

out_err:
    F3_REPLY_ERR(req, errno);
}


static void sfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                        int valid, fuse_file_info *fi) {
    (void) ino;
    do_setattr(req, ino, attr, valid, fi);
}


static int do_lookup(fuse_ino_t parent, const char *name,
                     fuse_entry_param *e) {
    if (fs.debug)
        cerr << "DEBUG: lookup(): name=" << name
             << ", parent=" << parent << endl;
    memset(e, 0, sizeof(*e));
    e->attr_timeout = fs.timeout;
    e->entry_timeout = fs.timeout;

    auto newfd = openat(get_fs_fd(parent), name, O_PATH | O_NOFOLLOW);
    if (newfd == -1)
        return errno;

    auto res = fstatat(newfd, "", &e->attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
    if (res == -1) {
        auto saveerr = errno;
        close(newfd);
        if (fs.debug)
            cerr << "DEBUG: lookup(): fstatat failed" << endl;
        return saveerr;
    }

    if (e->attr.st_dev != fs.src_dev) {
        cerr << "WARNING: Mountpoints in the source directory tree will be hidden." << endl;
        return ENOTSUP;
    } else if (e->attr.st_ino == FUSE_ROOT_ID) {
        cerr << "ERROR: Source directory tree must not include inode "
             << FUSE_ROOT_ID << endl;
        return EIO;
    }

    SrcId id {e->attr.st_ino, e->attr.st_dev};
    unique_lock<mutex> fs_lock {fs.mutex};
    Inode* inode_p;
    try {
        inode_p = &fs.inodes[id];
    } catch (std::bad_alloc&) {
        return ENOMEM;
    }
    e->ino = reinterpret_cast<fuse_ino_t>(inode_p);
    Inode& inode {*inode_p};
    e->generation = inode.generation;

    if (inode.fd == -ENOENT) { // found unlinked inode
        if (fs.debug)
            cerr << "DEBUG: lookup(): inode " << e->attr.st_ino
                 << " recycled; generation=" << inode.generation << endl;
    /* fallthrough to new inode but keep existing inode.nlookup */
    }

    if (inode.fd > 0) { // found existing inode
        fs_lock.unlock();
        if (fs.debug)
            cerr << "DEBUG: lookup(): inode " << e->attr.st_ino
                 << " (userspace) already known; fd = " << inode.fd
                 << " " << (long unsigned int)inode_p << endl;
        lock_guard<mutex> g {inode.m};
        inode.nlookup++;
        close(newfd);
    } else { // no existing inode
        /* This is just here to make Helgrind happy. It violates the
           lock ordering requirement (inode.m must be acquired before
           fs.mutex), but this is of no consequence because at this
           point no other thread has access to the inode mutex */
        lock_guard<mutex> g {inode.m};
        inode.src_ino = e->attr.st_ino;
        inode.src_dev = e->attr.st_dev;
        inode.nlookup++;
        inode.fd = newfd;

        if (f3_is_new_id(parent, name)) {
            inode.is_id = true;
        }

        auto id_fd = openat(get_fs_id_fd(parent), name, O_PATH | O_NOFOLLOW);
        if (id_fd == -1) {
            auto saverr = errno;
            if (saverr == ENOENT) {
                F3_LOG("%s: ENOENT, creating...", __func__);
                if (S_ISDIR(e->attr.st_mode)) {
                    F3_LOG("...directory");
                    auto res = mkdirat(get_fs_id_fd(parent), name, e->attr.st_mode);
                    if (res == -1) {
                        saverr = errno;
                        F3_LOG("!!! %d", saverr);
                    }
                    id_fd = openat(get_fs_id_fd(parent), name, O_PATH | O_NOFOLLOW);
                    if (id_fd == -1) {
                        saverr = errno;
                        F3_LOG("!!! %d", saverr);
                    }
                } else {
                    F3_LOG("...file");
                    id_fd = openat(get_fs_id_fd(parent), name,O_CREAT & ~O_NOFOLLOW,
                            e->attr.st_mode);
                    if (id_fd == -1) {
                        saverr = errno;
                        F3_LOG("!!! %d", saverr);
                    }
                    close(id_fd);
                    id_fd = openat(get_fs_id_fd(parent), name, O_PATH | O_NOFOLLOW);
                    if (id_fd == -1) {
                        saverr = errno;
                        F3_LOG("!!! %d", saverr);
                    }
                    if (inode.is_id)
                        inode.needs_download = true;
                }
                // XXX Don't need to mark as ID, since the FS file is what's marked and
                // we're creating the ID file/directory here
                // If it's an ID file, need to download it
                // XXX Check access modes to see if it's being read?  Where do we do that?
                // Otherwise (non-ID file/directory or ID directory), just create
                // the place holder
            }
            //F3_LOG("!!! %d %lu", get_fs_id_fd(parent), (long unsigned int)parent);
            //return saverr;
        }
        inode.id_fd = id_fd;

        fs_lock.unlock();

        if (fs.debug)
            cerr << "DEBUG: lookup(): created userspace inode " << e->attr.st_ino
                 << "; fd = " << inode.fd << " " << inode.id_fd 
                 << " " << (long unsigned int)inode_p << endl;
    }

    if (inode.is_id) {
        F3_LOG("%s: replacing attr with ID attr", __func__);
        auto res = fstatat(inode.id_fd, "", &e->attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
        if (res == -1) {
            auto saveerr = errno;
            close(newfd);
            if (fs.debug)
                cerr << "DEBUG: lookup(): fstatat failed" << endl;
            return saveerr;
        }
        inode.src_ino = e->attr.st_ino;
        inode.src_dev = e->attr.st_dev;
    }

    return 0;
}


static void sfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    F3_LOG("%s: %lu %s", __func__, (long unsigned int)parent, name);
    fuse_entry_param e {};
    auto err = do_lookup(parent, name, &e);
    if (err == ENOENT) {
        e.attr_timeout = fs.timeout;
        e.entry_timeout = fs.timeout;
        e.ino = e.attr.st_ino = 0;
        fuse_reply_entry(req, &e);
    } else if (err) {
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
    } else {
        F3_LOG("%s: returning with attr size %lu", __func__, e.attr.st_size);
        fuse_reply_entry(req, &e);
    }
}


static void mknod_symlink(fuse_req_t req, fuse_ino_t parent,
                              const char *name, mode_t mode, dev_t rdev,
                              const char *link) {
    int res;
    Inode& inode_p = get_inode(parent);
    auto saverr = ENOMEM;

    F3_LOG("%s: %lu %s", __func__, (long unsigned int)parent, name);

    if (S_ISDIR(mode))
        res = mkdirat(inode_p.fd, name, mode);
    else if (S_ISLNK(mode))
        res = symlinkat(link, inode_p.fd, name);
    else
        res = mknodat(inode_p.fd, name, mode, rdev);
    saverr = errno;
    if (res == -1)
        goto out;

    if (S_ISDIR(mode)) {
        res = mkdirat(inode_p.id_fd, name, mode);
        saverr = errno;
        if (res == -1)
            goto out;

        if (f3_is_new_id(parent, name)) {
            auto fd = openat(inode_p.fd, name, O_PATH | O_NOFOLLOW);
            f3_mark_as_id(fd, fs.source.length());
            close(fd);
            fd = openat(inode_p.id_fd, name, O_PATH | O_NOFOLLOW);
            f3_mark_as_id(fd, fs.idroot.length());
            close(fd);
        }

    } else if (S_ISLNK(mode)) {
        res = symlinkat(link, inode_p.id_fd, name);
        saverr = errno;
        if (res == -1)
            goto out;
    }

    fuse_entry_param e;
    saverr = do_lookup(parent, name, &e);
    if (saverr)
        goto out;

    fuse_reply_entry(req, &e);
    return;

out:
    if (saverr == ENFILE || saverr == EMFILE)
        cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    F3_REPLY_ERR(req, saverr);
}


static void sfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                      mode_t mode, dev_t rdev) {
    mknod_symlink(req, parent, name, mode, rdev, nullptr);
}


static void sfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                      mode_t mode) {
    mknod_symlink(req, parent, name, S_IFDIR | mode, 0, nullptr);
}


static void sfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                        const char *name) {
    mknod_symlink(req, parent, name, S_IFLNK, 0, link);
}


static void sfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
                     const char *name) {
    Inode& inode = get_inode(ino);
    Inode& inode_p = get_inode(parent);
    fuse_entry_param e {};

    e.attr_timeout = fs.timeout;
    e.entry_timeout = fs.timeout;

    char procname[64];
    sprintf(procname, "/proc/self/fd/%i", inode.fd);
    auto res = linkat(AT_FDCWD, procname, inode_p.fd, name, AT_SYMLINK_FOLLOW);
    if (res == -1) {
        F3_REPLY_ERR(req, errno);
        return;
    }

    sprintf(procname, "/proc/self/fd/%i", inode.id_fd);
    res = linkat(AT_FDCWD, procname, inode_p.id_fd, name, AT_SYMLINK_FOLLOW);
    if (res == -1) {
        F3_REPLY_ERR(req, errno);
        return;
    }

    res = fstatat(INODE(inode), "", &e.attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
    if (res == -1) {
        F3_REPLY_ERR(req, errno);
        return;
    }
    e.ino = reinterpret_cast<fuse_ino_t>(&inode);
    {
        lock_guard<mutex> g {inode.m};
        inode.nlookup++;
    }

    fuse_reply_entry(req, &e);
    return;
}


static void sfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    Inode& inode_p = get_inode(parent);
    lock_guard<mutex> g {inode_p.m};
    auto res = unlinkat(inode_p.fd, name, AT_REMOVEDIR);
    if (res == -1)
        F3_REPLY_ERR(req, errno);
    res = unlinkat(inode_p.id_fd, name, AT_REMOVEDIR);
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


static void sfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                       fuse_ino_t newparent, const char *newname,
                       unsigned int flags) {
    Inode& inode_p = get_inode(parent);
    Inode& inode_np = get_inode(newparent);
    if (flags) {
        F3_REPLY_ERR(req, EINVAL);
        return;
    }

    auto res = renameat(inode_p.fd, name, inode_np.fd, newname);
    if (res == -1)
        F3_REPLY_ERR(req, errno);
    res = renameat(inode_p.id_fd, name, inode_np.id_fd, newname);
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


static void sfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    Inode& inode_p = get_inode(parent);
    // Release inode.fd before last unlink like nfsd EXPORT_OP_CLOSE_BEFORE_UNLINK
    // to test reused inode numbers.
    // Skip this when inode has an open file and when writeback cache is enabled.
    if (!fs.timeout) {
        fuse_entry_param e;
        auto err = do_lookup(parent, name, &e);
        if (err) {
            F3_REPLY_ERR(req, err);
            return;
        }
        if (e.attr.st_nlink == 1) {
            Inode& inode = get_inode(e.ino);
            lock_guard<mutex> g {inode.m};
            if (inode.fd > 0 && !inode.nopen) {
                if (fs.debug)
                    cerr << "DEBUG: unlink: release inode " << e.attr.st_ino
                        << "; fd=" << inode.fd << endl;
                lock_guard<mutex> g_fs {fs.mutex};
                close(inode.fd);
                close(inode.id_fd);
                inode.fd = -ENOENT;
                inode.id_fd = -ENOENT;
                inode.generation++;
            }
        }
    }
    auto res = unlinkat(inode_p.fd, name, 0);
    if (res == -1)
        F3_REPLY_ERR(req, errno);
    res = unlinkat(inode_p.id_fd, name, 0);
    if (res == -1)
        F3_REPLY_ERR(req, errno);

    F3_REPLY_ERR(req, 0);
}


static void forget_one(fuse_ino_t ino, uint64_t n) {
    Inode& inode = get_inode(ino);
    unique_lock<mutex> l {inode.m};

    if(n > inode.nlookup) {
        cerr << "INTERNAL ERROR: Negative lookup count for inode "
             << inode.src_ino << endl;
        abort();
    }
    inode.nlookup -= n;
    if (!inode.nlookup) {
        if (fs.debug)
            cerr << "DEBUG: forget: cleaning up inode " << inode.src_ino << endl;
        {
            lock_guard<mutex> g_fs {fs.mutex};
            l.unlock();
            fs.inodes.erase({inode.src_ino, inode.src_dev});
        }
    } else if (fs.debug)
            cerr << "DEBUG: forget: inode " << inode.src_ino
                 << " lookup count now " << inode.nlookup << endl;
}

static void sfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
    forget_one(ino, nlookup);
    fuse_reply_none(req);
}


static void sfs_forget_multi(fuse_req_t req, size_t count,
                             fuse_forget_data *forgets) {
    for (size_t i = 0; i < count; i++)
        forget_one(forgets[i].ino, forgets[i].nlookup);
    fuse_reply_none(req);
}


static void sfs_readlink(fuse_req_t req, fuse_ino_t ino) {
    Inode& inode = get_inode(ino);
    char buf[PATH_MAX + 1];
    auto res = readlinkat(INODE(inode), "", buf, sizeof(buf));
    if (res == -1)
        F3_REPLY_ERR(req, errno);
    else if (res == sizeof(buf))
        F3_REPLY_ERR(req, ENAMETOOLONG);
    else {
        buf[res] = '\0';
        fuse_reply_readlink(req, buf);
    }
}


struct DirHandle {
    DIR *dp {nullptr};
    off_t offset;

    DirHandle() = default;
    DirHandle(const DirHandle&) = delete;
    DirHandle& operator=(const DirHandle&) = delete;

    ~DirHandle() {
        if(dp)
            closedir(dp);
    }
};


static DirHandle *get_dir_handle(fuse_file_info *fi) {
    return reinterpret_cast<DirHandle*>(fi->fh);
}


static void sfs_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    Inode& inode = get_inode(ino);
    auto d = new (nothrow) DirHandle;
    if (d == nullptr) {
        F3_REPLY_ERR(req, ENOMEM);
        return;
    }

    // Make Helgrind happy - it can't know that there's an implicit
    // synchronization due to the fact that other threads cannot
    // access d until we've called fuse_reply_*.
    lock_guard<mutex> g {inode.m};

    auto fd = openat(INODE(inode), ".", O_RDONLY);
    if (fd == -1)
        goto out_errno;

    // On success, dir stream takes ownership of fd, so we
    // do not have to close it.
    d->dp = fdopendir(fd);
    if(d->dp == nullptr)
        goto out_errno;

    d->offset = 0;

    fi->fh = reinterpret_cast<uint64_t>(d);
    if(fs.timeout) {
        fi->keep_cache = 1;
        fi->cache_readdir = 1;
    }
    fuse_reply_open(req, fi);
    return;

out_errno:
    auto error = errno;
    delete d;
    if (error == ENFILE || error == EMFILE)
        cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    F3_REPLY_ERR(req, error);
}


static bool is_dot_or_dotdot(const char *name) {
    return name[0] == '.' &&
           (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'));
}


static void do_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                    off_t offset, fuse_file_info *fi, int plus) {

    F3_LOG("%s: %lu", __func__, (long unsigned int)ino);
    auto d = get_dir_handle(fi);
    Inode& inode = get_inode(ino);
    lock_guard<mutex> g {inode.m};
    char *p;
    auto rem = size;
    int err = 0, count = 0;

    if (fs.debug)
        cerr << "DEBUG: readdir(): started with offset "
             << offset << endl;

    auto buf = new (nothrow) char[size];
    if (!buf) {
        F3_REPLY_ERR(req, ENOMEM);
        return;
    }
    p = buf;

    if (offset != d->offset) {
        if (fs.debug)
            cerr << "DEBUG: readdir(): seeking to " << offset << endl;
        seekdir(d->dp, offset);
        d->offset = offset;
    }

    while (1) {
        struct dirent *entry;
        errno = 0;
        entry = readdir(d->dp);
        if (!entry) {
            if(errno) {
                err = errno;
                if (fs.debug)
                    warn("DEBUG: readdir(): readdir failed with");
                goto error;
            }
            break; // End of stream
        }
        d->offset = entry->d_off;
        if (is_dot_or_dotdot(entry->d_name))
            continue;

        fuse_entry_param e{};
        size_t entsize;
        if(plus) {
            err = do_lookup(ino, entry->d_name, &e);
            if (err)
                goto error;
            entsize = fuse_add_direntry_plus(req, p, rem, entry->d_name, &e, entry->d_off);

            if (entsize > rem) {
                if (fs.debug)
                    cerr << "DEBUG: readdir(): buffer full, returning data. " << endl;
                forget_one(e.ino, 1);
                break;
            }
        } else {
            e.attr.st_ino = entry->d_ino;
            e.attr.st_mode = entry->d_type << 12;
            entsize = fuse_add_direntry(req, p, rem, entry->d_name, &e.attr, entry->d_off);

            if (entsize > rem) {
                if (fs.debug)
                    cerr << "DEBUG: readdir(): buffer full, returning data. " << endl;
                break;
            }
        }

        p += entsize;
        rem -= entsize;
        count++;
        if (fs.debug) {
            cerr << "DEBUG: readdir(): added to buffer: " << entry->d_name
                 << ", ino " << e.attr.st_ino << ", offset " << entry->d_off << endl;
        }
    }
    err = 0;
error:

    // If there's an error, we can only signal it if we haven't stored
    // any entries yet - otherwise we'd end up with wrong lookup
    // counts for the entries that are already in the buffer. So we
    // return what we've collected until that point.
    if (err && rem == size) {
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
    } else {
        if (fs.debug)
            cerr << "DEBUG: readdir(): returning " << count
                 << " entries, curr offset " << d->offset << endl;
        fuse_reply_buf(req, buf, size - rem);
    }
    delete[] buf;
    return;
}


static void sfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                        off_t offset, fuse_file_info *fi) {
    // operation logging is done in readdir to reduce code duplication
    do_readdir(req, ino, size, offset, fi, 0);
}


static void sfs_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                            off_t offset, fuse_file_info *fi) {
    // operation logging is done in readdir to reduce code duplication
    do_readdir(req, ino, size, offset, fi, 1);
}


static void sfs_releasedir(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    (void) ino;
    auto d = get_dir_handle(fi);
    delete d;
    fuse_reply_err(req, 0);
}


static void sfs_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode, fuse_file_info *fi) {
    Inode& inode_p = get_inode(parent);

    F3_LOG("%s: %lu %s", __func__, (long unsigned int)parent, name);

    auto fd = openat(inode_p.fd, name,
                     (fi->flags | O_CREAT) & ~O_NOFOLLOW, mode);
    if (fd == -1) {
        auto err = errno;
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
        return;
    }

    fi->fh = fd;
    /*
    fi->fh = fd;
    fuse_entry_param e;
    auto err = do_lookup(parent, name, &e);
    if (err) {
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
    return;
    }*/

    auto id_fd = openat(inode_p.id_fd, name,
                     (fi->flags | O_CREAT) & ~O_NOFOLLOW, mode);
    if (id_fd == -1) {
        auto err = errno;
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
        return;
    }

    if (f3_is_new_id(parent, name)) {
        f3_mark_as_id(fd, fs.source.length());
        f3_mark_as_id(id_fd, fs.idroot.length());
        fi->fh = id_fd;
    }

    // XXX Here is where we need to choose which fd to return to user
    //fi->fh = fd;
    fuse_entry_param e;
    auto err = do_lookup(parent, name, &e);
    if (err) {
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
        return;
    }

    Inode& inode = get_inode(e.ino);
    lock_guard<mutex> g {inode.m};
    inode.nopen++;
    fuse_reply_create(req, &e, fi);
}


static void sfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                         fuse_file_info *fi) {
    (void) ino;
    int res;
    int fd = dirfd(get_dir_handle(fi)->dp);
    if (datasync)
        res = fdatasync(fd);
    else
        res = fsync(fd);
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


static void sfs_open(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    Inode& inode = get_inode(ino);

    F3_LOG("%s: %lu %d", __func__, (long unsigned int)ino, inode.fd);

    /* With writeback cache, kernel may send read requests even
       when userspace opened write-only */
    if (fs.timeout && (fi->flags & O_ACCMODE) == O_WRONLY) {
        fi->flags &= ~O_ACCMODE;
        fi->flags |= O_RDWR;
    }

    /* With writeback cache, O_APPEND is handled by the kernel.  This
       breaks atomicity (since the file may change in the underlying
       filesystem, so that the kernel's idea of the end of the file
       isn't accurate anymore). However, no process should modify the
       file in the underlying filesystem once it has been read, so
       this is not a problem. */
    if (fs.timeout && fi->flags & O_APPEND)
        fi->flags &= ~O_APPEND;

    if (inode.needs_download) {
        F3_LOG("Needs download");

        char rel_path[PATH_MAX];
        f3_get_filepath(inode.fd, rel_path, PATH_MAX);
        char servers[100];
        f3_get_servers(inode.fd, servers, 100);

        auto ret = download_file(uds_path.c_str(), rel_path, servers);
        if (ret < 0) {
            perror("Download?");
        }
        inode.needs_download = false;
    }

    /* Unfortunately we cannot use inode.fd, because this was opened
       with O_PATH (so it doesn't allow read/write access). */
    F3_LOG("%s: ID fd: %d FS fd: %d is_id: %d", __func__, inode.id_fd, inode.fd, inode.is_id);
    char buf[64];
    sprintf(buf, "/proc/self/fd/%i", INODE(inode));
    auto fd = open(buf, fi->flags & ~O_NOFOLLOW);
    if (fd == -1) {
        auto err = errno;
        if (err == ENFILE || err == EMFILE)
            cerr << "ERROR: Reached maximum number of file descriptors." << endl;
        F3_REPLY_ERR(req, err);
        return;
    }

    lock_guard<mutex> g {inode.m};
    inode.nopen++;
    fi->keep_cache = (fs.timeout != 0);
    fi->fh = fd;
    fuse_reply_open(req, fi);
}


static void sfs_release(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    Inode& inode = get_inode(ino);
    lock_guard<mutex> g {inode.m};
    inode.nopen--;
    close(fi->fh);
    F3_REPLY_ERR(req, 0);
}


static void sfs_flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi) {
    (void) ino;
    auto res = close(dup(fi->fh));
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


static void sfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                      fuse_file_info *fi) {
    (void) ino;
    int res;
    if (datasync)
        res = fdatasync(fi->fh);
    else
        res = fsync(fi->fh);
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


static void do_read(fuse_req_t req, size_t size, off_t off, fuse_file_info *fi) {

    fuse_bufvec buf = FUSE_BUFVEC_INIT(size);
    buf.buf[0].flags = static_cast<fuse_buf_flags>(
        FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
    buf.buf[0].fd = fi->fh;
    buf.buf[0].pos = off;

    fuse_reply_data(req, &buf, FUSE_BUF_COPY_FLAGS);
}

static void sfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                     fuse_file_info *fi) {
    (void) ino;
    Inode& inode = get_inode(ino);
    F3_LOG("%s: ID fd: %d FS fd: %d is_id: %d fd: %lu", __func__, inode.id_fd, inode.fd, inode.is_id, fi->fh);
    do_read(req, size, off, fi);
}


static void do_write_buf(fuse_req_t req, size_t size, off_t off,
                         fuse_bufvec *in_buf, fuse_file_info *fi) {
    fuse_bufvec out_buf = FUSE_BUFVEC_INIT(size);
    out_buf.buf[0].flags = static_cast<fuse_buf_flags>(
        FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
    out_buf.buf[0].fd = fi->fh;
    out_buf.buf[0].pos = off;

    F3_LOG("%s: %lu", __func__, fi->fh);

    auto res = fuse_buf_copy(&out_buf, in_buf, FUSE_BUF_COPY_FLAGS);
    if (res < 0)
        F3_REPLY_ERR(req, (int)-res);
    else
        fuse_reply_write(req, (size_t)res);
}


static void sfs_write_buf(fuse_req_t req, fuse_ino_t ino, fuse_bufvec *in_buf,
                          off_t off, fuse_file_info *fi) {
    (void) ino;
    Inode& inode = get_inode(ino);
    F3_LOG("%s: ID fd: %d FS fd: %d is_id: %d", __func__, inode.id_fd, inode.fd, inode.is_id);
    auto size {fuse_buf_size(in_buf)};
    do_write_buf(req, size, off, in_buf, fi);
}


static void sfs_statfs(fuse_req_t req, fuse_ino_t ino) {
    struct statvfs stbuf;

    auto res = fstatvfs(get_fs_fd(ino), &stbuf);
    if (res == -1)
        F3_REPLY_ERR(req, errno);
    else
        fuse_reply_statfs(req, &stbuf);
}


#ifdef HAVE_POSIX_FALLOCATE
static void sfs_fallocate(fuse_req_t req, fuse_ino_t ino, int mode,
                          off_t offset, off_t length, fuse_file_info *fi) {
    (void) ino;
    if (mode) {
        F3_REPLY_ERR(req, EOPNOTSUPP);
        return;
    }

    auto err = posix_fallocate(fi->fh, offset, length);
    F3_REPLY_ERR(req, err);
}
#endif

static void sfs_flock(fuse_req_t req, fuse_ino_t ino, fuse_file_info *fi,
                      int op) {
    (void) ino;
    auto res = flock(fi->fh, op);
    F3_REPLY_ERR(req, res == -1 ? errno : 0);
}


#ifdef HAVE_SETXATTR
static void sfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                         size_t size) {
    char *value = nullptr;
    Inode& inode = get_inode(ino);
    ssize_t ret;
    int saverr;

    F3_LOG("%s: %s", __func__, name);

    // Doesn't matter which inode (ID vs FS) since the xattrs should be the same
    char procname[64];
    sprintf(procname, "/proc/self/fd/%i", INODE(inode));

    if (size) {
        value = new (nothrow) char[size];
        if (value == nullptr) {
            saverr = ENOMEM;
            goto out;
        }

        ret = getxattr(procname, name, value, size);
        if (ret == -1)
            goto out_err;
        saverr = 0;
        if (ret == 0)
            goto out;

        fuse_reply_buf(req, value, ret);
    } else {
        ret = getxattr(procname, name, nullptr, 0);
        if (ret == -1)
            goto out_err;

        fuse_reply_xattr(req, ret);
    }
out_free:
    delete[] value;
    return;

out_err:
    saverr = errno;
out:
    F3_REPLY_ERR(req, saverr);
    goto out_free;
}


static void sfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
    char *value = nullptr;
    Inode& inode = get_inode(ino);
    ssize_t ret;
    int saverr;

    // Doesn't matter which inode (ID vs FS) since the xattrs should be the same
    char procname[64];
    sprintf(procname, "/proc/self/fd/%i", INODE(inode));

    if (size) {
        value = new (nothrow) char[size];
        if (value == nullptr) {
            saverr = ENOMEM;
            goto out;
        }

        ret = listxattr(procname, value, size);
        if (ret == -1)
            goto out_err;
        saverr = 0;
        if (ret == 0)
            goto out;

        fuse_reply_buf(req, value, ret);
    } else {
        ret = listxattr(procname, nullptr, 0);
        if (ret == -1)
            goto out_err;

        fuse_reply_xattr(req, ret);
    }
out_free:
    delete[] value;
    return;
out_err:
    saverr = errno;
out:
    F3_REPLY_ERR(req, saverr);
    goto out_free;
}


static void sfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                         const char *value, size_t size, int flags) {
    Inode& inode = get_inode(ino);
    ssize_t ret;

    char procname[64];
    sprintf(procname, "/proc/self/fd/%i", inode.fd);
    ret = setxattr(procname, name, value, size, flags);
    if (ret == -1)
        F3_REPLY_ERR(req, errno);

    sprintf(procname, "/proc/self/fd/%i", inode.id_fd);
    ret = setxattr(procname, name, value, size, flags);
    F3_REPLY_ERR(req, ret == -1 ? errno : 0);
}


static void sfs_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name) {
    char procname[64];
    Inode& inode = get_inode(ino);
    ssize_t ret;

    F3_LOG("%s: %s", __func__, name);

    sprintf(procname, "/proc/self/fd/%i", inode.fd);
    ret = removexattr(procname, name);
    if (ret == -1)
        F3_REPLY_ERR(req, errno);

    sprintf(procname, "/proc/self/fd/%i", inode.id_fd);
    ret = removexattr(procname, name);
    if (ret == -1)
        F3_REPLY_ERR(req, errno);

    ret = 0;
    if (strncmp(name, "user.f3.id", sizeof("user.f3.id")) == 0) {
        F3_LOG("Converting ID to regular file");
        ret = f3_make_id_fs(ino);
    }

    F3_REPLY_ERR(req, (int)ret);
}
#endif


static void assign_operations(fuse_lowlevel_ops &sfs_oper) {
    sfs_oper.init = sfs_init;
    sfs_oper.lookup = sfs_lookup;
    sfs_oper.mkdir = sfs_mkdir;
    sfs_oper.mknod = sfs_mknod;
    sfs_oper.symlink = sfs_symlink;
    sfs_oper.link = sfs_link;
    sfs_oper.unlink = sfs_unlink;
    sfs_oper.rmdir = sfs_rmdir;
    sfs_oper.rename = sfs_rename;
    sfs_oper.forget = sfs_forget;
    sfs_oper.forget_multi = sfs_forget_multi;
    sfs_oper.getattr = sfs_getattr;
    sfs_oper.setattr = sfs_setattr;
    sfs_oper.readlink = sfs_readlink;
    sfs_oper.opendir = sfs_opendir;
    sfs_oper.readdir = sfs_readdir;
    sfs_oper.readdirplus = sfs_readdirplus;
    sfs_oper.releasedir = sfs_releasedir;
    sfs_oper.fsyncdir = sfs_fsyncdir;
    sfs_oper.create = sfs_create;
    sfs_oper.open = sfs_open;
    sfs_oper.release = sfs_release;
    sfs_oper.flush = sfs_flush;
    sfs_oper.fsync = sfs_fsync;
    sfs_oper.read = sfs_read;
    sfs_oper.write_buf = sfs_write_buf;
    sfs_oper.statfs = sfs_statfs;
#ifdef HAVE_POSIX_FALLOCATE
    sfs_oper.fallocate = sfs_fallocate;
#endif
    sfs_oper.flock = sfs_flock;
#ifdef HAVE_SETXATTR
    sfs_oper.setxattr = sfs_setxattr;
    sfs_oper.getxattr = sfs_getxattr;
    sfs_oper.listxattr = sfs_listxattr;
    sfs_oper.removexattr = sfs_removexattr;
#endif
}

static void print_usage(char *prog_name) {
    cout << "Usage: " << prog_name << " --help\n"
         << "       " << prog_name << " [options] <source> <mountpoint>\n";
}

static cxxopts::ParseResult parse_wrapper(cxxopts::Options& parser, int& argc, char**& argv) {
    try {
        return parser.parse(argc, argv);
    } catch (cxxopts::option_not_exists_exception& exc) {
        std::cout << argv[0] << ": " << exc.what() << std::endl;
        print_usage(argv[0]);
        exit(2);
    }
}


static cxxopts::ParseResult parse_options(int argc, char **argv) {
    cxxopts::Options opt_parser(argv[0]);
    opt_parser.add_options()
        ("debug", "Enable filesystem debug messages")
        ("debug-fuse", "Enable libfuse debug messages")
        ("help", "Print help")
        ("nocache", "Disable all caching")
        ("nosplice", "Do not use splice(2) to transfer data")
        ("single", "Run single-threaded")
        ("a,address", "Address of this node's transfer server", cxxopts::value<std::string>())
        ("idroot", "Directory to use as root of local cache", cxxopts::value<std::string>())
        ("s,socket-path", "Path of Unix Domain Socket for connecting to download client", cxxopts::value<std::string>());

    // FIXME: Find a better way to limit the try clause to just
    // opt_parser.parse() (cf. https://github.com/jarro2783/cxxopts/issues/146)
    auto options = parse_wrapper(opt_parser, argc, argv);

    if (options.count("help")) {
        print_usage(argv[0]);
        // Strip everything before the option list from the
        // default help string.
        auto help = opt_parser.help();
        std::cout << std::endl << "options:"
                  << help.substr(help.find("\n\n") + 1, string::npos);
        exit(0);

    } else if (argc != 3) {
        std::cout << argv[0] << ": invalid number of arguments\n";
        print_usage(argv[0]);
        exit(2);
    }

    if (options.count("address")) {
        fs.address = options["address"].as<std::string>();
    }

    if (options.count("idroot")) {
        fs.idroot = options["idroot"].as<std::string>();
    }

    if (options.count("socket-path")) {
        uds_path = options["socket-path"].as<std::string>();
    }

    fs.debug = options.count("debug") != 0;
    fs.nosplice = options.count("nosplice") != 0;
    char* resolved_path = realpath(argv[1], NULL);
    if (resolved_path == NULL)
        warn("WARNING: realpath() failed with");
    fs.source = std::string {resolved_path};
    free(resolved_path);

    return options;
}


static void maximize_fd_limit() {
    struct rlimit lim {};
    auto res = getrlimit(RLIMIT_NOFILE, &lim);
    if (res != 0) {
        warn("WARNING: getrlimit() failed with");
        return;
    }
    lim.rlim_cur = lim.rlim_max;
    res = setrlimit(RLIMIT_NOFILE, &lim);
    if (res != 0)
        warn("WARNING: setrlimit() failed with");
}


int main(int argc, char *argv[]) {

    // Parse command line options
    auto options {parse_options(argc, argv)};

    // We need an fd for every dentry in our the filesystem that the
    // kernel knows about. This is way more than most processes need,
    // so try to get rid of any resource softlimit.
    maximize_fd_limit();

    // Initialize filesystem root
    fs.root.fd = -1;
    fs.root.nlookup = 9999;
    fs.timeout = options.count("nocache") ? 0 : 86400.0;

    struct stat stat;
    auto ret = lstat(fs.source.c_str(), &stat);
    if (ret == -1)
        err(1, "ERROR: failed to stat source (\"%s\")", fs.source.c_str());
    if (!S_ISDIR(stat.st_mode))
        errx(1, "ERROR: source is not a directory");
    fs.src_dev = stat.st_dev;

    fs.root.fd = open(fs.source.c_str(), O_PATH);
    if (fs.root.fd == -1)
        err(1, "ERROR: open(\"%s\", O_PATH)", fs.source.c_str());

    fs.root.id_fd = open(fs.idroot.c_str(), O_PATH);
    if (fs.root.id_fd == -1)
        err(1, "ERROR: open(\"%s\", O_PATH)", fs.idroot.c_str());

    // Initialize fuse
    fuse_args args = FUSE_ARGS_INIT(0, nullptr);
    if (fuse_opt_add_arg(&args, argv[0]) ||
        fuse_opt_add_arg(&args, "-o") ||
        fuse_opt_add_arg(&args, "default_permissions,fsname=hpps") ||
        (options.count("debug-fuse") && fuse_opt_add_arg(&args, "-odebug")))
        errx(3, "ERROR: Out of memory");

    fuse_lowlevel_ops sfs_oper {};
    assign_operations(sfs_oper);
    auto se = fuse_session_new(&args, &sfs_oper, sizeof(sfs_oper), &fs);
    if (se == nullptr)
        goto err_out1;

    if (fuse_set_signal_handlers(se) != 0)
        goto err_out2;

    // Don't apply umask, use modes exactly as specified
    umask(0);

    // Mount and run main loop
    struct fuse_loop_config loop_config;
    loop_config.clone_fd = 0;
    loop_config.max_idle_threads = 10;
    if (fuse_session_mount(se, argv[2]) != 0)
        goto err_out3;
    if (options.count("single"))
        ret = fuse_session_loop(se);
    else
        ret = fuse_session_loop_mt(se, &loop_config);

    fuse_session_unmount(se);

err_out3:
    fuse_remove_signal_handlers(se);
err_out2:
    fuse_session_destroy(se);
err_out1:
    fuse_opt_free_args(&args);

    return ret ? 1 : 0;
}

