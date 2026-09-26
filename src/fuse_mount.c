#ifndef _WIN32
/*
 * fuse_mount.c — FUSE read-only filesystem for Redivis Directory objects
 *
 * FUSE and libcurl are loaded at RUNTIME via dlopen/dlsym, so no headers
 * or dev packages are needed at compile time. Only the runtime shared
 * libraries (libfuse3.so.3, libfuse-t.dylib, or libfuse.2.dylib; and
 * libcurl.so.4 / libcurl.4.dylib) must be present when mount() is called.
 * libcurl is effectively always present, since the curl R package (a hard
 * dependency) links against it.
 */

#include <R.h>
#include <Rinternals.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <dlfcn.h>
#include <dirent.h>

/* ================================================================== */
/*  FUSE ABI declarations (replaces #include <fuse.h>)                */
/* ================================================================== */

struct fuse;
struct fuse_chan;
struct fuse_conn_info;

struct fuse_config {
    int set_gid;        int gid;
    int set_uid;        int uid;
    int set_mode;       unsigned int umask;
    double entry_timeout;
    double negative_timeout;
    double attr_timeout;
    int intr;           int intr_signal;
    int remember;
    int hard_remove;
    int use_ino;
    int readdir_ino;
    int direct_io;
    int kernel_cache;
    int auto_cache;
    int ac_attr_timeout_set;
    double ac_attr_timeout;
    int nullpath_ok;
    int show_help;
};

/* FUSE 2 and FUSE 3 have different fuse_file_info layouts.
 * FUSE 2 has an extra `fh_old` (unsigned long) and `writepage` (int) field
 * before the bitfields. FUSE 3 removed fh_old and made writepage a bitfield.
 * Since FUSE passes this struct into our callbacks, we must match the
 * library's layout exactly. We define both and use accessors at runtime. */

struct fuse_file_info_v2 {
    int           flags;
    unsigned long fh_old;
    int           writepage;
    unsigned int  direct_io : 1;
    unsigned int  keep_cache : 1;
    unsigned int  flush : 1;
    unsigned int  nonseekable : 1;
    unsigned int  flock_release : 1;
    unsigned int  cache_readdir : 1;
    unsigned int  padding : 26;
    uint64_t      fh;
    uint64_t      lock_owner;
    uint32_t      poll_events;
};

struct fuse_file_info_v3 {
    int           flags;
    unsigned int  writepage : 1;
    unsigned int  direct_io : 1;
    unsigned int  keep_cache : 1;
    unsigned int  flush : 1;
    unsigned int  nonseekable : 1;
    unsigned int  flock_release : 1;
    unsigned int  cache_readdir : 1;
    unsigned int  padding : 25;
    unsigned int  padding2 : 32;
    uint64_t      fh;
    uint64_t      lock_owner;
    uint32_t      poll_events;
};

/* Forward-declare fuse_api_version so inline accessors can reference it.
 * The actual initialization (= 0) happens in the dlopen section below. */
static int fuse_api_version = 0;

/* Runtime accessors for fuse_file_info fields. */

static inline uint64_t fi_get_fh(void *fi) {
    if (fuse_api_version == 3)
        return ((struct fuse_file_info_v3 *)fi)->fh;
    else
        return ((struct fuse_file_info_v2 *)fi)->fh;
}

static inline void fi_set_fh(void *fi, uint64_t val) {
    if (fuse_api_version == 3)
        ((struct fuse_file_info_v3 *)fi)->fh = val;
    else
        ((struct fuse_file_info_v2 *)fi)->fh = val;
}

static inline void fi_set_keep_cache(void *fi, unsigned int val) {
    if (fuse_api_version == 3)
        ((struct fuse_file_info_v3 *)fi)->keep_cache = val;
    else
        ((struct fuse_file_info_v2 *)fi)->keep_cache = val;
}

static inline int fi_get_flags(void *fi) {
    /* flags is at offset 0 in both versions */
    return ((struct fuse_file_info_v2 *)fi)->flags;
}

/* Use void* for fuse_file_info in all callback signatures,
 * since the actual struct layout depends on the API version. */
typedef void fuse_file_info_t;

typedef int (*fuse_fill_dir_v2_t)(void *buf, const char *name,
                                  const struct stat *stbuf, off_t off);
typedef int (*fuse_fill_dir_v3_t)(void *buf, const char *name,
                                  const struct stat *stbuf, off_t off,
                                  int fill_dir_flags);

struct fuse_context {
    struct fuse *fuse;
    uid_t uid;
    gid_t gid;
    pid_t pid;
    void *private_data;
    mode_t umask;
};

/* FUSE 2 operations struct */
struct fuse_operations_v2 {
    int (*getattr)(const char *, struct stat *);
    int (*readlink)(const char *, char *, size_t);
    void *__deprecated1;
    int (*mknod)(const char *, mode_t, dev_t);
    int (*mkdir)(const char *, mode_t);
    int (*unlink)(const char *);
    int (*rmdir)(const char *);
    int (*symlink)(const char *, const char *);
    int (*rename)(const char *, const char *);
    int (*link)(const char *, const char *);
    int (*chmod)(const char *, mode_t);
    int (*chown)(const char *, uid_t, gid_t);
    int (*truncate)(const char *, off_t);
    void *__deprecated2;
    int (*open)(const char *, fuse_file_info_t *);
    int (*read)(const char *, char *, size_t, off_t, fuse_file_info_t *);
    int (*write)(const char *, const char *, size_t, off_t, fuse_file_info_t *);
    int (*statfs)(const char *, void *);
    int (*flush)(const char *, fuse_file_info_t *);
    int (*release)(const char *, fuse_file_info_t *);
    int (*fsync)(const char *, int, fuse_file_info_t *);
    int (*setxattr)(const char *, const char *, const char *, size_t, int);
    int (*getxattr)(const char *, const char *, char *, size_t);
    int (*listxattr)(const char *, char *, size_t);
    int (*removexattr)(const char *, const char *);
    int (*opendir)(const char *, fuse_file_info_t *);
    int (*readdir)(const char *, void *, fuse_fill_dir_v2_t, off_t,
                   fuse_file_info_t *);
    int (*releasedir)(const char *, fuse_file_info_t *);
    int (*fsyncdir)(const char *, int, fuse_file_info_t *);
    void *(*init)(struct fuse_conn_info *);
    void (*destroy)(void *);
    int (*access)(const char *, int);
    int (*create)(const char *, mode_t, fuse_file_info_t *);
    int (*ftruncate)(const char *, off_t, fuse_file_info_t *);
    int (*fgetattr)(const char *, struct stat *, fuse_file_info_t *);
    int (*lock)(const char *, fuse_file_info_t *, int, void *);
    int (*utimens)(const char *, const void *);
    int (*bmap)(const char *, size_t, uint64_t *);
    unsigned int flag_nullpath_ok : 1;
    unsigned int flag_nopath : 1;
    unsigned int flag_utime_omit_ok : 1;
    unsigned int flag_reserved : 29;
    int (*ioctl)(const char *, int, void *, fuse_file_info_t *,
                 unsigned int, void *);
    int (*poll)(const char *, fuse_file_info_t *, void *, unsigned *);
    int (*write_buf)(const char *, void *, off_t, fuse_file_info_t *);
    int (*read_buf)(const char *, void **, size_t, off_t,
                    fuse_file_info_t *);
    int (*flock)(const char *, fuse_file_info_t *, int);
    int (*fallocate)(const char *, int, off_t, off_t,
                     fuse_file_info_t *);
};

/* FUSE 3 operations struct */
struct fuse_operations_v3 {
    int (*getattr)(const char *, struct stat *, fuse_file_info_t *);
    int (*readlink)(const char *, char *, size_t);
    int (*mknod)(const char *, mode_t, dev_t);
    int (*mkdir)(const char *, mode_t);
    int (*unlink)(const char *);
    int (*rmdir)(const char *);
    int (*symlink)(const char *, const char *);
    int (*rename)(const char *, const char *, unsigned int);
    int (*link)(const char *, const char *);
    int (*chmod)(const char *, mode_t, fuse_file_info_t *);
    int (*chown)(const char *, uid_t, gid_t, fuse_file_info_t *);
    int (*truncate)(const char *, off_t, fuse_file_info_t *);
    int (*open)(const char *, fuse_file_info_t *);
    int (*read)(const char *, char *, size_t, off_t, fuse_file_info_t *);
    int (*write)(const char *, const char *, size_t, off_t,
                 fuse_file_info_t *);
    int (*statfs)(const char *, void *);
    int (*flush)(const char *, fuse_file_info_t *);
    int (*release)(const char *, fuse_file_info_t *);
    int (*fsync)(const char *, int, fuse_file_info_t *);
    int (*setxattr)(const char *, const char *, const char *, size_t, int);
    int (*getxattr)(const char *, const char *, char *, size_t);
    int (*listxattr)(const char *, char *, size_t);
    int (*removexattr)(const char *, const char *);
    int (*opendir)(const char *, fuse_file_info_t *);
    int (*readdir)(const char *, void *, fuse_fill_dir_v3_t, off_t,
                   fuse_file_info_t *, int);
    int (*releasedir)(const char *, fuse_file_info_t *);
    int (*fsyncdir)(const char *, int, fuse_file_info_t *);
    void *(*init)(struct fuse_conn_info *, struct fuse_config *);
    void (*destroy)(void *);
    int (*access)(const char *, int);
    int (*create)(const char *, mode_t, fuse_file_info_t *);
    int (*lock)(const char *, fuse_file_info_t *, int, void *);
    int (*utimens)(const char *, const void *, fuse_file_info_t *);
    int (*bmap)(const char *, size_t, uint64_t *);
    int (*ioctl)(const char *, unsigned int, void *, fuse_file_info_t *,
                 unsigned int, void *);
    int (*poll)(const char *, fuse_file_info_t *, void *, unsigned *);
    int (*write_buf)(const char *, void *, off_t, fuse_file_info_t *);
    int (*read_buf)(const char *, void **, size_t, off_t,
                    fuse_file_info_t *);
    int (*flock)(const char *, fuse_file_info_t *, int);
    int (*fallocate)(const char *, int, off_t, off_t,
                     fuse_file_info_t *);
    int (*copy_file_range)(const char *, fuse_file_info_t *, off_t,
                           const char *, fuse_file_info_t *, off_t,
                           size_t, int);
    off_t (*lseek)(const char *, off_t, int, fuse_file_info_t *);
};

struct fuse_args {
    int    argc;
    char **argv;
    int    allocated;
};
#define FUSE_ARGS_INIT(argc, argv) { argc, argv, 0 }

/* ================================================================== */
/*  dlopen function pointer table                                     */
/* ================================================================== */

static void *fuse_lib_handle = NULL;

static int  (*dl_fuse_opt_add_arg)(struct fuse_args *, const char *);
static void (*dl_fuse_opt_free_args)(struct fuse_args *);
static int  (*dl_fuse_loop)(struct fuse *);
static void (*dl_fuse_exit)(struct fuse *);
static void (*dl_fuse_destroy)(struct fuse *);
static struct fuse_context *(*dl_fuse_get_context)(void);

static struct fuse_chan *(*dl_fuse2_mount)(const char *, struct fuse_args *);
static struct fuse *(*dl_fuse2_new)(struct fuse_chan *, struct fuse_args *,
                                    const void *, size_t, void *);
static void (*dl_fuse2_unmount)(const char *, struct fuse_chan *);

static struct fuse *(*dl_fuse3_new)(struct fuse_args *, const void *,
                                    size_t, void *);
static int  (*dl_fuse3_mount)(struct fuse *, const char *);
static void (*dl_fuse3_unmount)(struct fuse *);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
static int load_fuse_library(void)
{
    if (fuse_lib_handle) return fuse_api_version;

    const char *fuse3_names[] = {
        "libfuse3.so.3", "libfuse3.so", "libfuse3.dylib",
        "/usr/lib/x86_64-linux-gnu/libfuse3.so.3",
        "/usr/lib/aarch64-linux-gnu/libfuse3.so.3",
        "/usr/lib64/libfuse3.so.3",
        "/usr/lib/libfuse3.so.3",
        "/usr/local/lib/libfuse3.so.3",
        "/usr/local/lib/libfuse3.dylib",
        "/opt/homebrew/lib/libfuse3.dylib",
        NULL
    };
    for (const char **name = fuse3_names; *name; name++) {
        fuse_lib_handle = dlopen(*name, RTLD_LAZY);
        if (fuse_lib_handle) {
            if (dlsym(fuse_lib_handle, "fuse_session_mount")) {
                fuse_api_version = 3;
                goto resolve;
            }
            dlclose(fuse_lib_handle);
            fuse_lib_handle = NULL;
        }
    }

    const char *fuse2_names[] = {
        "libfuse-t.dylib", "libfuse.2.dylib", "libfuse.dylib",
        "/usr/local/lib/libfuse-t.dylib",
        "/usr/local/lib/libfuse.2.dylib",
        "/usr/local/lib/libfuse.dylib",
        "/opt/homebrew/lib/libfuse-t.dylib",
        "/opt/homebrew/lib/libfuse.dylib",
        "/Library/Frameworks/macFUSE.framework/Libraries/libfuse.2.dylib",
        "libfuse.so.2", "libfuse.so",
        NULL
    };
    for (const char **name = fuse2_names; *name; name++) {
        fuse_lib_handle = dlopen(*name, RTLD_LAZY);
        if (fuse_lib_handle) {
            fuse_api_version = 2;
            goto resolve;
        }
    }
    return 0;

resolve:
    dl_fuse_opt_add_arg   = dlsym(fuse_lib_handle, "fuse_opt_add_arg");
    dl_fuse_opt_free_args = dlsym(fuse_lib_handle, "fuse_opt_free_args");
    dl_fuse_loop          = dlsym(fuse_lib_handle, "fuse_loop");
    dl_fuse_exit          = dlsym(fuse_lib_handle, "fuse_exit");
    dl_fuse_destroy       = dlsym(fuse_lib_handle, "fuse_destroy");
    dl_fuse_get_context   = dlsym(fuse_lib_handle, "fuse_get_context");

    if (!dl_fuse_opt_add_arg || !dl_fuse_opt_free_args ||
        !dl_fuse_loop || !dl_fuse_exit || !dl_fuse_destroy ||
        !dl_fuse_get_context) {
        dlclose(fuse_lib_handle);
        fuse_lib_handle = NULL; fuse_api_version = 0;
        return 0;
    }

    if (fuse_api_version == 3) {
        dl_fuse3_new = dlsym(fuse_lib_handle, "fuse_new_31");
        if (!dl_fuse3_new)
            dl_fuse3_new = dlsym(fuse_lib_handle, "fuse_new");
        dl_fuse3_mount   = dlsym(fuse_lib_handle, "fuse_mount");
        dl_fuse3_unmount = dlsym(fuse_lib_handle, "fuse_unmount");
        if (!dl_fuse3_new || !dl_fuse3_mount || !dl_fuse3_unmount) {
            dlclose(fuse_lib_handle);
            fuse_lib_handle = NULL; fuse_api_version = 0;
            return 0;
        }
    } else {
        dl_fuse2_mount   = dlsym(fuse_lib_handle, "fuse_mount");
        dl_fuse2_new     = dlsym(fuse_lib_handle, "fuse_new");
        dl_fuse2_unmount = dlsym(fuse_lib_handle, "fuse_unmount");
        if (!dl_fuse2_mount || !dl_fuse2_new || !dl_fuse2_unmount) {
            dlclose(fuse_lib_handle);
            fuse_lib_handle = NULL; fuse_api_version = 0;
            return 0;
        }
    }
    return fuse_api_version;
}
#pragma GCC diagnostic pop

/* ================================================================== */
/*  libcurl ABI declarations & dlopen (replaces #include <curl/curl.h>) */
/* ================================================================== */

/* These values are part of libcurl's stable ABI (unchanged since the
 * libcurl.so.4 soname was introduced), so it is safe to hardcode them. */
typedef void CURL;
struct curl_slist;
typedef int CURLcode;
#define CURLE_OK                  0
#define CURLE_HTTP_RETURNED_ERROR 22
#define CURL_GLOBAL_DEFAULT       3L
#define CURLOPT_WRITEDATA         10001
#define CURLOPT_URL               10002
#define CURLOPT_RANGE             10007
#define CURLOPT_LOW_SPEED_LIMIT   19
#define CURLOPT_LOW_SPEED_TIME    20
#define CURLOPT_WRITEFUNCTION     20011
#define CURLOPT_HTTPHEADER        10023
#define CURLOPT_NOPROGRESS        43
#define CURLOPT_FAILONERROR       45
#define CURLOPT_FOLLOWLOCATION    52
#define CURLOPT_XFERINFODATA      10057
#define CURLOPT_SSL_VERIFYPEER    64
#define CURLOPT_CONNECTTIMEOUT    78
#define CURLOPT_SSL_VERIFYHOST    81
#define CURLOPT_NOSIGNAL          99
#define CURLOPT_XFERINFOFUNCTION  20219
#define CURLINFO_RESPONSE_CODE    0x200002

static void *curl_lib_handle = NULL;

static CURLcode (*dl_curl_global_init)(long);
static CURL *(*dl_curl_easy_init)(void);
static CURLcode (*dl_curl_easy_setopt)(CURL *, int, ...);
static CURLcode (*dl_curl_easy_perform)(CURL *);
static CURLcode (*dl_curl_easy_getinfo)(CURL *, int, ...);
static void (*dl_curl_easy_cleanup)(CURL *);
static struct curl_slist *(*dl_curl_slist_append)(struct curl_slist *,
                                                  const char *);
static void (*dl_curl_slist_free_all)(struct curl_slist *);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
/* Must be called from the main thread: curl_global_init is not thread-safe. */
static int load_curl_library(void)
{
    if (curl_lib_handle) return 1;

    const char *curl_names[] = {
        "libcurl.so.4", "libcurl-gnutls.so.4", "libcurl-nss.so.4",
        "libcurl.so",
        "libcurl.4.dylib", "libcurl.dylib",
        "/usr/lib/libcurl.4.dylib",
        "/opt/homebrew/opt/curl/lib/libcurl.4.dylib",
        "/usr/local/opt/curl/lib/libcurl.4.dylib",
        NULL
    };
#ifdef RTLD_NOLOAD
    /* Prefer a copy that is already loaded (e.g., by the curl R package). */
    for (const char **name = curl_names; *name && !curl_lib_handle; name++)
        curl_lib_handle = dlopen(*name, RTLD_LAZY | RTLD_NOLOAD);
#endif
    for (const char **name = curl_names; *name && !curl_lib_handle; name++)
        curl_lib_handle = dlopen(*name, RTLD_LAZY);
    if (!curl_lib_handle) return 0;

    dl_curl_global_init    = dlsym(curl_lib_handle, "curl_global_init");
    dl_curl_easy_init      = dlsym(curl_lib_handle, "curl_easy_init");
    dl_curl_easy_setopt    = dlsym(curl_lib_handle, "curl_easy_setopt");
    dl_curl_easy_perform   = dlsym(curl_lib_handle, "curl_easy_perform");
    dl_curl_easy_getinfo   = dlsym(curl_lib_handle, "curl_easy_getinfo");
    dl_curl_easy_cleanup   = dlsym(curl_lib_handle, "curl_easy_cleanup");
    dl_curl_slist_append   = dlsym(curl_lib_handle, "curl_slist_append");
    dl_curl_slist_free_all = dlsym(curl_lib_handle, "curl_slist_free_all");

    if (!dl_curl_global_init || !dl_curl_easy_init || !dl_curl_easy_setopt ||
        !dl_curl_easy_perform || !dl_curl_easy_getinfo || !dl_curl_easy_cleanup ||
        !dl_curl_slist_append || !dl_curl_slist_free_all ||
        dl_curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        dlclose(curl_lib_handle);
        curl_lib_handle = NULL;
        return 0;
    }
    return 1;
}
#pragma GCC diagnostic pop

/* ------------------------------------------------------------------ */
/*  Constants & data structures                                       */
/* ------------------------------------------------------------------ */

/* Files are downloaded into the local cache in blocks of this size, and a read is served once every
   block it touches has been cached. The cache's layout (and so this) is kept in step with
   redivis-python's mount_directory.py, so the two can share a cache_dir. */
#define DEFAULT_BLOCK_SIZE (4 * 1024 * 1024)

/* A read up to this many blocks past where an open sequential stream has reached is served by the
   stream, rather than abandoning it for a new request. This absorbs the kernel's readahead requests
   arriving slightly out of order. */
#define MAX_STREAM_SKIP_BLOCKS 4

/* A stream downloads up to this many blocks past the furthest one a reader has asked for, then
   pauses until reads catch up, so that a file that's only partly read isn't downloaded in full. */
#define STREAM_READAHEAD_BLOCKS 2

/* Open sequential streams kept per file, so that a few interleaved sequential readers (e.g.
   parallel column chunk reads of a parquet file) each keep their own request. */
#define MAX_STREAMS_PER_FILE 4

/* Files too large for the cache are read through a buffer of this many recent blocks instead. It
   holds every block a single read touches, including those a stream reads ahead. */
#define MEMORY_BUFFER_BLOCKS (MAX_STREAM_SKIP_BLOCKS + STREAM_READAHEAD_BLOCKS + 2)

/* Once the cache exceeds its maximum size, files are evicted until it is back under this fraction
   of it, so that eviction runs occasionally rather than on every block downloaded. */
#define EVICTION_TARGET 0.9

/* Without an explicit cache_dir, a mount caches files in a directory of its own in the system's
   temporary directory, named with this prefix. It holds a lock on the TEMPORARY_CACHE_LOCK file inside
   for as long as its process is alive, so that a later mount can remove the cache of a process that
   was killed while mounted (e.g. by a notebook kernel restart), and so never removed its own. Keep in
   step with redivis-python's mount_directory.py, so that each cleans up after the other. */
#define TEMPORARY_CACHE_PREFIX "redivis_mount_cache_"
#define TEMPORARY_CACHE_LOCK ".lock"

/* Consecutive failed requests (ones that received nothing) before a read gives up */
#define MAX_RETRIES 10

typedef struct redivis_mount_ctx redivis_mount_ctx_t;
typedef struct redivis_content redivis_content_t;

/* A single open-ended request for a file, run on its own thread, that caches each block as it
   arrives. It resumes, with a Range request from exactly where it left off, if the connection
   drops or the server times out while it's paused. */
typedef struct redivis_stream {
    redivis_content_t *content;
    pthread_t thread;
    int fd;             /* The cache file (-1 if in memory), as it was when the stream opened */
    size_t next_block;  /* The block being received */
    size_t want_block;  /* The furthest block a reader has asked the stream for */
    int abandon;        /* Set to stop the stream; read with atomics outside the mutex */
    int done, failed;
} redivis_stream_t;

typedef struct {
    long block; unsigned char *data; unsigned long last_use;
} memory_slot_t;

/* The cache for one file's contents, shared by every path with those contents. Its contents are in
   "<key>.data", and which blocks have been downloaded is tracked alongside it in "<key>.blocks" (one
   byte per block), so a cache_dir that outlives the mount can be reused by later mounts. A file too
   large for the cache altogether is instead read through a small in-memory buffer of recent blocks. */
struct redivis_content {
    redivis_mount_ctx_t *ctx;
    char *key; char *file_id; size_t size; size_t n_blocks;
    char *data_path; char *blocks_path;
    /* Guards everything below, and is waited on (via cond) for blocks to arrive */
    pthread_mutex_t mutex; pthread_cond_t cond;
    unsigned char *blocks;    /* 1 for each block cached on disk */
    unsigned char *fetching;  /* 1 for each block a bounded request is fetching */
    int blocks_loaded;
    int open_count; int fd; int in_memory;
    memory_slot_t memory[MEMORY_BUFFER_BLOCKS]; unsigned long memory_clock;
    redivis_stream_t *streams[MAX_STREAMS_PER_FILE]; int n_streams;
    size_t size_on_disk;
    int64_t last_used;        /* In nanoseconds; accessed with atomics */
};

typedef struct redivis_file_entry {
    char *rel_path; size_t size; time_t added_at;
    redivis_content_t *content;
} redivis_file_entry_t;

typedef struct hash_node {
    const char *key; void *value; struct hash_node *next;
} hash_node_t;

typedef struct hash_table {
    hash_node_t **buckets; size_t size;
} hash_table_t;

typedef struct dir_entry {
    char *path; char **child_names; int *child_is_dir; size_t n_children;
} dir_entry_t;

struct redivis_mount_ctx {
    char *mount_point; char *api_base_url; int verify_ssl;
    /* Replaced by C_fuse_set_auth_token while mounted, so always read under auth_mutex */
    char *auth_token; pthread_mutex_t auth_mutex;
    redivis_file_entry_t *entries; size_t n_entries;
    hash_table_t file_ht; hash_table_t dir_ht;
    dir_entry_t *dirs; size_t n_dirs;

    /* A temporary cache_dir is removed with the mount, and locked by cache_lock_fd until then */
    char *cache_dir; int remove_cache_dir; int cache_lock_fd; size_t block_size;
    redivis_content_t **contents; size_t n_contents; size_t contents_cap;
    hash_table_t content_ht;
    /* The total size of the cache, and whether a thread is evicting from it, guarded by cache_mutex */
    double max_cache_size; int64_t cache_size; int evicting;
    pthread_mutex_t cache_mutex;

    struct fuse *fuse; struct fuse_chan *chan;
    int api_version; pthread_t thread; int running;
    /* Whether mount() created the mount point, and so removes it again on unmount */
    int remove_mount_point;
    /* Set once mounting succeeds: the FUSE thread must then be joined before ctx is freed */
    int thread_live;
    char error_msg[512];
    /* When mount() was called, reported as every inode's st_ctime. See getattr_impl. */
    time_t mounted_at;
    /* Condition variable to signal mount success/failure without polling */
    pthread_mutex_t startup_mutex;
    pthread_cond_t  startup_cond;
    int startup_done;  /* 1 once the thread has set running or error_msg */
};

/* ------------------------------------------------------------------ */
/*  Hash table                                                        */
/* ------------------------------------------------------------------ */

static unsigned int hash_str(const char *s) {
    unsigned int h = 5381;
    while (*s) h = ((h << 5) + h) ^ (unsigned char)*s++;
    return h;
}

static int is_prime(size_t n) {
    if (n < 2) return 0; if (n < 4) return 1;
    if (n % 2 == 0 || n % 3 == 0) return 0;
    for (size_t i = 5; i * i <= n; i += 6)
        if (n % i == 0 || n % (i + 2) == 0) return 0;
    return 1;
}

static size_t next_prime(size_t n) {
    if (n <= 2) return 2;
    if (n % 2 == 0) n++;
    while (!is_prime(n)) n += 2;
    return n;
}

static void ht_init(hash_table_t *ht, size_t n) {
    ht->size = next_prime(n < 16 ? 31 : n * 2);
    ht->buckets = calloc(ht->size, sizeof(hash_node_t *));
}

static void ht_insert(hash_table_t *ht, const char *key, void *value) {
    unsigned int idx = hash_str(key) % ht->size;
    hash_node_t *node = malloc(sizeof(hash_node_t));
    node->key = key; node->value = value;
    node->next = ht->buckets[idx]; ht->buckets[idx] = node;
}

static void *ht_lookup(hash_table_t *ht, const char *key) {
    unsigned int idx = hash_str(key) % ht->size;
    for (hash_node_t *n = ht->buckets[idx]; n; n = n->next)
        if (strcmp(n->key, key) == 0) return n->value;
    return NULL;
}

static void ht_free(hash_table_t *ht) {
    if (!ht->buckets) return;
    for (size_t i = 0; i < ht->size; i++) {
        hash_node_t *n = ht->buckets[i];
        while (n) { hash_node_t *next = n->next; free(n); n = next; }
    }
    free(ht->buckets); ht->buckets = NULL; ht->size = 0;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

static redivis_file_entry_t *find_entry(redivis_mount_ctx_t *ctx,
                                        const char *path) {
    const char *rel = path;
    while (*rel == '/') rel++;
    return (redivis_file_entry_t *)ht_lookup(&ctx->file_ht, rel);
}

static void mkdirs(const char *dir) {
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s", dir);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    mkdir(tmp, 0755);
}

/* Removes a temporary cache directory, which only ever holds files */
static void remove_temporary_cache_dir(const char *dir) {
    DIR *d = opendir(dir);
    if (d) {
        struct dirent *e;
        char path[4096];
        while ((e = readdir(d)) != NULL) {
            if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
            snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
            unlink(path);
        }
        closedir(d);
    }
    rmdir(dir);
}

/* Removes the temporary caches in parent of processes that exited while mounted */
static void remove_orphaned_cache_dirs(const char *parent) {
    DIR *d = opendir(parent);
    if (!d) return;
    struct dirent *e;
    char path[4096], lock_path[4096 + sizeof(TEMPORARY_CACHE_LOCK) + 1];
    while ((e = readdir(d)) != NULL) {
        if (strncmp(e->d_name, TEMPORARY_CACHE_PREFIX, strlen(TEMPORARY_CACHE_PREFIX)) != 0) continue;
        snprintf(path, sizeof(path), "%s/%s", parent, e->d_name);
        /* Only this user's own directories, so as never to follow a link somewhere else */
        struct stat st;
        if (lstat(path, &st) != 0 || !S_ISDIR(st.st_mode) || st.st_uid != getuid()) continue;
        snprintf(lock_path, sizeof(lock_path), "%s/" TEMPORARY_CACHE_LOCK, path);
        /* Missing if not a cache set up this way, or not yet */
        int fd = open(lock_path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
        if (fd < 0) continue;
        char pid[32];
        /* Locked by a live process, or empty while its owner is still setting it up */
        if (flock(fd, LOCK_EX | LOCK_NB) == 0 && read(fd, pid, sizeof(pid)) > 0)
            remove_temporary_cache_dir(path);
        close(fd);
    }
    closedir(d);
}

/* Makes a temporary cache directory in parent, locked by *lock_fd for as long as this process is
   alive. Returns its path, or NULL with errno set. */
static char *make_temporary_cache_dir(const char *parent, int *lock_fd) {
    remove_orphaned_cache_dirs(parent);
    char path[4096], lock_path[4096 + sizeof(TEMPORARY_CACHE_LOCK) + 1];
    if (snprintf(path, sizeof(path), "%s/" TEMPORARY_CACHE_PREFIX "XXXXXX", parent)
        >= (int)sizeof(path)) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    if (!mkdtemp(path)) return NULL;
    snprintf(lock_path, sizeof(lock_path), "%s/" TEMPORARY_CACHE_LOCK, path);
    int fd = open(lock_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    char pid[32];
    int len = snprintf(pid, sizeof(pid), "%ld\n", (long)getpid());
    /* Written once locked, since until then another process could take the lock itself */
    if (fd < 0 || flock(fd, LOCK_EX) != 0 || write(fd, pid, (size_t)len) != len) {
        int err = errno;
        if (fd >= 0) close(fd);
        remove_temporary_cache_dir(path);
        errno = err;
        return NULL;
    }
    char *result = strdup(path);
    if (!result) {
        close(fd);
        remove_temporary_cache_dir(path);
        errno = ENOMEM;
        return NULL;
    }
    *lock_fd = fd;
    return result;
}

/* Whether dir is the root of a mount, i.e. on a different device from its parent */
static int is_mount_point(const char *dir) {
    char parent[4096];
    struct stat st, parent_st;
    if (snprintf(parent, sizeof(parent), "%s/..", dir) >= (int)sizeof(parent)) return 0;
    if (stat(dir, &st) != 0 || stat(parent, &parent_st) != 0) return 0;
    return st.st_dev != parent_st.st_dev;
}

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static int pwrite_all(int fd, const char *buf, size_t len, size_t offset) {
    while (len > 0) {
        ssize_t n = pwrite(fd, buf, len, (off_t)offset);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        buf += n; len -= (size_t)n; offset += (size_t)n;
    }
    return 0;
}

static size_t block_length(redivis_content_t *c, size_t block) {
    size_t start = block * c->ctx->block_size;
    return c->size - start < c->ctx->block_size ? c->size - start : c->ctx->block_size;
}

/* ------------------------------------------------------------------ */
/*  Cache: block maps, memory buffers, and eviction                   */
/* ------------------------------------------------------------------ */

static void blocks_save(redivis_content_t *c) {
    FILE *fp = fopen(c->blocks_path, "wb");
    if (fp) { fwrite(c->blocks, 1, c->n_blocks, fp); fclose(fp); }
}

static void blocks_load(redivis_content_t *c) {
    memset(c->blocks, 0, c->n_blocks);
    FILE *fp = fopen(c->blocks_path, "rb");
    if (!fp) return;
    size_t rd = fread(c->blocks, 1, c->n_blocks, fp);
    int extra = fgetc(fp) != EOF;
    fclose(fp);
    struct stat st;
    if (rd != c->n_blocks || extra || stat(c->data_path, &st) != 0) {
        memset(c->blocks, 0, c->n_blocks);
        return;
    }
    /* Left over from an interrupted write, or otherwise inconsistent; start over */
    for (size_t b = c->n_blocks; b-- > 0;) {
        if (!c->blocks[b]) continue;
        if ((size_t)st.st_size < b * c->ctx->block_size + block_length(c, b))
            memset(c->blocks, 0, c->n_blocks);
        break;
    }
}

static memory_slot_t *memory_find(redivis_content_t *c, size_t block) {
    for (int i = 0; i < MEMORY_BUFFER_BLOCKS; i++)
        if (c->memory[i].data && c->memory[i].block == (long)block) return &c->memory[i];
    return NULL;
}

/* Takes ownership of data, replacing the least recently used block if the buffer is full */
static void memory_store(redivis_content_t *c, size_t block, unsigned char *data) {
    memory_slot_t *slot = memory_find(c, block);
    for (int i = 0; !slot && i < MEMORY_BUFFER_BLOCKS; i++)
        if (!c->memory[i].data) slot = &c->memory[i];
    if (!slot) {
        slot = &c->memory[0];
        for (int i = 1; i < MEMORY_BUFFER_BLOCKS; i++)
            if (c->memory[i].last_use < slot->last_use) slot = &c->memory[i];
    }
    free(slot->data);
    slot->data = data; slot->block = (long)block; slot->last_use = ++c->memory_clock;
}

static void memory_free(redivis_content_t *c) {
    for (int i = 0; i < MEMORY_BUFFER_BLOCKS; i++) {
        free(c->memory[i].data); c->memory[i].data = NULL; c->memory[i].block = -1;
    }
}

static int has_block(redivis_content_t *c, size_t block) {
    return c->in_memory ? memory_find(c, block) != NULL : c->blocks[block];
}

/* Deletes a file's cached copy, unless it's in use, returning the space freed */
static size_t content_evict(redivis_content_t *c) {
    if (pthread_mutex_trylock(&c->mutex) != 0) return 0;
    size_t freed = 0;
    if (c->open_count == 0 &&
        (unlink(c->data_path) == 0 || errno == ENOENT) &&
        (unlink(c->blocks_path) == 0 || errno == ENOENT)) {
        freed = c->size_on_disk;
        c->size_on_disk = 0; c->blocks_loaded = 0;
    }
    pthread_mutex_unlock(&c->mutex);
    return freed;
}

typedef struct { redivis_content_t *content; int64_t last_used; } eviction_candidate_t;

static int compare_last_used(const void *a, const void *b) {
    int64_t x = ((const eviction_candidate_t *)a)->last_used;
    int64_t y = ((const eviction_candidate_t *)b)->last_used;
    return (x > y) - (x < y);
}

/* Records bytes added to the cache, evicting the least recently used files once it exceeds its
   maximum size. Only closed files are evicted, so the cache can run over while large files are
   open. Safe to call with a content's mutex held, since eviction never waits on one. */
static void cache_add(redivis_mount_ctx_t *ctx, int64_t bytes) {
    pthread_mutex_lock(&ctx->cache_mutex);
    ctx->cache_size += bytes;
    int evict = (double)ctx->cache_size > ctx->max_cache_size && !ctx->evicting;
    /* Only one thread evicts at a time; others carry on without waiting for it */
    if (evict) ctx->evicting = 1;
    pthread_mutex_unlock(&ctx->cache_mutex);
    if (!evict) return;

    eviction_candidate_t *order = malloc(ctx->n_contents * sizeof(*order));
    if (order) {
        for (size_t i = 0; i < ctx->n_contents; i++) {
            order[i].content = ctx->contents[i];
            order[i].last_used = __atomic_load_n(&ctx->contents[i]->last_used, __ATOMIC_RELAXED);
        }
        qsort(order, ctx->n_contents, sizeof(*order), compare_last_used);
        for (size_t i = 0; i < ctx->n_contents; i++) {
            pthread_mutex_lock(&ctx->cache_mutex);
            int under = (double)ctx->cache_size <= ctx->max_cache_size * EVICTION_TARGET;
            pthread_mutex_unlock(&ctx->cache_mutex);
            if (under) break;
            size_t freed = content_evict(order[i].content);
            if (freed) {
                pthread_mutex_lock(&ctx->cache_mutex);
                ctx->cache_size -= (int64_t)freed;
                pthread_mutex_unlock(&ctx->cache_mutex);
            }
        }
        free(order);
    }
    pthread_mutex_lock(&ctx->cache_mutex);
    ctx->evicting = 0;
    pthread_mutex_unlock(&ctx->cache_mutex);
}

/* ------------------------------------------------------------------ */
/*  Downloads via libcurl                                             */
/* ------------------------------------------------------------------ */

/* One range of a file being downloaded into the cache, over as many requests as it takes */
typedef struct {
    redivis_content_t *content;
    redivis_stream_t *stream;   /* NULL for a bounded request */
    size_t pos, end;            /* The next byte to receive, and the last byte wanted */
    int fd;                     /* The cache file, or -1 when in memory */
    unsigned char *block_buf;   /* In memory: the block being received */
    CURL *curl;
    int status_checked, complete, abandoned;
    size_t discard;             /* Bytes to skip from the start of the response */
} transfer_t;

static int stream_abandoned(redivis_stream_t *s) {
    return s && __atomic_load_n(&s->abandon, __ATOMIC_RELAXED);
}

/* Called once a transfer has received all of block b. Returns 0 if the transfer should stop. */
static int transfer_block_done(transfer_t *t, size_t b) {
    redivis_content_t *c = t->content;
    redivis_stream_t *s = t->stream;
    pthread_mutex_lock(&c->mutex);
    if (!(s && s->abandon)) {
        if (c->in_memory) {
            memory_store(c, b, t->block_buf);
            t->block_buf = NULL;
        } else if (!c->blocks[b]) {
            c->blocks[b] = 1;
            c->size_on_disk += block_length(c, b);
            blocks_save(c);
            /* Before waking readers, so that any eviction has happened by the time they see it */
            cache_add(c->ctx, (int64_t)block_length(c, b));
        }
    }
    int keep_going = 1;
    if (s) {
        s->next_block = b + 1;
        pthread_cond_broadcast(&c->cond);
        while (!s->abandon && b + 1 < c->n_blocks &&
               b + 1 > s->want_block + STREAM_READAHEAD_BLOCKS)
            pthread_cond_wait(&c->cond, &c->mutex);
        keep_going = !s->abandon;
    } else {
        pthread_cond_broadcast(&c->cond);
    }
    pthread_mutex_unlock(&c->mutex);
    return keep_going;
}

static size_t transfer_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    transfer_t *t = (transfer_t *)userdata;
    redivis_content_t *c = t->content;
    size_t total = size * nmemb, done = 0;

    if (!t->status_checked) {
        long code = 0;
        dl_curl_easy_getinfo(t->curl, CURLINFO_RESPONSE_CODE, &code);
        if (code != 206 && code != 200) return 0;
        /* The whole file, rather than the range asked for (as storage sends for some encoded files),
           so skip ahead to where this is reading from */
        if (code == 200) t->discard = t->pos;
        t->status_checked = 1;
    }

    if (t->discard > 0) {
        size_t skip = total < t->discard ? total : t->discard;
        t->discard -= skip; done += skip;
    }
    while (done < total) {
        if (t->pos > t->end) {
            /* More than was asked for; everything wanted has arrived */
            t->complete = 1;
            return 0;
        }
        size_t b = t->pos / c->ctx->block_size;
        size_t block_end = b * c->ctx->block_size + block_length(c, b);
        size_t piece = total - done;
        if (piece > block_end - t->pos) piece = block_end - t->pos;
        if (t->fd >= 0) {
            if (pwrite_all(t->fd, ptr + done, piece, t->pos) != 0) return 0;
        } else {
            if (!t->block_buf && !(t->block_buf = malloc(c->ctx->block_size))) return 0;
            memcpy(t->block_buf + (t->pos - b * c->ctx->block_size), ptr + done, piece);
        }
        t->pos += piece; done += piece;
        if (t->pos == block_end && !transfer_block_done(t, b)) {
            t->abandoned = 1;
            return 0;
        }
    }
    return total;
}

static int transfer_progress_cb(void *userdata, int64_t dltotal, int64_t dlnow,
                                int64_t ultotal, int64_t ulnow) {
    (void)dltotal; (void)dlnow; (void)ultotal; (void)ulnow;
    /* Stop promptly once a stream is closed, even while connecting or waiting on the server */
    return stream_abandoned(((transfer_t *)userdata)->stream);
}

/* Makes one request for the rest of the transfer. Returns 0 once it has everything it asked for,
   1 if another request is worth trying, and -1 otherwise. */
static int transfer_attempt(transfer_t *t) {
    redivis_mount_ctx_t *ctx = t->content->ctx;
    CURL *curl = dl_curl_easy_init();
    if (!curl) return 1;

    char url[4096];
    snprintf(url, sizeof(url), "%s/rawFiles/%s", ctx->api_base_url, t->content->file_id);
    char range[64];
    if (t->stream) snprintf(range, sizeof(range), "%zu-", t->pos);
    else snprintf(range, sizeof(range), "%zu-%zu", t->pos, t->end);

    struct curl_slist *hdrs = NULL;
    pthread_mutex_lock(&ctx->auth_mutex);
    if (ctx->auth_token[0]) {
        char auth_hdr[4096];
        snprintf(auth_hdr, sizeof(auth_hdr), "Authorization: Bearer %s", ctx->auth_token);
        hdrs = dl_curl_slist_append(hdrs, auth_hdr);
    }
    pthread_mutex_unlock(&ctx->auth_mutex);

    t->curl = curl; t->status_checked = 0; t->complete = 0; t->abandoned = 0; t->discard = 0;
    dl_curl_easy_setopt(curl, CURLOPT_URL, url);
    if (hdrs) dl_curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
    dl_curl_easy_setopt(curl, CURLOPT_RANGE, range);
    dl_curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, transfer_write_cb);
    dl_curl_easy_setopt(curl, CURLOPT_WRITEDATA, t);
    dl_curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    dl_curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    /* Required in a multithreaded program, where curl's signal-based timeouts aren't safe */
    dl_curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    dl_curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    /* A stall rather than a total time limit, since a stream stays open for as long as it's read.
       One left paused past this is dropped, and resumed when reads reach it. */
    dl_curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
    dl_curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 60L);
    dl_curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    dl_curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, transfer_progress_cb);
    dl_curl_easy_setopt(curl, CURLOPT_XFERINFODATA, t);
    if (!ctx->verify_ssl) {
        dl_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        dl_curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }

    CURLcode res = dl_curl_easy_perform(curl);
    long code = 0;
    dl_curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    dl_curl_easy_cleanup(curl);
    dl_curl_slist_free_all(hdrs);
    t->curl = NULL;

    if (t->complete || (res == CURLE_OK && t->pos > t->end)) return 0;
    if (t->abandoned || stream_abandoned(t->stream)) return -1;
    /* Client errors (other than rate limiting) won't be resolved by retrying */
    if (res == CURLE_HTTP_RETURNED_ERROR && code >= 400 && code < 500 && code != 429) return -1;
    return 1;
}

/* Returns 1 if the transfer's stream was closed while waiting */
static int transfer_backoff(transfer_t *t, int seconds) {
    redivis_content_t *c = t->content;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += seconds;
    pthread_mutex_lock(&c->mutex);
    while (!(t->stream && t->stream->abandon))
        if (pthread_cond_timedwait(&c->cond, &c->mutex, &ts) == ETIMEDOUT) break;
    int abandoned = t->stream && t->stream->abandon;
    pthread_mutex_unlock(&c->mutex);
    return abandoned;
}

/* Makes requests until the transfer has everything it asked for, each resuming where the last left
   off. A request that received something is retried straight away. Returns 0 on success. */
static int transfer_run(transfer_t *t) {
    int failures = 0;
    for (;;) {
        size_t pos_before = t->pos;
        int rc = transfer_attempt(t);
        if (rc <= 0) return rc;
        if (t->pos > pos_before) { failures = 0; continue; }
        if (++failures > MAX_RETRIES) return -1;
        if (transfer_backoff(t, failures)) return -1;
    }
}

/* ------------------------------------------------------------------ */
/*  Streams                                                           */
/* ------------------------------------------------------------------ */

static void *stream_thread_func(void *arg) {
    redivis_stream_t *s = (redivis_stream_t *)arg;
    redivis_content_t *c = s->content;
    transfer_t t;
    memset(&t, 0, sizeof(t));
    t.content = c; t.stream = s; t.fd = s->fd;
    t.pos = s->next_block * c->ctx->block_size; t.end = c->size - 1;
    int rc = transfer_run(&t);
    free(t.block_buf);

    pthread_mutex_lock(&c->mutex);
    s->done = 1;
    s->failed = rc != 0 && !s->abandon;
    pthread_cond_broadcast(&c->cond);
    pthread_mutex_unlock(&c->mutex);
    return NULL;
}

/* The functions below are called with c->mutex held. Streams they stop are added to reap, to be
   joined once it's released (they need it to finish). */

#define MAX_REAP (MAX_STREAMS_PER_FILE * 2)

static void stream_remove(redivis_content_t *c, int i, redivis_stream_t **reap, int *n_reap) {
    redivis_stream_t *s = c->streams[i];
    __atomic_store_n(&s->abandon, 1, __ATOMIC_RELAXED);
    reap[(*n_reap)++] = s;
    memmove(&c->streams[i], &c->streams[i + 1], (size_t)(c->n_streams - i - 1) * sizeof(s));
    c->n_streams--;
    pthread_cond_broadcast(&c->cond);
}

static void streams_reap_done(redivis_content_t *c, redivis_stream_t **reap, int *n_reap) {
    for (int i = c->n_streams - 1; i >= 0 && *n_reap < MAX_REAP; i--)
        if (c->streams[i]->done) stream_remove(c, i, reap, n_reap);
}

static redivis_stream_t *stream_find(redivis_content_t *c, size_t block) {
    int found = -1;
    for (int i = 0; i < c->n_streams; i++) {
        redivis_stream_t *s = c->streams[i];
        if (!s->done && s->next_block <= block && block <= s->next_block + MAX_STREAM_SKIP_BLOCKS &&
            (found < 0 || s->next_block > c->streams[found]->next_block))
            found = i;
    }
    if (found < 0) return NULL;
    /* Keep the list in least-recently-used order */
    redivis_stream_t *s = c->streams[found];
    memmove(&c->streams[found], &c->streams[found + 1],
            (size_t)(c->n_streams - found - 1) * sizeof(s));
    c->streams[c->n_streams - 1] = s;
    return s;
}

/* Whether a stream that would have served block has given up */
static int stream_failed_at(redivis_content_t *c, size_t block) {
    for (int i = 0; i < c->n_streams; i++) {
        redivis_stream_t *s = c->streams[i];
        if (s->failed && s->next_block <= block && block <= s->next_block + MAX_STREAM_SKIP_BLOCKS)
            return 1;
    }
    return 0;
}

static redivis_stream_t *stream_open(redivis_content_t *c, size_t block,
                                     redivis_stream_t **reap, int *n_reap) {
    if (*n_reap >= MAX_REAP) return NULL;
    if (c->n_streams >= MAX_STREAMS_PER_FILE) stream_remove(c, 0, reap, n_reap);
    redivis_stream_t *s = calloc(1, sizeof(redivis_stream_t));
    if (!s) return NULL;
    s->content = c; s->fd = c->fd; s->next_block = block; s->want_block = block;
    if (pthread_create(&s->thread, NULL, stream_thread_func, s) != 0) { free(s); return NULL; }
    c->streams[c->n_streams++] = s;
    return s;
}

static void streams_join(redivis_stream_t **reap, int n_reap) {
    for (int i = 0; i < n_reap; i++) { pthread_join(reap[i]->thread, NULL); free(reap[i]); }
}

/* ------------------------------------------------------------------ */
/*  Reading through the cache                                         */
/* ------------------------------------------------------------------ */

/* Downloads whichever of blocks first..last aren't cached. Returns 0 or a negative errno. */
static int content_ensure(redivis_content_t *c, size_t first, size_t last) {
    redivis_stream_t *reap[MAX_REAP]; int n_reap = 0;
    int rc = 0;
    size_t bs = c->ctx->block_size;
    pthread_mutex_lock(&c->mutex);
    /* Let streams reading ahead of this read carry on, so they stay ahead of it */
    for (int i = 0; i < c->n_streams; i++) {
        redivis_stream_t *s = c->streams[i];
        if (first < s->next_block && s->next_block <= last + STREAM_READAHEAD_BLOCKS + 1 &&
            s->want_block < last) {
            s->want_block = last;
            pthread_cond_broadcast(&c->cond);
        }
    }
    for (size_t b = first; b <= last && rc == 0; b++) {
        while (!has_block(c, b)) {
            if (c->fetching[b]) {
                pthread_cond_wait(&c->cond, &c->mutex);
                continue;
            }
            if (stream_failed_at(c, b)) { rc = -EIO; break; }
            streams_reap_done(c, reap, &n_reap);
            redivis_stream_t *s = stream_find(c, b);
            if (!s && (b == 0 || has_block(c, b - 1)))
                /* A read continuing on from cached data (or from the start of the file) looks
                   sequential, so request the rest of the file in one go rather than block by block */
                s = stream_open(c, b, reap, &n_reap);
            if (s) {
                if (b > s->want_block) { s->want_block = b; pthread_cond_broadcast(&c->cond); }
                /* Other readers can stop and free the stream while this waits, so rather than wait on
                   it specifically, look again from the top once anything changes */
                pthread_cond_wait(&c->cond, &c->mutex);
                continue;
            }

            /* Random access: fetch just the missing blocks this read needs, in one bounded request */
            size_t end = b;
            while (end < last && !has_block(c, end + 1) && !c->fetching[end + 1]) end++;
            for (size_t i = b; i <= end; i++) c->fetching[i] = 1;
            transfer_t t;
            memset(&t, 0, sizeof(t));
            t.content = c; t.fd = c->in_memory ? -1 : c->fd;
            t.pos = b * bs; t.end = end * bs + block_length(c, end) - 1;
            pthread_mutex_unlock(&c->mutex);
            int trc = transfer_run(&t);
            free(t.block_buf);
            pthread_mutex_lock(&c->mutex);
            for (size_t i = b; i <= end; i++) c->fetching[i] = 0;
            pthread_cond_broadcast(&c->cond);
            if (trc != 0) { rc = -EIO; break; }
        }
    }
    streams_reap_done(c, reap, &n_reap);
    pthread_mutex_unlock(&c->mutex);
    streams_join(reap, n_reap);
    return rc;
}

static int content_read(redivis_content_t *c, char *buf, size_t size, size_t offset) {
    if (offset >= c->size || size == 0) return 0;
    if (size > c->size - offset) size = c->size - offset;
    size_t bs = c->ctx->block_size;
    size_t first = offset / bs, last = (offset + size - 1) / bs;
    __atomic_store_n(&c->last_used, now_ns(), __ATOMIC_RELAXED);

    if (!c->in_memory) {
        int rc = content_ensure(c, first, last);
        if (rc != 0) return rc;
        size_t got = 0;
        while (got < size) {
            ssize_t n = pread(c->fd, buf + got, size - got, (off_t)(offset + got));
            if (n < 0) { if (errno == EINTR) continue; return -errno; }
            if (n == 0) return -EIO;
            got += (size_t)n;
        }
        return (int)size;
    }

    /* In memory, a block at a time, since the buffer needn't hold every block of a large read at once,
       and a block can be pushed out of it before it's copied (in which case, try again) */
    for (size_t got = 0; got < size;) {
        size_t pos = offset + got, b = pos / bs, start = pos - b * bs;
        size_t piece = block_length(c, b) - start;
        if (piece > size - got) piece = size - got;
        int copied = 0;
        for (int attempt = 0; attempt < 3 && !copied; attempt++) {
            int rc = content_ensure(c, b, b);
            if (rc != 0) return rc;
            pthread_mutex_lock(&c->mutex);
            memory_slot_t *slot = memory_find(c, b);
            if (slot) { memcpy(buf + got, slot->data + start, piece); copied = 1; }
            pthread_mutex_unlock(&c->mutex);
        }
        if (!copied) return -EIO;
        got += piece;
    }
    return (int)size;
}

static int content_open(redivis_content_t *c) {
    pthread_mutex_lock(&c->mutex);
    if (c->open_count == 0) {
        c->in_memory = (double)c->size > c->ctx->max_cache_size;
        if (!c->in_memory) {
            int fd = open(c->data_path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
            if (fd < 0) { int e = errno; pthread_mutex_unlock(&c->mutex); return -e; }
            c->fd = fd;
            if (!c->blocks_loaded) { blocks_load(c); c->blocks_loaded = 1; }
        }
    }
    c->open_count++;
    __atomic_store_n(&c->last_used, now_ns(), __ATOMIC_RELAXED);
    pthread_mutex_unlock(&c->mutex);
    return 0;
}

static void content_release(redivis_content_t *c) {
    redivis_stream_t *reap[MAX_REAP]; int n_reap = 0;
    int fd = -1, closed;
    pthread_mutex_lock(&c->mutex);
    closed = --c->open_count == 0;
    if (closed) {
        while (c->n_streams > 0) stream_remove(c, 0, reap, &n_reap);
        /* Streams store nothing once abandoned, so the buffer can go before they've finished */
        memory_free(c);
        fd = c->fd; c->fd = -1;
    }
    pthread_mutex_unlock(&c->mutex);
    streams_join(reap, n_reap);
    if (fd >= 0) close(fd);
    /* Now evictable, so if the cache ran over while it was open, it may be what brings it back under */
    if (closed) cache_add(c->ctx, 0);
}

static redivis_content_t *content_add(redivis_mount_ctx_t *ctx, const char *key,
                                      const char *file_id, size_t size) {
    if (ctx->n_contents == ctx->contents_cap) {
        size_t cap = ctx->contents_cap ? ctx->contents_cap * 2 : 64;
        redivis_content_t **grown = realloc(ctx->contents, cap * sizeof(*grown));
        if (!grown) return NULL;
        ctx->contents = grown; ctx->contents_cap = cap;
    }
    redivis_content_t *c = calloc(1, sizeof(redivis_content_t));
    if (!c) return NULL;
    c->ctx = ctx; c->key = strdup(key); c->file_id = file_id ? strdup(file_id) : NULL;
    c->size = size; c->n_blocks = (size + ctx->block_size - 1) / ctx->block_size;
    c->blocks = calloc(c->n_blocks ? c->n_blocks : 1, 1);
    c->fetching = calloc(c->n_blocks ? c->n_blocks : 1, 1);
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s.data", ctx->cache_dir, key);
    c->data_path = strdup(path);
    snprintf(path, sizeof(path), "%s/%s.blocks", ctx->cache_dir, key);
    c->blocks_path = strdup(path);
    c->fd = -1;
    for (int i = 0; i < MEMORY_BUFFER_BLOCKS; i++) c->memory[i].block = -1;
    pthread_mutex_init(&c->mutex, NULL);
    pthread_cond_init(&c->cond, NULL);
    ctx->contents[ctx->n_contents++] = c;
    ht_insert(&ctx->content_ht, c->key, c);
    return c;
}

/* Accounts for anything left in cache_dir by an earlier mount of it. Only files with both parts of a
   cached copy are considered, so nothing else in cache_dir is ever evicted. */
static void cache_scan(redivis_mount_ctx_t *ctx) {
    DIR *dir = opendir(ctx->cache_dir);
    if (!dir) return;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        size_t len = strlen(de->d_name);
        if (len <= 5 || len - 5 >= 1024 || strcmp(de->d_name + len - 5, ".data") != 0) continue;
        char key[1024], path[4096];
        memcpy(key, de->d_name, len - 5); key[len - 5] = '\0';
        struct stat data_st, blocks_st;
        snprintf(path, sizeof(path), "%s/%s.blocks", ctx->cache_dir, key);
        if (stat(path, &blocks_st) != 0 || !S_ISREG(blocks_st.st_mode)) continue;
        snprintf(path, sizeof(path), "%s/%s.data", ctx->cache_dir, key);
        if (stat(path, &data_st) != 0 || !S_ISREG(data_st.st_mode)) continue;

        redivis_content_t *c = ht_lookup(&ctx->content_ht, key);
        if (!c) c = content_add(ctx, key, NULL, 0);
        if (!c) continue;
        /* Cached files are sparse, so count the space actually allocated to them */
        c->size_on_disk = (size_t)data_st.st_blocks * 512;
        c->last_used = (int64_t)data_st.st_mtime * 1000000000;
        ctx->cache_size += (int64_t)c->size_on_disk;
    }
    closedir(dir);
}

/* ------------------------------------------------------------------ */
/*  FUSE callbacks                                                    */
/* ------------------------------------------------------------------ */

static int getattr_impl(const char *path, struct stat *stbuf) {
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    memset(stbuf, 0, sizeof(struct stat));
    const char *rel = path;
    while (*rel == '/') rel++;

    if (strlen(rel) == 0) {
        stbuf->st_mode = S_IFDIR | 0555; stbuf->st_nlink = 2;
        /* Directories have no added_at, so the mount time is the only thing we know. Without this
           the memset above leaves them at the epoch, which reads as a broken filesystem. */
        stbuf->st_mtime = stbuf->st_atime = stbuf->st_ctime = ctx->mounted_at;
        return 0;
    }
    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (entry) {
        stbuf->st_mode = S_IFREG | 0444; stbuf->st_nlink = 1;
        stbuf->st_size = (off_t)entry->size;
        stbuf->st_mtime = entry->added_at;
        /* We do not track access, so atime tracks mtime — the resting state of a file that has been
           written and not read since, which is what relatime would leave behind anyway. */
        stbuf->st_atime = entry->added_at;
        /* ctime is when the file appeared on THIS filesystem, not when it was added to Redivis.
           This deliberately diverges from the usual FUSE convention of ctime == mtime: Redivis
           notebooks select which files under /out to persist by sorting on ctime oldest-first, and
           reporting the source timestamp would let a mounted directory — whose files are typically
           far older than anything the session produced — claim the whole storage budget ahead of
           freshly written output. See selectPersistentFiles.sh in the redivis app, and keep this in
           step with the equivalent in redivis-python's mount_directory.py. */
        stbuf->st_ctime = ctx->mounted_at;
        return 0;
    }
    if (ht_lookup(&ctx->dir_ht, rel)) {
        stbuf->st_mode = S_IFDIR | 0555; stbuf->st_nlink = 2;
        stbuf->st_mtime = stbuf->st_atime = stbuf->st_ctime = ctx->mounted_at;
        return 0;
    }
    return -ENOENT;
}

static int fuse2_getattr_cb(const char *path, struct stat *stbuf) {
    return getattr_impl(path, stbuf);
}
static int fuse3_getattr_cb(const char *path, struct stat *stbuf,
                            fuse_file_info_t *fi) {
    (void)fi; return getattr_impl(path, stbuf);
}

static int fuse2_readdir_cb(const char *path, void *buf,
                            fuse_fill_dir_v2_t filler, off_t offset,
                            fuse_file_info_t *fi) {
    (void)offset; (void)fi;
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    const char *rel = path;
    while (*rel == '/') rel++;
    dir_entry_t *d = ht_lookup(&ctx->dir_ht, rel);
    if (!d) return -ENOENT;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    for (size_t i = 0; i < d->n_children; i++)
        filler(buf, d->child_names[i], NULL, 0);
    return 0;
}

static int fuse3_readdir_cb(const char *path, void *buf,
                            fuse_fill_dir_v3_t filler, off_t offset,
                            fuse_file_info_t *fi, int flags) {
    (void)offset; (void)fi; (void)flags;
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    const char *rel = path;
    while (*rel == '/') rel++;
    dir_entry_t *d = ht_lookup(&ctx->dir_ht, rel);
    if (!d) return -ENOENT;
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
    for (size_t i = 0; i < d->n_children; i++)
        filler(buf, d->child_names[i], NULL, 0, 0);
    return 0;
}

static void *fuse2_init_cb(struct fuse_conn_info *conn) {
    (void)conn; return dl_fuse_get_context()->private_data;
}
static void *fuse3_init_cb(struct fuse_conn_info *conn,
                           struct fuse_config *cfg) {
    (void)conn;
    cfg->kernel_cache = 1; cfg->auto_cache = 1;
    cfg->entry_timeout = 86400; cfg->attr_timeout = 86400;
    cfg->negative_timeout = 86400;
    return dl_fuse_get_context()->private_data;
}

static int fuse_open_cb(const char *path, fuse_file_info_t *fi) {
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (!entry) return -ENOENT;
    if ((fi_get_flags(fi) & O_ACCMODE) != O_RDONLY) return -EACCES;
    /* All handles on files with the same contents share one cached copy */
    int rc = content_open(entry->content);
    if (rc != 0) return rc;
    fi_set_fh(fi, (uint64_t)(uintptr_t)entry->content);
    fi_set_keep_cache(fi, 1);
    return 0;
}

static int fuse_read_cb(const char *path, char *buf, size_t size,
                        off_t offset, fuse_file_info_t *fi) {
    (void)path;
    redivis_content_t *c = (redivis_content_t *)(uintptr_t)fi_get_fh(fi);
    return content_read(c, buf, size, (size_t)offset);
}

static int fuse_release_cb(const char *path, fuse_file_info_t *fi) {
    (void)path;
    content_release((redivis_content_t *)(uintptr_t)fi_get_fh(fi));
    return 0;
}

static struct fuse_operations_v2 ops_v2;
static struct fuse_operations_v3 ops_v3;

static const void *build_fuse_ops(int version, size_t *ops_size) {
    if (version == 3) {
        memset(&ops_v3, 0, sizeof(ops_v3));
        ops_v3.init = fuse3_init_cb; ops_v3.getattr = fuse3_getattr_cb;
        ops_v3.readdir = fuse3_readdir_cb;
        ops_v3.open = fuse_open_cb; ops_v3.read = fuse_read_cb;
        ops_v3.release = fuse_release_cb;
        *ops_size = sizeof(ops_v3); return &ops_v3;
    } else {
        memset(&ops_v2, 0, sizeof(ops_v2));
        ops_v2.init = fuse2_init_cb; ops_v2.getattr = fuse2_getattr_cb;
        ops_v2.readdir = fuse2_readdir_cb;
        ops_v2.open = fuse_open_cb; ops_v2.read = fuse_read_cb;
        ops_v2.release = fuse_release_cb;
        *ops_size = sizeof(ops_v2); return &ops_v2;
    }
}

/* ------------------------------------------------------------------ */
/*  Background thread                                                 */
/* ------------------------------------------------------------------ */

/* Called by the FUSE thread once its loop exits. After an unmount the loop can exit at any moment,
   so ctx->fuse is handed over under the mutex, which is what stop_fuse_loop() checks it under. */
static struct fuse *take_fuse(redivis_mount_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->startup_mutex);
    struct fuse *f = ctx->fuse;
    ctx->fuse = NULL; ctx->running = 0;
    pthread_mutex_unlock(&ctx->startup_mutex);
    return f;
}

#ifdef __linux__
/* Whether libfuse can exec fusermount3, which it looks for in its install directory and on PATH */
static int fusermount3_available(void) {
    if (access("/usr/bin/fusermount3", X_OK) == 0 || access("/bin/fusermount3", X_OK) == 0) return 1;
    const char *path = getenv("PATH");
    if (!path) return 0;
    char candidate[4096];
    while (*path) {
        size_t len = strcspn(path, ":");
        if (len > 0 && len + sizeof("/fusermount3") <= sizeof(candidate)) {
            memcpy(candidate, path, len);
            memcpy(candidate + len, "/fusermount3", sizeof("/fusermount3"));
            if (access(candidate, X_OK) == 0) return 1;
        }
        path += len;
        if (*path == ':') path++;
    }
    return 0;
}
#endif

static void *fuse_thread_func(void *arg) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)arg;
    ctx->error_msg[0] = '\0';
    struct fuse_args fargs = FUSE_ARGS_INIT(0, NULL);
    dl_fuse_opt_add_arg(&fargs, "redivis");
#ifdef __linux__
    /* Have fusermount3 unmount the directory if this process exits without unmounting it (e.g. an R
       session restart), rather than leave a dead mount behind that fails every access with
       "Transport endpoint is not connected". With this option libfuse always mounts through
       fusermount3, even as root, so it's only asked for when that's installed; otherwise a root
       process that can mount(2) directly would stop being able to mount at all. */
    if (ctx->api_version == 3 && fusermount3_available()) {
        dl_fuse_opt_add_arg(&fargs, "-o");
        dl_fuse_opt_add_arg(&fargs, "auto_unmount");
    }
#endif

    size_t ops_size;
    const void *ops = build_fuse_ops(ctx->api_version, &ops_size);

    if (ctx->api_version == 2) {
        ctx->chan = dl_fuse2_mount(ctx->mount_point, &fargs);
        if (!ctx->chan) {
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "fuse_mount(\"%s\") returned NULL — check that the FUSE "
                     "library is working correctly", ctx->mount_point);
            dl_fuse_opt_free_args(&fargs);
            pthread_mutex_lock(&ctx->startup_mutex);
            ctx->running = 0; ctx->startup_done = 1;
            pthread_cond_signal(&ctx->startup_cond);
            pthread_mutex_unlock(&ctx->startup_mutex);
            return NULL;
        }
        ctx->fuse = dl_fuse2_new(ctx->chan, &fargs, ops, ops_size, ctx);
        if (!ctx->fuse) {
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "fuse_new() returned NULL — the FUSE operations struct "
                     "may be incompatible with the installed FUSE library");
            dl_fuse2_unmount(ctx->mount_point, ctx->chan);
            ctx->chan = NULL; dl_fuse_opt_free_args(&fargs);
            pthread_mutex_lock(&ctx->startup_mutex);
            ctx->running = 0; ctx->startup_done = 1;
            pthread_cond_signal(&ctx->startup_cond);
            pthread_mutex_unlock(&ctx->startup_mutex);
            return NULL;
        }
        pthread_mutex_lock(&ctx->startup_mutex);
        ctx->running = 1; ctx->startup_done = 1;
        pthread_cond_signal(&ctx->startup_cond);
        pthread_mutex_unlock(&ctx->startup_mutex);

        dl_fuse_loop(ctx->fuse);
        struct fuse *f = take_fuse(ctx);
        dl_fuse2_unmount(ctx->mount_point, ctx->chan);
        dl_fuse_destroy(f);
        if (ctx->remove_mount_point) rmdir(ctx->mount_point);
        ctx->chan = NULL;
        dl_fuse_opt_free_args(&fargs);
        return NULL;
    } else {
        /* Note: do NOT pass -f here. In FUSE 3, fuse_new() does not
         * accept -f (foreground); it's handled by fuse_daemonize().
         * We call fuse_loop() directly in this thread, so -f is
         * unnecessary and would cause fuse_new() to return NULL. */
        ctx->fuse = dl_fuse3_new(&fargs, ops, ops_size, ctx);
        if (!ctx->fuse) {
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "fuse_new() returned NULL (ops_size=%zu) — the FUSE "
                     "operations struct may be incompatible with the installed "
                     "FUSE library version", ops_size);
            dl_fuse_opt_free_args(&fargs);
            pthread_mutex_lock(&ctx->startup_mutex);
            ctx->running = 0; ctx->startup_done = 1;
            pthread_cond_signal(&ctx->startup_cond);
            pthread_mutex_unlock(&ctx->startup_mutex);
            return NULL;
        }
        int mount_rc = dl_fuse3_mount(ctx->fuse, ctx->mount_point);
        if (mount_rc != 0) {
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "fuse_mount(\"%s\") failed (rc=%d, errno=%d: %s)",
                     ctx->mount_point, mount_rc, errno, strerror(errno));
            dl_fuse_destroy(ctx->fuse); ctx->fuse = NULL;
            dl_fuse_opt_free_args(&fargs);
            pthread_mutex_lock(&ctx->startup_mutex);
            ctx->running = 0; ctx->startup_done = 1;
            pthread_cond_signal(&ctx->startup_cond);
            pthread_mutex_unlock(&ctx->startup_mutex);
            return NULL;
        }
        pthread_mutex_lock(&ctx->startup_mutex);
        ctx->running = 1; ctx->startup_done = 1;
        pthread_cond_signal(&ctx->startup_cond);
        pthread_mutex_unlock(&ctx->startup_mutex);

        dl_fuse_loop(ctx->fuse);
        struct fuse *f = take_fuse(ctx);
        dl_fuse3_unmount(f);
        dl_fuse_destroy(f);
        if (ctx->remove_mount_point) rmdir(ctx->mount_point);
        dl_fuse_opt_free_args(&fargs);
        return NULL;
    }
}

/* ------------------------------------------------------------------ */
/*  Kernel unmount                                                    */
/* ------------------------------------------------------------------ */

#ifndef __APPLE__
extern char **environ;

/* Runs argv (searched on PATH) and waits for it. Returns its exit status, 127 if it could not be
   started, or -1 on any other failure. Combined stdout/stderr is left in out. */
static int run_command(char *const argv[], char *out, size_t out_len) {
    out[0] = '\0';
    int fds[2];
    if (pipe(fds) != 0) return -1;
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, fds[1], STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&fa, fds[1], STDERR_FILENO);
    posix_spawn_file_actions_addclose(&fa, fds[0]);
    pid_t pid;
    int rc = posix_spawnp(&pid, argv[0], &fa, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(fds[1]);
    if (rc != 0) { close(fds[0]); return rc == ENOENT ? 127 : -1; }

    size_t used = 0; ssize_t r;
    char buf[256];
    while ((r = read(fds[0], buf, sizeof(buf))) != 0) {
        if (r < 0) { if (errno == EINTR) continue; break; }
        size_t take = (size_t)r;
        if (take > out_len - 1 - used) take = out_len - 1 - used;
        memcpy(out + used, buf, take); used += take;
    }
    out[used] = '\0';
    while (used > 0 && (out[used - 1] == '\n' || out[used - 1] == '\r')) out[--used] = '\0';
    close(fds[0]);

    int status;
    while (waitpid(pid, &status, 0) < 0) if (errno != EINTR) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}
#endif

/* Detaches the filesystem from the kernel. This must happen before the FUSE loop is asked to stop:
   fuse_exit() only sets a flag that the loop checks between requests, so on its own it leaves the
   loop blocked reading /dev/fuse (the hang), and once the loop does exit, libfuse closes /dev/fuse
   before its own unmount attempt, which fails quietly for unprivileged users and leaves a dead
   mount behind ("Transport endpoint is not connected"). Unmounting here instead makes the loop's
   read return straight away, and when it fails, the filesystem is still being served and the
   caller can report it. lazy detaches even while files are open, but the loop then keeps serving
   until they close, so it is only a last resort. Returns 0 on success, otherwise describes the failure in err. */
static int kernel_unmount(const char *mount_point, int lazy, char *err, size_t err_len) {
#ifdef __APPLE__
    /* FUSE-T attaches its NFS mount asynchronously after fuse_mount() returns, so right after
       mounting the kernel can still report EINVAL (not a mount point). Give it a moment. */
    for (int tries = 0; tries < 100; tries++) {
        if (unmount(mount_point, lazy ? MNT_FORCE : 0) == 0) return 0;
        if (errno != EINVAL) break;
        usleep(50000);
    }
    snprintf(err, err_len, "unmount(\"%s\") failed: %s", mount_point, strerror(errno));
    return -1;
#else
    /* Works as root or with CAP_SYS_ADMIN, which is how the mount was made in that case */
    if (umount2(mount_point, lazy ? MNT_DETACH : 0) == 0) return 0;
    int umount_errno = errno;

    /* Unprivileged mounts go through the setuid fusermount helper, so their unmounts must too */
    const char *helpers[] = {"fusermount3", "fusermount"};
    for (size_t i = 0; i < sizeof(helpers) / sizeof(helpers[0]); i++) {
        char *argv[6]; int a = 0;
        argv[a++] = (char *)helpers[i]; argv[a++] = "-u";
        if (lazy) argv[a++] = "-z";
        argv[a++] = "--"; argv[a++] = (char *)mount_point; argv[a] = NULL;
        char out[256];
        int rc = run_command(argv, out, sizeof(out));
        if (rc == 0) return 0;
        if (rc == 127) continue;
        snprintf(err, err_len, "%s -u failed (exit %d): %s", helpers[i], rc,
                 out[0] ? out : "no output");
        return -1;
    }
    snprintf(err, err_len, "umount2(\"%s\") failed (%s), and neither fusermount3 nor fusermount "
             "was found on PATH", mount_point, strerror(umount_errno));
    return -1;
#endif
}

/* ------------------------------------------------------------------ */
/*  R interface                                                       */
/* ------------------------------------------------------------------ */

/* Stops the loop of a filesystem kernel_unmount() has already detached, and waits for it */
static void stop_fuse_loop(redivis_mount_ctx_t *ctx) {
    /* With the mount gone, FUSE-T's connection is closed, and fuse_exit() writing to it raises
       SIGPIPE, for which R's handler throws an R error. Ignoring it discards the signal. */
    struct sigaction ignore, prev;
    memset(&ignore, 0, sizeof(ignore)); ignore.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore, &prev);
    pthread_mutex_lock(&ctx->startup_mutex);
    if (ctx->fuse) dl_fuse_exit(ctx->fuse);
    pthread_mutex_unlock(&ctx->startup_mutex);
    pthread_join(ctx->thread, NULL);
    ctx->thread_live = 0;
    sigaction(SIGPIPE, &prev, NULL);
}

/* Unmounts and stops the FUSE thread. Returns -1, with err filled and nothing changed, if the
   kernel refused the unmount; the filesystem is then still being served. */
static int unmount_ctx(redivis_mount_ctx_t *ctx, char *err, size_t err_len) {
    if (!ctx->thread_live) return 0;
    pthread_mutex_lock(&ctx->startup_mutex);
    int running = ctx->running;
    pthread_mutex_unlock(&ctx->startup_mutex);
    /* Not running means the loop already exited, e.g. after an external fusermount -u */
    if (running && kernel_unmount(ctx->mount_point, 0, err, err_len) != 0) return -1;
    stop_fuse_loop(ctx);
    /* The FUSE thread removes it too, but only when its own unmount has succeeded */
    if (ctx->remove_mount_point) rmdir(ctx->mount_point);
    return 0;
}

/* Frees ctx, which must no longer be mounted */
static void ctx_free(redivis_mount_ctx_t *ctx) {
    for (size_t i = 0; i < ctx->n_contents; i++) {
        redivis_content_t *c = ctx->contents[i];
        /* Stop any streams still running, e.g. for files left open when an unmounted cache is collected */
        redivis_stream_t *reap[MAX_REAP]; int n_reap = 0;
        pthread_mutex_lock(&c->mutex);
        while (c->n_streams > 0) stream_remove(c, 0, reap, &n_reap);
        pthread_mutex_unlock(&c->mutex);
        streams_join(reap, n_reap);
        if (c->fd >= 0) close(c->fd);
        memory_free(c);
        pthread_mutex_destroy(&c->mutex);
        pthread_cond_destroy(&c->cond);
        free(c->key); free(c->file_id); free(c->data_path); free(c->blocks_path);
        free(c->blocks); free(c->fetching); free(c);
    }
    if (ctx->remove_cache_dir) remove_temporary_cache_dir(ctx->cache_dir);
    if (ctx->cache_lock_fd >= 0) close(ctx->cache_lock_fd);
    free(ctx->contents);
    for (size_t i = 0; i < ctx->n_entries; i++) free(ctx->entries[i].rel_path);
    free(ctx->entries);
    for (size_t i = 0; i < ctx->n_dirs; i++) {
        free(ctx->dirs[i].path);
        for (size_t j = 0; j < ctx->dirs[i].n_children; j++)
            free(ctx->dirs[i].child_names[j]);
        free(ctx->dirs[i].child_names); free(ctx->dirs[i].child_is_dir);
    }
    free(ctx->dirs);
    ht_free(&ctx->dir_ht); ht_free(&ctx->file_ht); ht_free(&ctx->content_ht);
    pthread_mutex_destroy(&ctx->startup_mutex);
    pthread_cond_destroy(&ctx->startup_cond);
    pthread_mutex_destroy(&ctx->cache_mutex);
    pthread_mutex_destroy(&ctx->auth_mutex);
    free(ctx->mount_point); free(ctx->cache_dir);
    free(ctx->api_base_url); free(ctx->auth_token);
    free(ctx);
}

static void fuse_mount_finalizer(SEXP ptr) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ptr);
    if (!ctx) return;
    char err[512];
    if (unmount_ctx(ctx, err, sizeof(err)) != 0) {
        /* Nobody can be told about the failure here, so detach lazily rather than leave the mount
           in place. The loop keeps serving from ctx until open files close, so neither wait for it
           nor free ctx. */
        kernel_unmount(ctx->mount_point, 1, err, sizeof(err));
        pthread_detach(ctx->thread);
        R_ClearExternalPtr(ptr);
        return;
    }
    ctx_free(ctx);
    R_ClearExternalPtr(ptr);
}

/* Builds the filesystem from the directory's manifest (see Directory$mount), without mounting it.
   Files with the same contents (by key) share one cached copy. */
static redivis_mount_ctx_t *build_ctx(const char *mount_point, SEXP s_cache_dir,
                                      SEXP s_temporary_cache, SEXP s_max_cache_size,
                                      SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids,
                                      SEXP s_keys, SEXP s_added_ats,
                                      SEXP s_dir_paths, SEXP s_dir_child_names,
                                      SEXP s_dir_child_is_dir,
                                      SEXP s_api_base_url, SEXP s_auth_token,
                                      SEXP s_verify_ssl, size_t block_size) {
    R_xlen_t n = XLENGTH(s_rel_paths);
    R_xlen_t n_dirs = XLENGTH(s_dir_paths);

    redivis_mount_ctx_t *ctx = calloc(1, sizeof(redivis_mount_ctx_t));
    if (!ctx) Rf_error("fuse_mount: allocation failed");
    pthread_mutex_init(&ctx->startup_mutex, NULL);
    pthread_cond_init(&ctx->startup_cond, NULL);
    pthread_mutex_init(&ctx->cache_mutex, NULL);
    pthread_mutex_init(&ctx->auth_mutex, NULL);

    ctx->cache_lock_fd = -1;
    ctx->mount_point = strdup(mount_point);
    ctx->cache_dir = strdup(CHAR(STRING_ELT(s_cache_dir, 0)));
    ctx->api_base_url = strdup(CHAR(STRING_ELT(s_api_base_url, 0)));
    ctx->auth_token = strdup(CHAR(STRING_ELT(s_auth_token, 0)));
    ctx->verify_ssl = Rf_asLogical(s_verify_ssl) != FALSE;
    ctx->block_size = block_size;
    ctx->mounted_at = time(NULL);
    ctx->entries = calloc(n ? (size_t)n : 1, sizeof(redivis_file_entry_t));
    ctx->dirs = calloc(n_dirs ? (size_t)n_dirs : 1, sizeof(dir_entry_t));
    ht_init(&ctx->file_ht, (size_t)n);
    ht_init(&ctx->dir_ht, (size_t)n_dirs);
    ht_init(&ctx->content_ht, (size_t)n);
    if (!ctx->mount_point || !ctx->cache_dir || !ctx->api_base_url || !ctx->auth_token ||
        !ctx->entries || !ctx->dirs) {
        ctx_free(ctx);
        Rf_error("fuse_mount: allocation failed");
    }
    if (Rf_asLogical(s_temporary_cache) == TRUE) {
        /* A cache of its own, made in s_cache_dir (see TEMPORARY_CACHE_PREFIX) */
        char *dir = make_temporary_cache_dir(ctx->cache_dir, &ctx->cache_lock_fd);
        if (!dir) {
            char msg[4096 + 256];
            snprintf(msg, sizeof(msg), "fuse_mount: could not create a cache directory in '%s': %s",
                     ctx->cache_dir, strerror(errno));
            ctx_free(ctx);
            Rf_error("%s", msg);
        }
        free(ctx->cache_dir);
        ctx->cache_dir = dir;
        ctx->remove_cache_dir = 1;
    } else {
        mkdirs(ctx->cache_dir);
    }

    for (R_xlen_t i = 0; i < n; i++) {
        redivis_file_entry_t *entry = &ctx->entries[ctx->n_entries++];
        entry->rel_path = strdup(CHAR(STRING_ELT(s_rel_paths, i)));
        entry->size = (size_t)REAL(s_sizes)[i];
        entry->added_at = (time_t)REAL(s_added_ats)[i];
        const char *key = CHAR(STRING_ELT(s_keys, i));
        entry->content = ht_lookup(&ctx->content_ht, key);
        if (!entry->content)
            entry->content = content_add(ctx, key, CHAR(STRING_ELT(s_file_ids, i)), entry->size);
        if (!entry->rel_path || !entry->content) {
            ctx_free(ctx);
            Rf_error("fuse_mount: allocation failed");
        }
        ht_insert(&ctx->file_ht, entry->rel_path, entry);
    }

    for (R_xlen_t i = 0; i < n_dirs; i++) {
        dir_entry_t *d = &ctx->dirs[ctx->n_dirs++];
        d->path = strdup(CHAR(STRING_ELT(s_dir_paths, i)));
        SEXP cnv = VECTOR_ELT(s_dir_child_names, i);
        SEXP civ = VECTOR_ELT(s_dir_child_is_dir, i);
        R_xlen_t nc = XLENGTH(cnv);
        d->child_names = calloc((size_t)nc, sizeof(char *));
        d->child_is_dir = calloc((size_t)nc, sizeof(int));
        for (R_xlen_t j = 0; j < nc; j++) {
            d->child_names[j] = strdup(CHAR(STRING_ELT(cnv, j)));
            d->child_is_dir[j] = LOGICAL(civ)[j];
        }
        d->n_children = (size_t)nc;
        ht_insert(&ctx->dir_ht, d->path, d);
    }

    cache_scan(ctx);
    double max_cache_size = Rf_asReal(s_max_cache_size);
    if (ISNAN(max_cache_size) || max_cache_size < 0) {
        /* Default to half the space available to the cache, leaving room for everything else */
        struct statvfs vfs;
        max_cache_size = statvfs(ctx->cache_dir, &vfs) == 0
            ? ((double)vfs.f_bavail * (double)vfs.f_frsize + (double)ctx->cache_size) / 2
            : R_PosInf;
    }
    ctx->max_cache_size = max_cache_size;
    /* Trims whatever an earlier mount left to the maximum size */
    cache_add(ctx, 0);
    return ctx;
}

SEXP C_fuse_mount(SEXP s_mount_point, SEXP s_cache_dir,
                  SEXP s_temporary_cache, SEXP s_max_cache_size,
                  SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids,
                  SEXP s_keys, SEXP s_added_ats,
                  SEXP s_dir_paths, SEXP s_dir_child_names,
                  SEXP s_dir_child_is_dir,
                  SEXP s_api_base_url, SEXP s_auth_token, SEXP s_verify_ssl,
                  SEXP s_remove_mount_point) {
    int api_ver = load_fuse_library();
    if (api_ver == 0) {
        Rf_error("No FUSE library found. Install one of:\n"
                 "  - Linux:  sudo apt install fuse3  (or libfuse3-3)\n"
                 "  - macOS:  FUSE-T (https://www.fuse-t.org/) or macFUSE\n"
                 "No development headers or packages are needed.");
    }
    if (!load_curl_library()) {
        Rf_error("libcurl could not be loaded. Install the libcurl runtime:\n"
                 "  - Debian/Ubuntu: sudo apt install libcurl4\n"
                 "  - Fedora/RHEL:   sudo dnf install libcurl\n"
                 "No development headers or packages are needed.");
    }

    const char *mount_point = CHAR(STRING_ELT(s_mount_point, 0));
    /* mount() accepts an existing empty directory, but mounting over another mount would hide it */
    if (is_mount_point(mount_point)) Rf_error("Mount path '%s' is already a mount point.", mount_point);
    redivis_mount_ctx_t *ctx = build_ctx(
        mount_point, s_cache_dir, s_temporary_cache, s_max_cache_size,
        s_rel_paths, s_sizes, s_file_ids, s_keys, s_added_ats,
        s_dir_paths, s_dir_child_names, s_dir_child_is_dir,
        s_api_base_url, s_auth_token, s_verify_ssl, DEFAULT_BLOCK_SIZE);
    ctx->api_version = api_ver;
    ctx->remove_mount_point = Rf_asLogical(s_remove_mount_point) == TRUE;

    mkdirs(mount_point);
    ctx->startup_done = 0;

    int rc = pthread_create(&ctx->thread, NULL, fuse_thread_func, ctx);
    if (rc != 0) {
        ctx_free(ctx);
        Rf_error("fuse_mount: pthread_create failed (errno=%d)", rc);
    }

    /* Wait for the FUSE thread to report success or failure */
    pthread_mutex_lock(&ctx->startup_mutex);
    while (!ctx->startup_done) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 10;  /* 10 second timeout */
        int wait_rc = pthread_cond_timedwait(&ctx->startup_cond,
                                              &ctx->startup_mutex, &ts);
        if (wait_rc != 0) break;  /* timeout or error */
    }
    pthread_mutex_unlock(&ctx->startup_mutex);

    if (!ctx->running) {
        char detail[512];
        strncpy(detail, ctx->error_msg, sizeof(detail));
        detail[sizeof(detail) - 1] = '\0';

        pthread_join(ctx->thread, NULL);
        ctx_free(ctx);

#ifdef __APPLE__
        Rf_error("fuse_mount: FUSE failed to start.\n"
                 "  - Ensure FUSE-T or macFUSE is installed and working\n"
                 "  - Is the mount point '%s' accessible?\n"
                 "  - Detail: %s",
                 mount_point, detail);
#else
        int dev_fuse_exists = (access("/dev/fuse", F_OK) == 0);
        int dev_fuse_readable = (access("/dev/fuse", R_OK | W_OK) == 0);

        if (!dev_fuse_exists) {
            Rf_error("fuse_mount: FUSE failed to start — /dev/fuse does not exist.\n"
                     "  - Load the kernel module: sudo modprobe fuse\n"
                     "  - In Docker: run with --device /dev/fuse --cap-add SYS_ADMIN\n"
                     "  - Detail: %s", detail);
        } else if (!dev_fuse_readable) {
            Rf_error("fuse_mount: FUSE failed to start — /dev/fuse permission denied.\n"
                     "  - Check: ls -la /dev/fuse\n"
                     "  - Add your user to the 'fuse' group, or run as root\n"
                     "  - In Docker: run with --device /dev/fuse --cap-add SYS_ADMIN\n"
                     "  - Detail: %s", detail);
        } else {
            Rf_error("fuse_mount: FUSE failed to start.\n"
                     "  - Is the mount point '%s' accessible?\n"
                     "  - Check 'dmesg | tail' for kernel FUSE errors\n"
                     "  - Detail: %s",
                     mount_point, detail);
        }
#endif
    }

    ctx->thread_live = 1;
    SEXP ptr = PROTECT(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, fuse_mount_finalizer, TRUE);
    UNPROTECT(1);
    return ptr;
}

SEXP C_fuse_set_auth_token(SEXP ext_ptr, SEXP s_auth_token) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ext_ptr);
    if (!ctx) return R_NilValue;
    char *token = strdup(CHAR(STRING_ELT(s_auth_token, 0)));
    if (!token) Rf_error("fuse_set_auth_token: allocation failed");
    pthread_mutex_lock(&ctx->auth_mutex);
    free(ctx->auth_token);
    ctx->auth_token = token;
    pthread_mutex_unlock(&ctx->auth_mutex);
    return R_NilValue;
}

/* The filesystem's cache, without mounting it, for tests. Its files are read with C_fuse_cache_file,
   exactly as the FUSE callbacks read them, so none of this needs a FUSE installation. */
SEXP C_fuse_cache_open(SEXP s_cache_dir, SEXP s_temporary_cache, SEXP s_max_cache_size,
                       SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids, SEXP s_keys,
                       SEXP s_api_base_url, SEXP s_auth_token, SEXP s_verify_ssl,
                       SEXP s_block_size) {
    if (!load_curl_library()) Rf_error("libcurl could not be loaded");
    SEXP added_ats = PROTECT(Rf_allocVector(REALSXP, XLENGTH(s_rel_paths)));
    for (R_xlen_t i = 0; i < XLENGTH(added_ats); i++) REAL(added_ats)[i] = 0;
    SEXP no_dirs = PROTECT(Rf_allocVector(STRSXP, 0));
    SEXP no_children = PROTECT(Rf_allocVector(VECSXP, 0));
    redivis_mount_ctx_t *ctx = build_ctx(
        "", s_cache_dir, s_temporary_cache, s_max_cache_size,
        s_rel_paths, s_sizes, s_file_ids, s_keys, added_ats,
        no_dirs, no_children, no_children,
        s_api_base_url, s_auth_token, s_verify_ssl, (size_t)Rf_asReal(s_block_size));
    SEXP ptr = PROTECT(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, fuse_mount_finalizer, TRUE);
    UNPROTECT(4);
    return ptr;
}

/* Opens, reads from, or releases (by op) a file in a cache from C_fuse_cache_open */
SEXP C_fuse_cache_file(SEXP ext_ptr, SEXP s_path, SEXP s_op, SEXP s_offset, SEXP s_length) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ext_ptr);
    if (!ctx) Rf_error("fuse_cache_file: the cache has been closed");
    redivis_file_entry_t *entry = find_entry(ctx, CHAR(STRING_ELT(s_path, 0)));
    if (!entry) Rf_error("fuse_cache_file: no such file");
    const char *op = CHAR(STRING_ELT(s_op, 0));

    if (strcmp(op, "open") == 0) {
        int rc = content_open(entry->content);
        if (rc != 0) Rf_error("fuse_cache_file: open failed: %s", strerror(-rc));
        return R_NilValue;
    }
    if (strcmp(op, "release") == 0) {
        content_release(entry->content);
        return R_NilValue;
    }
    size_t length = (size_t)Rf_asReal(s_length);
    char *buf = R_alloc(length ? length : 1, 1);
    int n = content_read(entry->content, buf, length, (size_t)Rf_asReal(s_offset));
    if (n < 0) Rf_error("fuse_cache_file: read failed: %s", strerror(-n));
    SEXP out = PROTECT(Rf_allocVector(RAWSXP, n));
    memcpy(RAW(out), buf, (size_t)n);
    UNPROTECT(1);
    return out;
}

SEXP C_fuse_unmount(SEXP ext_ptr) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ext_ptr);
    if (!ctx) return R_NilValue;
    char err[512];
    if (unmount_ctx(ctx, err, sizeof(err)) != 0) {
        /* The mount is untouched and still being served, so the caller can retry */
        Rf_error("fuse_unmount: could not unmount '%s'.\n"
                 "  - Close any files open under it (including a shell or R session whose "
                 "working directory is inside it), then try again\n"
                 "  - Detail: %s", ctx->mount_point, err);
    }
    fuse_mount_finalizer(ext_ptr);
    return R_NilValue;
}
#endif /* _WIN32 */
