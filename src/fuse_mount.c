/*
 * fuse_mount.c — FUSE read-only filesystem for Redivis Directory objects
 *
 * FUSE libraries are loaded at RUNTIME via dlopen/dlsym, so no FUSE
 * headers or dev packages are needed at compile time. Only the runtime
 * shared library (libfuse3.so.3, libfuse-t.dylib, or libfuse.2.dylib)
 * must be present on the system when mount() is called.
 */

#include <R.h>
#include <Rinternals.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>
#include <curl/curl.h>

/* ================================================================== */
/*  FUSE ABI declarations (replaces #include <fuse.h>)                */
/* ================================================================== */

struct fuse;
struct fuse_chan;
struct fuse_conn_info;

struct fuse_config {
    int set_gid;        int gid;
    int set_uid;        int uid;
    int set_mode;       int umask;
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
};

struct fuse_file_info {
    int           flags;
    unsigned long fh_old;       /* FUSE 2: old-style file handle (deprecated) */
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

typedef int (*fuse_fill_dir_t)(void *buf, const char *name,
                               const struct stat *stbuf, off_t off, ...);

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
    int (*open)(const char *, struct fuse_file_info *);
    int (*read)(const char *, char *, size_t, off_t, struct fuse_file_info *);
    int (*write)(const char *, const char *, size_t, off_t, struct fuse_file_info *);
    int (*statfs)(const char *, void *);
    int (*flush)(const char *, struct fuse_file_info *);
    int (*release)(const char *, struct fuse_file_info *);
    int (*fsync)(const char *, int, struct fuse_file_info *);
    int (*setxattr)(const char *, const char *, const char *, size_t, int);
    int (*getxattr)(const char *, const char *, char *, size_t);
    int (*listxattr)(const char *, char *, size_t);
    int (*removexattr)(const char *, const char *);
    int (*opendir)(const char *, struct fuse_file_info *);
    int (*readdir)(const char *, void *, fuse_fill_dir_t, off_t,
                   struct fuse_file_info *);
    int (*releasedir)(const char *, struct fuse_file_info *);
    int (*fsyncdir)(const char *, int, struct fuse_file_info *);
    void *(*init)(struct fuse_conn_info *);
    void (*destroy)(void *);
    int (*access)(const char *, int);
    int (*create)(const char *, mode_t, struct fuse_file_info *);
    int (*ftruncate)(const char *, off_t, struct fuse_file_info *);
    int (*fgetattr)(const char *, struct stat *, struct fuse_file_info *);
    int (*lock)(const char *, struct fuse_file_info *, int, void *);
    int (*utimens)(const char *, const void *);
    int (*bmap)(const char *, size_t, uint64_t *);
    unsigned int flag_nullpath_ok : 1;
    unsigned int flag_nopath : 1;
    unsigned int flag_utime_omit_ok : 1;
    unsigned int flag_reserved : 29;
    int (*ioctl)(const char *, int, void *, struct fuse_file_info *,
                 unsigned int, void *);
    int (*poll)(const char *, struct fuse_file_info *, void *, unsigned *);
    int (*write_buf)(const char *, void *, off_t, struct fuse_file_info *);
    int (*read_buf)(const char *, void **, size_t, off_t,
                    struct fuse_file_info *);
    int (*flock)(const char *, struct fuse_file_info *, int);
    int (*fallocate)(const char *, int, off_t, off_t,
                     struct fuse_file_info *);
};

/* FUSE 3 operations struct */
struct fuse_operations_v3 {
    int (*getattr)(const char *, struct stat *, struct fuse_file_info *);
    int (*readlink)(const char *, char *, size_t);
    int (*mknod)(const char *, mode_t, dev_t);
    int (*mkdir)(const char *, mode_t);
    int (*unlink)(const char *);
    int (*rmdir)(const char *);
    int (*symlink)(const char *, const char *);
    int (*rename)(const char *, const char *, unsigned int);
    int (*link)(const char *, const char *);
    int (*chmod)(const char *, mode_t, struct fuse_file_info *);
    int (*chown)(const char *, uid_t, gid_t, struct fuse_file_info *);
    int (*truncate)(const char *, off_t, struct fuse_file_info *);
    int (*open)(const char *, struct fuse_file_info *);
    int (*read)(const char *, char *, size_t, off_t, struct fuse_file_info *);
    int (*write)(const char *, const char *, size_t, off_t,
                 struct fuse_file_info *);
    int (*statfs)(const char *, void *);
    int (*flush)(const char *, struct fuse_file_info *);
    int (*release)(const char *, struct fuse_file_info *);
    int (*fsync)(const char *, int, struct fuse_file_info *);
    int (*setxattr)(const char *, const char *, const char *, size_t, int);
    int (*getxattr)(const char *, const char *, char *, size_t);
    int (*listxattr)(const char *, char *, size_t);
    int (*removexattr)(const char *, const char *);
    int (*opendir)(const char *, struct fuse_file_info *);
    int (*readdir)(const char *, void *, fuse_fill_dir_t, off_t,
                   struct fuse_file_info *, int);
    int (*releasedir)(const char *, struct fuse_file_info *);
    int (*fsyncdir)(const char *, int, struct fuse_file_info *);
    void *(*init)(struct fuse_conn_info *, struct fuse_config *);
    void (*destroy)(void *);
    int (*access)(const char *, int);
    int (*create)(const char *, mode_t, struct fuse_file_info *);
    int (*lock)(const char *, struct fuse_file_info *, int, void *);
    int (*utimens)(const char *, const void *, struct fuse_file_info *);
    int (*bmap)(const char *, size_t, uint64_t *);
    int (*ioctl)(const char *, unsigned int, void *, struct fuse_file_info *,
                 unsigned int, void *);
    int (*poll)(const char *, struct fuse_file_info *, void *, unsigned *);
    int (*write_buf)(const char *, void *, off_t, struct fuse_file_info *);
    int (*read_buf)(const char *, void **, size_t, off_t,
                    struct fuse_file_info *);
    int (*flock)(const char *, struct fuse_file_info *, int);
    int (*fallocate)(const char *, int, off_t, off_t,
                     struct fuse_file_info *);
    int (*copy_file_range)(const char *, struct fuse_file_info *, off_t,
                           const char *, struct fuse_file_info *, off_t,
                           size_t, int);
    off_t (*lseek)(const char *, off_t, int, struct fuse_file_info *);
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

static int fuse_api_version = 0;
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

static int load_fuse_library(void)
{
    if (fuse_lib_handle) return fuse_api_version;

    const char *fuse3_names[] = {
        "libfuse3.so.3", "libfuse3.so", "libfuse3.dylib",
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

/* ------------------------------------------------------------------ */
/*  Constants & data structures                                       */
/* ------------------------------------------------------------------ */

#define BLOCK_SIZE (4 * 1024 * 1024)

typedef struct redivis_file_entry {
    char *rel_path; size_t size; char *file_id;
    char *cache_path; char *bitmap_path;
    int fully_cached; unsigned char *bitmap; size_t n_blocks;
    time_t added_at; pthread_mutex_t mutex;
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

typedef struct redivis_mount_ctx {
    char *mount_point; char *cache_dir; char *api_base_url; char *auth_token;
    redivis_file_entry_t *entries; size_t n_entries;
    hash_table_t file_ht; hash_table_t dir_ht;
    dir_entry_t *dirs; size_t n_dirs;
    struct fuse *fuse; struct fuse_chan *chan;
    int api_version; pthread_t thread; int running;
} redivis_mount_ctx_t;

typedef struct redivis_fh {
    redivis_file_entry_t *entry; int fd;
} redivis_fh_t;

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
/*  Bitmap                                                            */
/* ------------------------------------------------------------------ */

static inline int bitmap_get(const unsigned char *bm, size_t i) {
    return (bm[i/8] >> (i%8)) & 1;
}
static inline void bitmap_set(unsigned char *bm, size_t i) {
    bm[i/8] |= (unsigned char)(1u << (i%8));
}
static size_t bitmap_bytes(size_t n) { return (n+7)/8; }

static int all_blocks_cached(const unsigned char *bm, size_t n) {
    for (size_t i = 0; i < n; i++) if (!bitmap_get(bm, i)) return 0;
    return 1;
}

static void bitmap_save(redivis_file_entry_t *e) {
    FILE *fp = fopen(e->bitmap_path, "wb");
    if (fp) { fwrite(e->bitmap, 1, bitmap_bytes(e->n_blocks), fp); fclose(fp); }
}

static int bitmap_load(redivis_file_entry_t *e) {
    size_t nb = bitmap_bytes(e->n_blocks);
    FILE *fp = fopen(e->bitmap_path, "rb");
    if (!fp) return 0;
    size_t rd = fread(e->bitmap, 1, nb, fp); fclose(fp);
    if (rd != nb) { memset(e->bitmap, 0, nb); return 0; }
    return 1;
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

/* ------------------------------------------------------------------ */
/*  Block download via libcurl                                        */
/* ------------------------------------------------------------------ */

typedef struct { int fd; off_t offset; size_t written; } range_write_ctx_t;

static size_t curl_range_write_cb(void *ptr, size_t size, size_t nmemb,
                                  void *userdata) {
    range_write_ctx_t *w = (range_write_ctx_t *)userdata;
    size_t total = size * nmemb;
    ssize_t n = pwrite(w->fd, ptr, total, w->offset + (off_t)w->written);
    if (n < 0) return 0;
    w->written += (size_t)n;
    return (size_t)n;
}

static int ensure_blocks_cached(redivis_mount_ctx_t *ctx,
                                redivis_file_entry_t *entry,
                                int fd, size_t blk_start, size_t blk_end) {
    if (entry->fully_cached) return 0;
    pthread_mutex_lock(&entry->mutex);

    size_t i = blk_start;
    while (i < blk_end) {
        if (bitmap_get(entry->bitmap, i)) { i++; continue; }
        size_t run_start = i;
        while (i < blk_end && !bitmap_get(entry->bitmap, i)) i++;
        size_t run_end = i;

        off_t byte_start = (off_t)run_start * BLOCK_SIZE;
        off_t byte_end = (off_t)run_end * BLOCK_SIZE - 1;
        if ((size_t)(byte_end+1) > entry->size)
            byte_end = (off_t)(entry->size - 1);

        char url[4096];
        snprintf(url, sizeof(url), "%s/rawFiles/%s",
                 ctx->api_base_url, entry->file_id);
        char range_val[128];
        snprintf(range_val, sizeof(range_val), "%lld-%lld",
                 (long long)byte_start, (long long)byte_end);

        CURL *curl = curl_easy_init();
        if (!curl) { pthread_mutex_unlock(&entry->mutex); return -EIO; }

        char auth_hdr[2048];
        snprintf(auth_hdr, sizeof(auth_hdr), "Authorization: Bearer %s",
                 ctx->auth_token);
        struct curl_slist *hdrs = NULL;
        hdrs = curl_slist_append(hdrs, auth_hdr);

        range_write_ctx_t wctx = { .fd = fd, .offset = byte_start, .written = 0 };
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
        curl_easy_setopt(curl, CURLOPT_RANGE, range_val);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_range_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &wctx);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(hdrs);
        curl_easy_cleanup(curl);
        if (res != CURLE_OK) { pthread_mutex_unlock(&entry->mutex); return -EIO; }

        for (size_t b = run_start; b < run_end; b++)
            bitmap_set(entry->bitmap, b);
        bitmap_save(entry);
    }

    if (all_blocks_cached(entry->bitmap, entry->n_blocks))
        entry->fully_cached = 1;
    pthread_mutex_unlock(&entry->mutex);
    return 0;
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
        stbuf->st_mode = S_IFDIR | 0555; stbuf->st_nlink = 2; return 0;
    }
    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (entry) {
        stbuf->st_mode = S_IFREG | 0444; stbuf->st_nlink = 1;
        stbuf->st_size = (off_t)entry->size;
        stbuf->st_mtime = entry->added_at;
        stbuf->st_atime = entry->added_at;
        stbuf->st_ctime = entry->added_at;
        return 0;
    }
    if (ht_lookup(&ctx->dir_ht, rel)) {
        stbuf->st_mode = S_IFDIR | 0555; stbuf->st_nlink = 2; return 0;
    }
    return -ENOENT;
}

static int fuse2_getattr_cb(const char *path, struct stat *stbuf) {
    return getattr_impl(path, stbuf);
}
static int fuse3_getattr_cb(const char *path, struct stat *stbuf,
                            struct fuse_file_info *fi) {
    (void)fi; return getattr_impl(path, stbuf);
}

static int readdir_impl(const char *path, void *buf, fuse_fill_dir_t filler) {
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

static int fuse2_readdir_cb(const char *path, void *buf,
                            fuse_fill_dir_t filler, off_t offset,
                            struct fuse_file_info *fi) {
    (void)offset; (void)fi;
    return readdir_impl(path, buf, filler);
}
static int fuse3_readdir_cb(const char *path, void *buf,
                            fuse_fill_dir_t filler, off_t offset,
                            struct fuse_file_info *fi, int flags) {
    (void)offset; (void)fi; (void)flags;
    return readdir_impl(path, buf, filler);
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

static int fuse_open_cb(const char *path, struct fuse_file_info *fi) {
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (!entry) return -ENOENT;
    if ((fi->flags & O_ACCMODE) != O_RDONLY) return -EACCES;
    { char *dup = strdup(entry->cache_path);
      char *sl = strrchr(dup, '/');
      if (sl) { *sl = '\0'; mkdirs(dup); } free(dup); }
    int fd = open(entry->cache_path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return -errno;
    if (entry->size > 0) {
        struct stat st;
        if (fstat(fd, &st) == 0 && (size_t)st.st_size < entry->size)
            ftruncate(fd, (off_t)entry->size);
    }
    redivis_fh_t *fh = malloc(sizeof(redivis_fh_t));
    if (!fh) { close(fd); return -ENOMEM; }
    fh->entry = entry; fh->fd = fd;
    fi->fh = (uint64_t)(uintptr_t)fh; fi->keep_cache = 1;
    return 0;
}

static int fuse_read_cb(const char *path, char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi) {
    (void)path;
    redivis_fh_t *fh = (redivis_fh_t *)(uintptr_t)fi->fh;
    redivis_file_entry_t *entry = fh->entry;
    redivis_mount_ctx_t *ctx = dl_fuse_get_context()->private_data;
    if ((size_t)offset >= entry->size) return 0;
    if ((size_t)offset + size > entry->size)
        size = entry->size - (size_t)offset;
    size_t blk_start = (size_t)offset / BLOCK_SIZE;
    size_t blk_end = ((size_t)offset + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (blk_end > entry->n_blocks) blk_end = entry->n_blocks;
    int rc = ensure_blocks_cached(ctx, entry, fh->fd, blk_start, blk_end);
    if (rc != 0) return rc;
    ssize_t n = pread(fh->fd, buf, size, offset);
    if (n < 0) return -errno;
    return (int)n;
}

static int fuse_release_cb(const char *path, struct fuse_file_info *fi) {
    (void)path;
    redivis_fh_t *fh = (redivis_fh_t *)(uintptr_t)fi->fh;
    close(fh->fd); free(fh); return 0;
}

static struct fuse_operations_v2 ops_v2;
static struct fuse_operations_v3 ops_v3;

static const void *build_fuse_ops(int version, size_t *ops_size) {
    if (version == 3) {
        memset(&ops_v3, 0, sizeof(ops_v3));
        ops_v3.init = fuse3_init_cb; ops_v3.getattr = fuse3_getattr_cb;
        ops_v3.readdir = (void *)fuse3_readdir_cb;
        ops_v3.open = fuse_open_cb; ops_v3.read = fuse_read_cb;
        ops_v3.release = fuse_release_cb;
        *ops_size = sizeof(ops_v3); return &ops_v3;
    } else {
        memset(&ops_v2, 0, sizeof(ops_v2));
        ops_v2.init = fuse2_init_cb; ops_v2.getattr = fuse2_getattr_cb;
        ops_v2.readdir = (void *)fuse2_readdir_cb;
        ops_v2.open = fuse_open_cb; ops_v2.read = fuse_read_cb;
        ops_v2.release = fuse_release_cb;
        *ops_size = sizeof(ops_v2); return &ops_v2;
    }
}

/* ------------------------------------------------------------------ */
/*  Background thread                                                 */
/* ------------------------------------------------------------------ */

static void *fuse_thread_func(void *arg) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)arg;
    struct fuse_args fargs = FUSE_ARGS_INIT(0, NULL);
    dl_fuse_opt_add_arg(&fargs, "redivis");

    size_t ops_size;
    const void *ops = build_fuse_ops(ctx->api_version, &ops_size);

    if (ctx->api_version == 2) {
        ctx->chan = dl_fuse2_mount(ctx->mount_point, &fargs);
        if (!ctx->chan) {
            dl_fuse_opt_free_args(&fargs); ctx->running = 0; return NULL;
        }
        ctx->fuse = dl_fuse2_new(ctx->chan, &fargs, ops, ops_size, ctx);
        if (!ctx->fuse) {
            dl_fuse2_unmount(ctx->mount_point, ctx->chan);
            ctx->chan = NULL; dl_fuse_opt_free_args(&fargs);
            ctx->running = 0; return NULL;
        }
        ctx->running = 1;
        dl_fuse_loop(ctx->fuse);
        dl_fuse2_unmount(ctx->mount_point, ctx->chan);
        dl_fuse_destroy(ctx->fuse);
        rmdir(ctx->mount_point);
        ctx->fuse = NULL; ctx->chan = NULL; ctx->running = 0;
        dl_fuse_opt_free_args(&fargs);
        return NULL;
    } else {
        dl_fuse_opt_add_arg(&fargs, "-f");
        ctx->fuse = dl_fuse3_new(&fargs, ops, ops_size, ctx);
        if (!ctx->fuse) {
            dl_fuse_opt_free_args(&fargs); ctx->running = 0; return NULL;
        }
        if (dl_fuse3_mount(ctx->fuse, ctx->mount_point) != 0) {
            dl_fuse_destroy(ctx->fuse); ctx->fuse = NULL;
            dl_fuse_opt_free_args(&fargs); ctx->running = 0; return NULL;
        }
        ctx->running = 1;
        dl_fuse_loop(ctx->fuse);
        dl_fuse3_unmount(ctx->fuse);
        dl_fuse_destroy(ctx->fuse);
        rmdir(ctx->mount_point);
        ctx->fuse = NULL; ctx->running = 0;
        dl_fuse_opt_free_args(&fargs);
        return NULL;
    }
}

/* ------------------------------------------------------------------ */
/*  R interface                                                       */
/* ------------------------------------------------------------------ */

static void fuse_mount_finalizer(SEXP ptr) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ptr);
    if (!ctx) return;
    if (ctx->running && ctx->fuse) {
        dl_fuse_exit(ctx->fuse);
        if (ctx->api_version == 2 && ctx->chan)
            dl_fuse2_unmount(ctx->mount_point, ctx->chan);
        pthread_join(ctx->thread, NULL);
    }
    for (size_t i = 0; i < ctx->n_entries; i++) {
        pthread_mutex_destroy(&ctx->entries[i].mutex);
        free(ctx->entries[i].rel_path); free(ctx->entries[i].file_id);
        free(ctx->entries[i].cache_path); free(ctx->entries[i].bitmap_path);
        free(ctx->entries[i].bitmap);
    }
    free(ctx->entries);
    for (size_t i = 0; i < ctx->n_dirs; i++) {
        free(ctx->dirs[i].path);
        for (size_t j = 0; j < ctx->dirs[i].n_children; j++)
            free(ctx->dirs[i].child_names[j]);
        free(ctx->dirs[i].child_names); free(ctx->dirs[i].child_is_dir);
    }
    free(ctx->dirs);
    ht_free(&ctx->dir_ht); ht_free(&ctx->file_ht);
    free(ctx->mount_point); free(ctx->cache_dir);
    free(ctx->api_base_url); free(ctx->auth_token);
    free(ctx);
    R_ClearExternalPtr(ptr);
}

SEXP C_fuse_mount(SEXP s_mount_point, SEXP s_cache_dir,
                  SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids,
                  SEXP s_added_ats,
                  SEXP s_dir_paths, SEXP s_dir_child_names,
                  SEXP s_dir_child_is_dir,
                  SEXP s_api_base_url, SEXP s_auth_token) {
    int api_ver = load_fuse_library();
    if (api_ver == 0) {
        Rf_error("No FUSE library found. Install one of:\n"
                 "  - Linux:  sudo apt install fuse3  (or libfuse3-3)\n"
                 "  - macOS:  FUSE-T (https://www.fuse-t.org/) or macFUSE\n"
                 "No development headers or packages are needed.");
    }

    const char *mount_point  = CHAR(STRING_ELT(s_mount_point, 0));
    const char *cache_dir    = CHAR(STRING_ELT(s_cache_dir, 0));
    const char *api_base_url = CHAR(STRING_ELT(s_api_base_url, 0));
    const char *auth_token   = CHAR(STRING_ELT(s_auth_token, 0));
    R_xlen_t n = XLENGTH(s_rel_paths);
    R_xlen_t n_dirs = XLENGTH(s_dir_paths);

    redivis_mount_ctx_t *ctx = calloc(1, sizeof(redivis_mount_ctx_t));
    if (!ctx) Rf_error("fuse_mount: allocation failed");

    ctx->mount_point = strdup(mount_point); ctx->cache_dir = strdup(cache_dir);
    ctx->api_base_url = strdup(api_base_url); ctx->auth_token = strdup(auth_token);
    ctx->api_version = api_ver; ctx->n_entries = (size_t)n;
    ctx->entries = calloc((size_t)n, sizeof(redivis_file_entry_t));
    ht_init(&ctx->file_ht, (size_t)n);
    ht_init(&ctx->dir_ht, (size_t)n_dirs);

    if (!ctx->entries) {
        free(ctx->mount_point); free(ctx->cache_dir);
        free(ctx->api_base_url); free(ctx->auth_token); free(ctx);
        Rf_error("fuse_mount: allocation failed");
    }

    for (R_xlen_t i = 0; i < n; i++) {
        const char *rp = CHAR(STRING_ELT(s_rel_paths, i));
        const char *fid = CHAR(STRING_ELT(s_file_ids, i));
        redivis_file_entry_t *entry = &ctx->entries[i];
        entry->rel_path = strdup(rp); entry->file_id = strdup(fid);
        entry->size = (size_t)REAL(s_sizes)[i]; entry->fully_cached = 0;
        entry->added_at = (time_t)REAL(s_added_ats)[i];

        char cp[4096]; snprintf(cp, sizeof(cp), "%s/%s", cache_dir, rp);
        entry->cache_path = strdup(cp);
        char bp[4096]; snprintf(bp, sizeof(bp), "%s/%s.bitmap", cache_dir, rp);
        entry->bitmap_path = strdup(bp);

        entry->n_blocks = (entry->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if (entry->n_blocks == 0) entry->n_blocks = 1;
        entry->bitmap = calloc(bitmap_bytes(entry->n_blocks), 1);
        pthread_mutex_init(&entry->mutex, NULL);
        ht_insert(&ctx->file_ht, entry->rel_path, entry);

        if (bitmap_load(entry)) {
            struct stat st;
            if (stat(entry->cache_path, &st) == 0 &&
                (size_t)st.st_size == entry->size) {
                if (all_blocks_cached(entry->bitmap, entry->n_blocks))
                    entry->fully_cached = 1;
            } else {
                memset(entry->bitmap, 0, bitmap_bytes(entry->n_blocks));
            }
        }
    }

    ctx->n_dirs = (size_t)n_dirs;
    ctx->dirs = calloc((size_t)n_dirs, sizeof(dir_entry_t));
    for (R_xlen_t i = 0; i < n_dirs; i++) {
        dir_entry_t *d = &ctx->dirs[i];
        d->path = strdup(CHAR(STRING_ELT(s_dir_paths, i)));
        SEXP cnv = VECTOR_ELT(s_dir_child_names, i);
        SEXP civ = VECTOR_ELT(s_dir_child_is_dir, i);
        R_xlen_t nc = XLENGTH(cnv);
        d->n_children = (size_t)nc;
        d->child_names = calloc((size_t)nc, sizeof(char *));
        d->child_is_dir = calloc((size_t)nc, sizeof(int));
        for (R_xlen_t j = 0; j < nc; j++) {
            d->child_names[j] = strdup(CHAR(STRING_ELT(cnv, j)));
            d->child_is_dir[j] = LOGICAL(civ)[j];
        }
        ht_insert(&ctx->dir_ht, d->path, d);
    }

    mkdirs(mount_point);

    int rc = pthread_create(&ctx->thread, NULL, fuse_thread_func, ctx);
    if (rc != 0) {
        fuse_mount_finalizer(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
        Rf_error("fuse_mount: pthread_create failed (errno=%d)", rc);
    }

    usleep(300000);
    if (!ctx->running) {
        pthread_join(ctx->thread, NULL);
        fuse_mount_finalizer(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
        Rf_error("fuse_mount: FUSE failed to start. "
                 "Is the mount point accessible? "
                 "Check that /dev/fuse exists and is accessible.");
    }

    SEXP ptr = PROTECT(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, fuse_mount_finalizer, TRUE);
    UNPROTECT(1);
    return ptr;
}

SEXP C_fuse_unmount(SEXP ext_ptr) {
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ext_ptr);
    if (!ctx) return R_NilValue;
    if (ctx->running && ctx->fuse) {
        dl_fuse_exit(ctx->fuse);
        if (ctx->api_version == 2 && ctx->chan)
            dl_fuse2_unmount(ctx->mount_point, ctx->chan);
        pthread_join(ctx->thread, NULL);
    }
    fuse_mount_finalizer(ext_ptr);
    return R_NilValue;
}
