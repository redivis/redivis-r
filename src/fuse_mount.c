/*
 * fuse_mount.c — FUSE3 read-only filesystem for Redivis Directory objects
 *
 * Exposes a directory tree of Redivis files as a local FUSE mount.
 * File contents are lazily fetched from Redivis via HTTP Range requests
 * on a per-block basis and cached on disk. Once a block is fetched it is
 * never re-downloaded. The FUSE event loop runs on a background pthread
 * so that R remains interactive.
 *
 * If HAVE_FUSE is not defined at compile time, this file provides stub
 * implementations of C_fuse_mount / C_fuse_unmount that error out with
 * a helpful message, so the package loads on any system.
 */

#include <R.h>
#include <Rinternals.h>

/* ================================================================== */
/*  No-FUSE stubs                                                     */
/* ================================================================== */
#ifndef HAVE_FUSE

SEXP C_fuse_mount(SEXP s_mount_point, SEXP s_cache_dir,
                  SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids,
                  SEXP s_api_base_url, SEXP s_auth_token)
{
    Rf_error("FUSE support was not available when the package was compiled. "
             "Install libfuse3-dev (Linux), macFUSE, or FUSE-T (macOS) and "
             "reinstall the package to enable directory mounting.");
    return R_NilValue;
}

SEXP C_fuse_unmount(SEXP ext_ptr)
{
    Rf_error("FUSE support was not available when the package was compiled.");
    return R_NilValue;
}

#else /* HAVE_FUSE */

/*
 * FUSE API selection:
 *   - HAVE_FUSE3 is defined by Makevars when libfuse3 or FUSE-T is detected
 *     via `pkg-config fuse3`. This provides the FUSE 3.x API.
 *   - Otherwise we fall back to the FUSE 2.x API (macFUSE on macOS).
 *
 * FUSE-T (macOS) provides a native FUSE 3 API without a kernel extension,
 * so it uses the same code path as Linux libfuse3.
 */
#ifdef HAVE_FUSE3
  #define FUSE_USE_VERSION 31
  #include <fuse3/fuse.h>
  #define REDIVIS_FUSE2 0
#else
  #define FUSE_USE_VERSION 26
  #include <fuse/fuse.h>
  #define REDIVIS_FUSE2 1
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <curl/curl.h>

/* ------------------------------------------------------------------ */
/*  Constants                                                         */
/* ------------------------------------------------------------------ */

/* Block size for sparse cache: 4 MiB */
#define BLOCK_SIZE (4 * 1024 * 1024)

/* ------------------------------------------------------------------ */
/*  Data structures                                                   */
/* ------------------------------------------------------------------ */

typedef struct redivis_file_entry {
    char   *rel_path;       /* e.g. "sub/file.h5"                     */
    size_t  size;           /* file size in bytes                      */
    char   *file_id;        /* Redivis file id for API calls           */
    char   *cache_path;     /* absolute path to on-disk cache file     */
    char   *bitmap_path;    /* absolute path to block bitmap file      */
    int     fully_cached;   /* 1 once every block has been downloaded  */

    /* Block bitmap: 1 bit per block, 1 = cached */
    unsigned char *bitmap;
    size_t  n_blocks;

    /* Mutex for per-file download serialisation */
    pthread_mutex_t mutex;
} redivis_file_entry_t;

typedef struct redivis_mount_ctx {
    char                  *mount_point;
    char                  *cache_dir;
    char                  *api_base_url;
    char                  *auth_token;
    redivis_file_entry_t  *entries;
    size_t                 n_entries;
    struct fuse           *fuse;
#if REDIVIS_FUSE2
    struct fuse_chan       *chan;
#endif
    pthread_t              thread;
    int                    running;
} redivis_mount_ctx_t;

/* Per-open-file handle: keeps an fd to the cache file */
typedef struct redivis_fh {
    redivis_file_entry_t *entry;
    int                   fd;
} redivis_fh_t;

/* ------------------------------------------------------------------ */
/*  Bitmap helpers                                                    */
/* ------------------------------------------------------------------ */

static inline int bitmap_get(const unsigned char *bm, size_t idx) {
    return (bm[idx / 8] >> (idx % 8)) & 1;
}

static inline void bitmap_set(unsigned char *bm, size_t idx) {
    bm[idx / 8] |= (unsigned char)(1u << (idx % 8));
}

static size_t bitmap_bytes(size_t n_blocks) {
    return (n_blocks + 7) / 8;
}

static int all_blocks_cached(const unsigned char *bm, size_t n_blocks) {
    for (size_t i = 0; i < n_blocks; i++) {
        if (!bitmap_get(bm, i)) return 0;
    }
    return 1;
}

/* Persist bitmap to disk so cache survives across sessions */
static void bitmap_save(redivis_file_entry_t *entry) {
    FILE *fp = fopen(entry->bitmap_path, "wb");
    if (fp) {
        fwrite(entry->bitmap, 1, bitmap_bytes(entry->n_blocks), fp);
        fclose(fp);
    }
}

/* Load bitmap from disk; returns 1 if loaded, 0 otherwise */
static int bitmap_load(redivis_file_entry_t *entry) {
    size_t nb = bitmap_bytes(entry->n_blocks);
    FILE *fp = fopen(entry->bitmap_path, "rb");
    if (!fp) return 0;

    size_t rd = fread(entry->bitmap, 1, nb, fp);
    fclose(fp);
    if (rd != nb) {
        memset(entry->bitmap, 0, nb);
        return 0;
    }
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

static redivis_file_entry_t *find_entry(redivis_mount_ctx_t *ctx,
                                        const char *path)
{
    const char *rel = path;
    while (*rel == '/') rel++;

    for (size_t i = 0; i < ctx->n_entries; i++) {
        if (strcmp(ctx->entries[i].rel_path, rel) == 0)
            return &ctx->entries[i];
    }
    return NULL;
}

static int is_directory(redivis_mount_ctx_t *ctx, const char *prefix)
{
    size_t plen = strlen(prefix);
    if (plen == 0) return 1;

    for (size_t i = 0; i < ctx->n_entries; i++) {
        const char *rp = ctx->entries[i].rel_path;
        if (strncmp(rp, prefix, plen) == 0 && rp[plen] == '/')
            return 1;
    }
    return 0;
}

/* Recursive mkdir -p */
static void mkdirs(const char *dir)
{
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s", dir);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

/* ------------------------------------------------------------------ */
/*  Block-level download via libcurl Range requests                   */
/* ------------------------------------------------------------------ */

typedef struct {
    int     fd;
    off_t   offset;
    size_t  written;
} range_write_ctx_t;

static size_t curl_range_write_cb(void *ptr, size_t size, size_t nmemb,
                                  void *userdata)
{
    range_write_ctx_t *wctx = (range_write_ctx_t *)userdata;
    size_t total = size * nmemb;
    ssize_t n = pwrite(wctx->fd, ptr, total, wctx->offset + (off_t)wctx->written);
    if (n < 0) return 0;
    wctx->written += (size_t)n;
    return (size_t)n;
}

static int ensure_blocks_cached(redivis_mount_ctx_t *ctx,
                                redivis_file_entry_t *entry,
                                int fd,
                                size_t blk_start,
                                size_t blk_end)
{
    if (entry->fully_cached) return 0;

    pthread_mutex_lock(&entry->mutex);

    size_t i = blk_start;
    while (i < blk_end) {
        if (bitmap_get(entry->bitmap, i)) {
            i++;
            continue;
        }

        size_t run_start = i;
        while (i < blk_end && !bitmap_get(entry->bitmap, i)) {
            i++;
        }
        size_t run_end = i;

        off_t byte_start = (off_t)run_start * BLOCK_SIZE;
        off_t byte_end   = (off_t)run_end * BLOCK_SIZE - 1;
        if ((size_t)(byte_end + 1) > entry->size)
            byte_end = (off_t)(entry->size - 1);

        char url[4096];
        snprintf(url, sizeof(url), "%s/rawFiles/%s",
                 ctx->api_base_url, entry->file_id);

        char range_val[128];
        snprintf(range_val, sizeof(range_val), "%lld-%lld",
                 (long long)byte_start, (long long)byte_end);

        CURL *curl = curl_easy_init();
        if (!curl) {
            pthread_mutex_unlock(&entry->mutex);
            return -EIO;
        }

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

        if (res != CURLE_OK) {
            pthread_mutex_unlock(&entry->mutex);
            return -EIO;
        }

        for (size_t b = run_start; b < run_end; b++) {
            bitmap_set(entry->bitmap, b);
        }

        bitmap_save(entry);
    }

    if (all_blocks_cached(entry->bitmap, entry->n_blocks)) {
        entry->fully_cached = 1;
    }

    pthread_mutex_unlock(&entry->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  FUSE callbacks                                                    */
/* ------------------------------------------------------------------ */

#if REDIVIS_FUSE2

static void *fuse_init_cb(struct fuse_conn_info *conn)
{
    (void)conn;
    return fuse_get_context()->private_data;
}

static int fuse_getattr_cb(const char *path, struct stat *stbuf)
{
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;

#else /* FUSE 3 */

static void *fuse_init_cb(struct fuse_conn_info *conn,
                          struct fuse_config *cfg)
{
    (void)conn;
    cfg->kernel_cache = 1;
    cfg->auto_cache   = 1;
    cfg->entry_timeout    = 86400;
    cfg->attr_timeout     = 86400;
    cfg->negative_timeout = 0;
    return fuse_get_context()->private_data;
}

static int fuse_getattr_cb(const char *path, struct stat *stbuf,
                           struct fuse_file_info *fi)
{
    (void)fi;
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;

#endif /* REDIVIS_FUSE2 */

    memset(stbuf, 0, sizeof(struct stat));

    const char *rel = path;
    while (*rel == '/') rel++;

    if (strlen(rel) == 0) {
        stbuf->st_mode  = S_IFDIR | 0555;
        stbuf->st_nlink = 2;
        return 0;
    }

    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (entry) {
        stbuf->st_mode  = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size  = (off_t)entry->size;
        return 0;
    }

    if (is_directory(ctx, rel)) {
        stbuf->st_mode  = S_IFDIR | 0555;
        stbuf->st_nlink = 2;
        return 0;
    }

    return -ENOENT;
}

#if REDIVIS_FUSE2

static int fuse_readdir_cb(const char *path, void *buf,
                           fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info *fi)
{
    (void)offset; (void)fi;
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;

    const char *rel = path;
    while (*rel == '/') rel++;
    size_t plen = strlen(rel);

    filler(buf, ".",  NULL, 0);
    filler(buf, "..", NULL, 0);

    char **added = NULL;
    size_t n_added = 0;

    for (size_t i = 0; i < ctx->n_entries; i++) {
        const char *rp = ctx->entries[i].rel_path;

        if (plen > 0) {
            if (strncmp(rp, rel, plen) != 0 || rp[plen] != '/') continue;
            rp += plen + 1;
        }

        const char *slash = strchr(rp, '/');
        size_t name_len = slash ? (size_t)(slash - rp) : strlen(rp);
        if (name_len == 0) continue;

        int found = 0;
        for (size_t j = 0; j < n_added; j++) {
            if (strlen(added[j]) == name_len &&
                strncmp(added[j], rp, name_len) == 0) {
                found = 1;
                break;
            }
        }

        if (!found) {
            char name[1024];
            if (name_len >= sizeof(name)) name_len = sizeof(name) - 1;
            memcpy(name, rp, name_len);
            name[name_len] = '\0';

            filler(buf, name, NULL, 0);

            added = realloc(added, (n_added + 1) * sizeof(char *));
            added[n_added] = strdup(name);
            n_added++;
        }
    }

    for (size_t j = 0; j < n_added; j++) free(added[j]);
    free(added);
    return 0;
}

#else /* FUSE 3 */

static int fuse_readdir_cb(const char *path, void *buf,
                           fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info *fi,
                           enum fuse_readdir_flags flags)
{
    (void)offset; (void)fi; (void)flags;
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;

    const char *rel = path;
    while (*rel == '/') rel++;
    size_t plen = strlen(rel);

    filler(buf, ".",  NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);

    char **added = NULL;
    size_t n_added = 0;

    for (size_t i = 0; i < ctx->n_entries; i++) {
        const char *rp = ctx->entries[i].rel_path;

        if (plen > 0) {
            if (strncmp(rp, rel, plen) != 0 || rp[plen] != '/') continue;
            rp += plen + 1;
        }

        const char *slash = strchr(rp, '/');
        size_t name_len = slash ? (size_t)(slash - rp) : strlen(rp);
        if (name_len == 0) continue;

        int found = 0;
        for (size_t j = 0; j < n_added; j++) {
            if (strlen(added[j]) == name_len &&
                strncmp(added[j], rp, name_len) == 0) {
                found = 1;
                break;
            }
        }

        if (!found) {
            char name[1024];
            if (name_len >= sizeof(name)) name_len = sizeof(name) - 1;
            memcpy(name, rp, name_len);
            name[name_len] = '\0';

            filler(buf, name, NULL, 0, 0);

            added = realloc(added, (n_added + 1) * sizeof(char *));
            added[n_added] = strdup(name);
            n_added++;
        }
    }

    for (size_t j = 0; j < n_added; j++) free(added[j]);
    free(added);
    return 0;
}

#endif /* REDIVIS_FUSE2 */

static int fuse_open_cb(const char *path, struct fuse_file_info *fi)
{
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;
    redivis_file_entry_t *entry = find_entry(ctx, path);
    if (!entry) return -ENOENT;
    if ((fi->flags & O_ACCMODE) != O_RDONLY) return -EACCES;

    {
        char *dup = strdup(entry->cache_path);
        char *sl  = strrchr(dup, '/');
        if (sl) { *sl = '\0'; mkdirs(dup); }
        free(dup);
    }

    int fd = open(entry->cache_path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return -errno;

    if (entry->size > 0) {
        struct stat st;
        if (fstat(fd, &st) == 0 && (size_t)st.st_size < entry->size) {
            ftruncate(fd, (off_t)entry->size);
        }
    }

    redivis_fh_t *fh = malloc(sizeof(redivis_fh_t));
    if (!fh) { close(fd); return -ENOMEM; }
    fh->entry = entry;
    fh->fd    = fd;

    fi->fh = (uint64_t)(uintptr_t)fh;
    fi->keep_cache = 1;
    return 0;
}

static int fuse_read_cb(const char *path, char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi)
{
    (void)path;
    redivis_fh_t *fh = (redivis_fh_t *)(uintptr_t)fi->fh;
    redivis_file_entry_t *entry = fh->entry;
    redivis_mount_ctx_t *ctx = fuse_get_context()->private_data;

    if ((size_t)offset >= entry->size) return 0;
    if ((size_t)offset + size > entry->size)
        size = entry->size - (size_t)offset;

    size_t blk_start = (size_t)offset / BLOCK_SIZE;
    size_t blk_end   = ((size_t)offset + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (blk_end > entry->n_blocks) blk_end = entry->n_blocks;

    int rc = ensure_blocks_cached(ctx, entry, fh->fd, blk_start, blk_end);
    if (rc != 0) return rc;

    ssize_t n = pread(fh->fd, buf, size, offset);
    if (n < 0) return -errno;
    return (int)n;
}

static int fuse_release_cb(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    redivis_fh_t *fh = (redivis_fh_t *)(uintptr_t)fi->fh;
    close(fh->fd);
    free(fh);
    return 0;
}

static const struct fuse_operations redivis_fuse_ops = {
    .init    = fuse_init_cb,
    .getattr = fuse_getattr_cb,
    .readdir = fuse_readdir_cb,
    .open    = fuse_open_cb,
    .read    = fuse_read_cb,
    .release = fuse_release_cb,
};

/* ------------------------------------------------------------------ */
/*  Background thread                                                 */
/* ------------------------------------------------------------------ */

static void *fuse_thread_func(void *arg)
{
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)arg;
    struct fuse_args fargs = FUSE_ARGS_INIT(0, NULL);
    fuse_opt_add_arg(&fargs, "redivis");
#if !REDIVIS_FUSE2
    fuse_opt_add_arg(&fargs, "-f");          /* foreground — FUSE 2 / FUSE-T doesn't need this */
#endif

#if REDIVIS_FUSE2
    ctx->chan = fuse_mount(ctx->mount_point, &fargs);
    if (!ctx->chan) {
        fuse_opt_free_args(&fargs);
        ctx->running = 0;
        return NULL;
    }

    ctx->fuse = fuse_new(ctx->chan, &fargs, &redivis_fuse_ops,
                         sizeof(redivis_fuse_ops), ctx);
    if (!ctx->fuse) {
        fuse_unmount(ctx->mount_point, ctx->chan);
        ctx->chan = NULL;
        fuse_opt_free_args(&fargs);
        ctx->running = 0;
        return NULL;
    }

    ctx->running = 1;
    fuse_loop(ctx->fuse);

    fuse_unmount(ctx->mount_point, ctx->chan);
    fuse_destroy(ctx->fuse);

    /* Remove the empty mount point directory */
    rmdir(ctx->mount_point);

    ctx->fuse = NULL;
    ctx->chan = NULL;
    ctx->running = 0;
    fuse_opt_free_args(&fargs);
    return NULL;

#else /* FUSE 3 */

    ctx->fuse = fuse_new(&fargs, &redivis_fuse_ops,
                         sizeof(redivis_fuse_ops), ctx);
    if (!ctx->fuse) {
        fuse_opt_free_args(&fargs);
        ctx->running = 0;
        return NULL;
    }

    if (fuse_mount(ctx->fuse, ctx->mount_point) != 0) {
        fuse_destroy(ctx->fuse);
        ctx->fuse = NULL;
        fuse_opt_free_args(&fargs);
        ctx->running = 0;
        return NULL;
    }

    ctx->running = 1;
    fuse_loop(ctx->fuse);              /* blocks until unmount */

    fuse_unmount(ctx->fuse);
    fuse_destroy(ctx->fuse);

    /* Remove the empty mount point directory */
    rmdir(ctx->mount_point);

    ctx->fuse = NULL;
    ctx->running = 0;
    fuse_opt_free_args(&fargs);
    return NULL;

#endif /* REDIVIS_FUSE2 */
}

/* ------------------------------------------------------------------ */
/*  R interface                                                       */
/* ------------------------------------------------------------------ */

static void fuse_mount_finalizer(SEXP ptr)
{
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ptr);
    if (!ctx) return;

    if (ctx->running && ctx->fuse) {
        fuse_exit(ctx->fuse);
#if REDIVIS_FUSE2
        if (ctx->chan) {
            fuse_unmount(ctx->mount_point, ctx->chan);
        }
#endif
        pthread_join(ctx->thread, NULL);
    }

    for (size_t i = 0; i < ctx->n_entries; i++) {
        pthread_mutex_destroy(&ctx->entries[i].mutex);
        free(ctx->entries[i].rel_path);
        free(ctx->entries[i].file_id);
        free(ctx->entries[i].cache_path);
        free(ctx->entries[i].bitmap_path);
        free(ctx->entries[i].bitmap);
    }
    free(ctx->entries);
    free(ctx->mount_point);
    free(ctx->cache_dir);
    free(ctx->api_base_url);
    free(ctx->auth_token);
    free(ctx);

    R_ClearExternalPtr(ptr);
}

SEXP C_fuse_mount(SEXP s_mount_point, SEXP s_cache_dir,
                  SEXP s_rel_paths, SEXP s_sizes, SEXP s_file_ids,
                  SEXP s_api_base_url, SEXP s_auth_token)
{
    const char *mount_point  = CHAR(STRING_ELT(s_mount_point, 0));
    const char *cache_dir    = CHAR(STRING_ELT(s_cache_dir, 0));
    const char *api_base_url = CHAR(STRING_ELT(s_api_base_url, 0));
    const char *auth_token   = CHAR(STRING_ELT(s_auth_token, 0));
    R_xlen_t n = XLENGTH(s_rel_paths);

    redivis_mount_ctx_t *ctx = calloc(1, sizeof(redivis_mount_ctx_t));
    if (!ctx) Rf_error("fuse_mount: allocation failed");

    ctx->mount_point  = strdup(mount_point);
    ctx->cache_dir    = strdup(cache_dir);
    ctx->api_base_url = strdup(api_base_url);
    ctx->auth_token   = strdup(auth_token);
    ctx->n_entries    = (size_t)n;
    ctx->entries      = calloc((size_t)n, sizeof(redivis_file_entry_t));

    if (!ctx->entries) {
        free(ctx->mount_point); free(ctx->cache_dir);
        free(ctx->api_base_url); free(ctx->auth_token);
        free(ctx);
        Rf_error("fuse_mount: allocation failed");
    }

    for (R_xlen_t i = 0; i < n; i++) {
        const char *rp = CHAR(STRING_ELT(s_rel_paths, i));
        const char *fid = CHAR(STRING_ELT(s_file_ids, i));
        double fsize = REAL(s_sizes)[i];

        redivis_file_entry_t *entry = &ctx->entries[i];

        entry->rel_path   = strdup(rp);
        entry->file_id    = strdup(fid);
        entry->size       = (size_t)fsize;
        entry->fully_cached = 0;

        char cp[4096];
        snprintf(cp, sizeof(cp), "%s/%s", cache_dir, rp);
        entry->cache_path = strdup(cp);

        char bp[4096];
        snprintf(bp, sizeof(bp), "%s/%s.bitmap", cache_dir, rp);
        entry->bitmap_path = strdup(bp);

        entry->n_blocks = (entry->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if (entry->n_blocks == 0) entry->n_blocks = 1;
        entry->bitmap = calloc(bitmap_bytes(entry->n_blocks), 1);

        pthread_mutex_init(&entry->mutex, NULL);

        if (bitmap_load(entry)) {
            struct stat st;
            if (stat(entry->cache_path, &st) == 0 &&
                (size_t)st.st_size == entry->size) {
                if (all_blocks_cached(entry->bitmap, entry->n_blocks)) {
                    entry->fully_cached = 1;
                }
            } else {
                memset(entry->bitmap, 0, bitmap_bytes(entry->n_blocks));
            }
        }
    }

    mkdirs(mount_point);

    int rc = pthread_create(&ctx->thread, NULL, fuse_thread_func, ctx);
    if (rc != 0) {
        for (size_t i = 0; i < ctx->n_entries; i++) {
            pthread_mutex_destroy(&ctx->entries[i].mutex);
            free(ctx->entries[i].rel_path);
            free(ctx->entries[i].file_id);
            free(ctx->entries[i].cache_path);
            free(ctx->entries[i].bitmap_path);
            free(ctx->entries[i].bitmap);
        }
        free(ctx->entries);
        free(ctx->mount_point); free(ctx->cache_dir);
        free(ctx->api_base_url); free(ctx->auth_token);
        free(ctx);
        Rf_error("fuse_mount: pthread_create failed (errno=%d)", rc);
    }

    usleep(300000);

    if (!ctx->running) {
        pthread_join(ctx->thread, NULL);
        for (size_t i = 0; i < ctx->n_entries; i++) {
            pthread_mutex_destroy(&ctx->entries[i].mutex);
            free(ctx->entries[i].rel_path);
            free(ctx->entries[i].file_id);
            free(ctx->entries[i].cache_path);
            free(ctx->entries[i].bitmap_path);
            free(ctx->entries[i].bitmap);
        }
        free(ctx->entries);
        free(ctx->mount_point); free(ctx->cache_dir);
        free(ctx->api_base_url); free(ctx->auth_token);
        free(ctx);
        Rf_error("fuse_mount: FUSE failed to start. "
                 "Is libfuse3 installed? Is the mount point accessible?");
    }

    SEXP ptr = PROTECT(R_MakeExternalPtr(ctx, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(ptr, fuse_mount_finalizer, TRUE);
    UNPROTECT(1);
    return ptr;
}

SEXP C_fuse_unmount(SEXP ext_ptr)
{
    redivis_mount_ctx_t *ctx = (redivis_mount_ctx_t *)R_ExternalPtrAddr(ext_ptr);
    if (!ctx) return R_NilValue;

    if (ctx->running && ctx->fuse) {
        fuse_exit(ctx->fuse);
#if REDIVIS_FUSE2
        if (ctx->chan) {
            fuse_unmount(ctx->mount_point, ctx->chan);
        }
#endif
        pthread_join(ctx->thread, NULL);
    }

    fuse_mount_finalizer(ext_ptr);
    return R_NilValue;
}

#endif /* HAVE_FUSE */
