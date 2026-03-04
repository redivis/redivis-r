#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Connections.h>
#include <string.h>

/* R_EOF is not exposed under R_NO_REMAP */
#ifndef R_EOF
#define R_EOF (-1)
#endif

/*
 * Connection state:
 *   - pos:            current logical byte offset in the remote file
 *   - size:           total file size
 *   - stream_open:    whether an HTTP streaming connection is active
 *   - stream_pos:     the byte offset at which the current stream was opened
 *   - env:            R environment containing callback functions:
 *       open_stream(start_byte)  – opens a streaming HTTP connection from offset
 *       read_stream(n)           – reads up to n bytes from the open stream (raw)
 *       close_stream()           – closes the current stream
 */
typedef struct {
    SEXP       env;
    R_xlen_t   pos;
    R_xlen_t   size;
    int        stream_open;
    R_xlen_t   stream_pos;     /* byte offset where the current stream starts */
} redivis_con_t;

/* Call env$open_stream(start_byte) */
static void call_open_stream(SEXP env, R_xlen_t start) {
    SEXP fn = Rf_findVarInFrame(env, Rf_install("open_stream"));
    if (fn == R_UnboundValue) {
        Rf_error("redivis connection: open_stream not found in environment");
    }
    SEXP call = PROTECT(Rf_lang2(fn, Rf_ScalarReal((double)start)));
    Rf_eval(call, env);
    UNPROTECT(1);
}

/* Call env$read_stream(n) -> raw vector.
 * If the stream was idle-closed by R, returns raw(0) AND sets
 * env$stream_con to NULL.  We detect that via is_stream_open(). */
static SEXP call_read_stream(SEXP env, R_xlen_t n) {
    SEXP fn = Rf_findVarInFrame(env, Rf_install("read_stream"));
    if (fn == R_UnboundValue) {
        Rf_error("redivis connection: read_stream not found in environment");
    }
    SEXP call = PROTECT(Rf_lang2(fn, Rf_ScalarReal((double)n)));
    SEXP result = PROTECT(Rf_eval(call, env));
    UNPROTECT(2);
    return result;
}

/* Check whether the R-level stream is still open (env$stream_con != NULL) */
static int is_stream_open(SEXP env) {
    SEXP val = Rf_findVarInFrame(env, Rf_install("stream_con"));
    return (val != R_NilValue && val != R_UnboundValue);
}

/* Call env$close_stream() */
static void call_close_stream(SEXP env) {
    SEXP fn = Rf_findVarInFrame(env, Rf_install("close_stream"));
    if (fn == R_UnboundValue) return;  /* tolerate missing */
    SEXP call = PROTECT(Rf_lang1(fn));
    Rf_eval(call, env);
    UNPROTECT(1);
}

/* Ensure there is an open stream positioned at ctx->pos */
static void ensure_stream(redivis_con_t *ctx) {
    if (!ctx->stream_open) {
        call_open_stream(ctx->env, ctx->pos);
        ctx->stream_open = 1;
        ctx->stream_pos = ctx->pos;
    }
}

/* Invalidate (close) the current stream so the next read re-opens */
static void invalidate_stream(redivis_con_t *ctx) {
    if (ctx->stream_open) {
        call_close_stream(ctx->env);
        ctx->stream_open = 0;
    }
}

static Rboolean redivis_open(Rconnection con) {
    con->isopen = TRUE;
    return TRUE;
}

static void redivis_close(Rconnection con) {
    redivis_con_t *ctx = (redivis_con_t *)con->private;
    if (ctx) {
        /* Only close the stream if we can safely call back into R.
         * When called during GC finalization, Rf_eval is unsafe.
         * Skip the Rf_eval-based stream close here; the R-side
         * reg.finalizer on the environment will handle it safely. */
        if (ctx->stream_open) {
            ctx->stream_open = 0;
        }
        R_ReleaseObject(ctx->env);
        free(ctx);
        con->private = NULL;
    }
    con->isopen = FALSE;
}

static int redivis_fgetc_internal(Rconnection con) {
    redivis_con_t *ctx = (redivis_con_t *)con->private;
    if (ctx->pos >= ctx->size) return R_EOF;

    ensure_stream(ctx);

    SEXP raw = PROTECT(call_read_stream(ctx->env, 1));
    if (XLENGTH(raw) == 0 && !is_stream_open(ctx->env)) {
        /* Idle timeout — re-open and retry */
        UNPROTECT(1);
        ctx->stream_open = 0;
        ensure_stream(ctx);
        raw = PROTECT(call_read_stream(ctx->env, 1));
    }
    if (XLENGTH(raw) == 0) {
        UNPROTECT(1);
        return R_EOF;
    }
    int byte = (int)(RAW(raw)[0]);
    ctx->pos++;
    UNPROTECT(1);
    return byte;
}

static size_t redivis_read(void *buf, size_t size, size_t nitems,
                           Rconnection con)
{
    redivis_con_t *ctx = (redivis_con_t *)con->private;
    size_t total_requested = size * nitems;
    if (total_requested == 0 || size == 0) return 0;

    if (ctx->pos >= ctx->size) return 0;

    R_xlen_t remaining = ctx->size - ctx->pos;
    if ((R_xlen_t)total_requested > remaining) {
        total_requested = (size_t)remaining;
    }

    ensure_stream(ctx);

    SEXP raw = PROTECT(call_read_stream(ctx->env, (R_xlen_t)total_requested));
    R_xlen_t got = XLENGTH(raw);

    /* If we got 0 bytes but haven't reached EOF, the R layer may have
       closed the stream due to idle timeout. Re-open and retry once. */
    if (got == 0 && ctx->pos < ctx->size && !is_stream_open(ctx->env)) {
        UNPROTECT(1);
        ctx->stream_open = 0;  /* sync C state with R state */
        ensure_stream(ctx);    /* re-open from ctx->pos */
        raw = PROTECT(call_read_stream(ctx->env, (R_xlen_t)total_requested));
        got = XLENGTH(raw);
    }

    if (got > (R_xlen_t)total_requested) got = (R_xlen_t)total_requested;
    if (got > 0) {
        memcpy(buf, RAW(raw), (size_t)got);
        ctx->pos += got;
    }
    UNPROTECT(1);

    return (size_t)(got / size);
}

static double redivis_seek(Rconnection con, double where, int origin,
                           int rw)
{
    redivis_con_t *ctx = (redivis_con_t *)con->private;
    double old_pos = (double)ctx->pos;

    if (!ISNA(where)) {
        R_xlen_t new_pos;
        switch (origin) {
            case 1:
                new_pos = (R_xlen_t)where;
                break;
            case 2:
                new_pos = ctx->pos + (R_xlen_t)where;
                break;
            case 3:
                new_pos = ctx->size + (R_xlen_t)where;
                break;
            default:
                new_pos = (R_xlen_t)where;
                break;
        }
        if (new_pos < 0) new_pos = 0;
        if (new_pos > ctx->size) new_pos = ctx->size;

        if (new_pos != ctx->pos) {
            /* Position changed — invalidate the stream so next read
               opens a new HTTP connection from the new offset */
            invalidate_stream(ctx);
            ctx->pos = new_pos;
        }
    }

    return old_pos;
}

SEXP C_redivis_connection(SEXP env, SEXP description, SEXP s_size, SEXP s_mode) {
    const char *desc = CHAR(Rf_asChar(description));
    const char *mode = CHAR(Rf_asChar(s_mode));
    double dsize = Rf_asReal(s_size);

    int is_text = (strchr(mode, 'b') == NULL);

    Rconnection con;
    SEXP rc = PROTECT(R_new_custom_connection(
        desc,
        mode,
        "redivis",
        &con
    ));

    redivis_con_t *ctx = (redivis_con_t *)malloc(sizeof(redivis_con_t));
    if (!ctx) {
        UNPROTECT(1);
        Rf_error("redivis connection: failed to allocate memory");
    }
    R_PreserveObject(env);
    ctx->env         = env;
    ctx->pos         = 0;
    ctx->size        = (R_xlen_t)dsize;
    ctx->stream_open = 0;
    ctx->stream_pos  = 0;

    con->private        = ctx;
    con->isopen         = TRUE;
    con->canread        = TRUE;
    con->canwrite       = FALSE;
    con->canseek        = TRUE;
    con->blocking       = TRUE;
    con->text           = is_text ? TRUE : FALSE;
    con->open           = redivis_open;
    con->close          = redivis_close;
    con->read           = redivis_read;
    con->fgetc_internal = redivis_fgetc_internal;
    con->seek           = redivis_seek;

    UNPROTECT(1);
    return rc;
}
