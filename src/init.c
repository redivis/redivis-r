#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

extern SEXP C_redivis_connection(SEXP, SEXP, SEXP, SEXP);
#ifndef _WIN32
extern SEXP C_fuse_mount(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP C_fuse_unmount(SEXP);
#endif

static const R_CallMethodDef CallEntries[] = {
    {"C_redivis_connection", (DL_FUNC) &C_redivis_connection, 4},
#ifndef _WIN32
    {"C_fuse_mount",         (DL_FUNC) &C_fuse_mount,         11},
    {"C_fuse_unmount",       (DL_FUNC) &C_fuse_unmount,       1},
#endif
    {NULL, NULL, 0}
};

void R_init_redivis(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
