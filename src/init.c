#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

extern SEXP C_redivis_connection(SEXP, SEXP, SEXP, SEXP);
extern SEXP C_fuse_mount(SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP C_fuse_unmount(SEXP);
extern SEXP C_check_interrupt(void);

static const R_CallMethodDef CallEntries[] = {
    {"C_redivis_connection", (DL_FUNC) &C_redivis_connection, 4},
    {"C_fuse_mount",         (DL_FUNC) &C_fuse_mount,         11},
    {"C_fuse_unmount",       (DL_FUNC) &C_fuse_unmount,       1},
    {"C_check_interrupt",    (DL_FUNC) &C_check_interrupt,     0},
    {NULL, NULL, 0}
};

void R_init_redivis(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
