#include <R.h>
#include <Rinternals.h>

SEXP C_check_interrupt(void) {
    R_CheckUserInterrupt();
    return R_NilValue;
}
