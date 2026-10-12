# Tests that hit the real Redivis API need a network connection and
# credentials. Without credentials, a request would start an interactive login
# and wait for the device code to expire, so skip instead (e.g. on CRAN and
# r-universe). Locally, log in once, or set REDIVIS_API_TOKEN, to run them.
skip_if_no_live_api <- function() {
  testthat::skip_on_cran()
  testthat::skip_if_offline("redivis.com")
  testthat::skip_if_not(has_credentials(), "no Redivis credentials")
}
