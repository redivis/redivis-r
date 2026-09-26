## Local development
devtools::install()

- if devtools not installed, run install.packages("devtools")
- If you get SSL errors, run httr::set_config(httr::config(ssl_verifypeer = 0L))
- To install updates, run `devtools::install()`

Reload with Cmd + Shift + L or devtools::load_all(), experiment in console

## Testing
Sys.setenv(REDIVIS_API_ENDPOINT="https://local.host:8443/api/v1")
devtools::test()

The tests in `tests/testthat/test-offline-*.R` run against a mock of the API (see `helper-mock-api.R`), so they need no server or credentials:
devtools::test(filter = "offline")

## Building
- Run `devtools::document()` to run roxygen

https://r-pkgs.org/tests.html
