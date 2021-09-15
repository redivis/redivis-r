## Local development
devtools::install()

Reload with Cmd + Shift + L or devtools::load_all(), experimient in console

## Testing
Sys.setenv(REDIVIS_API_TOKEN="<TOKEN>")
Sys.setenv(REDIVIS_API_ENDPOINT="https://localhost:8443/api/v1")
devtools::test()

https://r-pkgs.org/tests.html
