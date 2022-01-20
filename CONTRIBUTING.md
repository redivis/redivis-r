## Local development
devtools::install()

- if devtools not installed, run install.packages("devtools")
- If you get SSL errors, run httr::set_config(httr::config(ssl_verifypeer = 0L))

Reload with Cmd + Shift + L or devtools::load_all(), experiment in console

## Testing
Sys.setenv(REDIVIS_API_TOKEN="<TOKEN>")
Sys.setenv(REDIVIS_API_ENDPOINT="https://localhost:8443/api/v1")
devtools::test()




https://r-pkgs.org/tests.html
