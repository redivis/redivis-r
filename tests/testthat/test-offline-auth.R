# Credential handling and the OAuth device flow. Runs against the mock API in
# helper-mock-api.R, which also stands in for the OAuth endpoints.

# local_mock_api() replaces the login flow with a guard, so keep the real one
real_perform_oauth_login <- perform_oauth_login

TABLE <- "/tables/a.b.mock"

local_real_login <- function(env = parent.frame()) {
  local_mocked_bindings(perform_oauth_login = real_perform_oauth_login, .env = env)
}

jwt_with_scope <- function(scope) {
  payload <- sub(
    "=+$",
    "",
    jsonlite::base64url_enc(charToRaw(jsonlite::toJSON(list(scope = scope), auto_unbox = TRUE)))
  )
  paste0("header.", payload, ".signature")
}

test_that("a login always requests the default scope", {
  mock <- local_mock_api()
  local_real_login()

  capture.output(perform_oauth_login(scope = list("data.data")))

  expect_equal(mock$oauth_scopes(), "data.edit workflow.write data.data")
})

test_that("a scope upgrade requests every scope the server asked for", {
  mock <- local_mock_api()
  local_real_login()
  mock$set(
    response_script = list(
      list(
        status = 403,
        body = list(status = 403, error = "insufficient_scope", scope = "data.data other.scope")
      ),
      list(status = NULL)
    )
  )

  capture.output(result <- make_request(path = TABLE))

  expect_equal(result$name, "mock")
  expect_equal(
    mock$oauth_scopes(),
    "data.edit workflow.write data.data other.scope"
  )
})

test_that("a private resource prompts a login, then succeeds", {
  mock <- local_mock_api(authenticated = FALSE)
  local_real_login()
  mock$set(require_auth = TRUE)

  capture.output(table <- mock_table()$get())

  expect_equal(table$properties$name, "mock")
  expect_length(mock$oauth_scopes(), 1)
  expect_equal(mock$authorized(), c(FALSE, TRUE))
})

test_that("a login that the API asks for is requested with the current token", {
  local_mock_api()
  local_real_login()
  auth_vars$cached_credentials <- fake_credentials()
  request <- NULL
  local_mocked_bindings(
    req_perform = function(req, ...) {
      request <<- req
      stop("stopped before the login")
    },
    .package = "httr2"
  )

  expect_error(
    perform_oauth_login(
      scope = NULL,
      amr_values = "123 456",
      upgrade_credentials = TRUE
    ),
    "stopped before the login"
  )

  expect_equal(httr2::req_get_body(request)$amr_values, "123 456")
  expect_equal(
    httr2::req_get_headers(request, "reveal")$Authorization,
    paste("Bearer", fake_credentials()$access_token)
  )
})

test_that("a login saves credentials privately, creating their directory", {
  local_mock_api(authenticated = FALSE)
  local_real_login()
  expect_false(dir.exists(get_redivis_dir()))

  capture.output(perform_oauth_login(scope = NULL))

  saved <- jsonlite::fromJSON(get_credentials_file())
  expect_equal(saved$refresh_token, "refresh-token")

  # Windows files don't have Unix permissions
  skip_on_os("windows")
  expect_equal(format(file.info(get_credentials_file())$mode), "600")
  expect_equal(format(file.info(get_credentials_dir())$mode), "700")
})

test_that("the credentials directory can be overridden", {
  local_mock_api()
  config_dir <- file.path(withr::local_tempdir(), "config")
  withr::local_envvar(REDIVIS_CONFIG_DIR = config_dir)

  write_cached_credentials()

  credentials_file <- file.path(
    config_dir, "environments", get_environment_host(), "r_credentials"
  )
  expect_equal(get_credentials_file(), credentials_file)
  expect_true(file.exists(credentials_file))
})

test_that("a failed save raises an error, and leaves no temporary file behind", {
  local_mock_api()
  # A non-empty directory in the way, so the file can't be moved into place
  dir.create(file.path(get_credentials_file(), "occupied"), recursive = TRUE)

  expect_error(
    suppressWarnings(write_cached_credentials()),
    class = "redivis_error"
  )
  expect_equal(
    list.files(get_credentials_dir(), all.files = TRUE, no.. = TRUE),
    "r_credentials"
  )
})

test_that("saving credentials restricts an existing file, and restores the umask", {
  # Windows files don't have Unix permissions
  skip_on_os("windows")
  local_mock_api()
  dir.create(get_credentials_dir(), recursive = TRUE, showWarnings = FALSE)
  writeLines("{}", get_credentials_file())
  Sys.chmod(get_credentials_file(), mode = "0644")
  umask <- Sys.umask()

  write_cached_credentials()

  expect_equal(format(file.info(get_credentials_file())$mode), "600")
  expect_equal(Sys.umask(), umask)
})

test_that("the PKCE verifier doesn't depend on the RNG seed", {
  withr::local_seed(1)
  first <- get_pkce()$verifier
  withr::local_seed(1)
  second <- get_pkce()$verifier

  expect_false(identical(first, second))
  expect_match(first, "^[A-Za-z0-9_-]{43,128}$")
})

test_that("the credentials' scope is read from their token", {
  local_mock_api()
  auth_vars$cached_credentials$access_token <- jwt_with_scope(
    "data.edit workflow.write data.data"
  )

  expect_equal(
    get_current_credential_scope(),
    c("data.edit", "workflow.write", "data.data")
  )
})

test_that("a token about to expire is refreshed before it's used", {
  local_mock_api()
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time()) + 60
  auth_vars$cached_credentials$access_token <- "about-to-expire"

  token <- get_auth_token()

  expect_false(identical(token, "about-to-expire"))
  expect_true(auth_vars$cached_credentials$expires_at > as.numeric(Sys.time()) + 600)
})

test_that("a token that isn't about to expire is used as is", {
  local_mock_api()

  expect_equal(get_auth_token(), auth_vars$cached_credentials$access_token)
})

test_that("has_credentials() finds each source of credentials", {
  local_mock_api(authenticated = FALSE)
  expect_false(has_credentials())

  dir.create(get_credentials_dir(), recursive = TRUE)
  writeLines(jsonlite::toJSON(fake_credentials(), auto_unbox = TRUE), get_credentials_file())
  expect_true(has_credentials())
  expect_equal(auth_vars$cached_credentials$refresh_token, "refresh-token")

  auth_vars$cached_credentials <- NULL
  unlink(get_credentials_file())
  withr::local_envvar(REDIVIS_API_TOKEN = "token")
  expect_true(has_credentials())
})

local_token_response <- function(response, env = parent.frame()) {
  local_mocked_bindings(
    req_perform = function(req, ...) response,
    .package = "httr2",
    .env = env
  )
}

test_that("a server error while refreshing keeps the credentials", {
  local_mock_api()
  write_cached_credentials()
  local_token_response(httr2::response(
    status_code = 502,
    headers = list(`Content-Type` = "text/html"),
    body = charToRaw("<html>Bad gateway</html>")
  ))

  expect_error(refresh_credentials(), class = "redivis_api_error")

  expect_equal(auth_vars$cached_credentials$refresh_token, "refresh-token")
  expect_true(file.exists(get_credentials_file()))
})

test_that("a rotated refresh token replaces the one that was used", {
  local_mock_api()
  sent <- NULL
  local_mocked_bindings(
    req_perform = function(req, ...) {
      # Form fields are kept as-is (I()) until the request is sent
      sent <<- unclass(req$body$data$refresh_token)
      httr2::response(
        status_code = 200,
        headers = list(`Content-Type` = "application/json"),
        body = charToRaw(jsonlite::toJSON(
          list(
            access_token = fake_credentials()$access_token,
            refresh_token = "refresh-2",
            expires_at = as.numeric(Sys.time()) + 3600,
            expires_in = 3600
          ),
          auto_unbox = TRUE,
          digits = NA
        ))
      )
    },
    .package = "httr2"
  )
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time())

  get_auth_token()

  expect_equal(sent, "refresh-token")
  saved <- jsonlite::fromJSON(readLines(get_credentials_file(), warn = FALSE))
  expect_equal(saved$refresh_token, "refresh-2")
})

test_that("credentials another process refreshed are used", {
  # Rather than refreshing with the refresh token they replaced, which the server
  # takes as a sign that it was stolen
  local_mock_api()
  # e.g. a second R session, sharing the credentials file
  theirs <- fake_credentials()
  theirs$access_token <- paste0(theirs$access_token, "-theirs")
  theirs$refresh_token <- "refresh-2"
  dir.create(get_credentials_dir(), recursive = TRUE, showWarnings = FALSE)
  writeLines(jsonlite::toJSON(theirs, auto_unbox = TRUE, digits = NA), get_credentials_file())
  local_mocked_bindings(
    req_perform = function(req, ...) stop("refreshed with a replaced refresh token"),
    .package = "httr2"
  )
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time())

  expect_equal(get_auth_token(), theirs$access_token)
  expect_equal(auth_vars$cached_credentials$refresh_token, "refresh-2")
})

test_that("a rejected refresh token prompts a login", {
  local_mock_api()
  write_cached_credentials()
  local_token_response(httr2::response(
    status_code = 400,
    headers = list(`Content-Type` = "application/json"),
    body = charToRaw('{"error":"invalid_grant"}')
  ))
  logins <- 0
  local_mocked_bindings(perform_oauth_login = function(...) {
    logins <<- logins + 1
    auth_vars$cached_credentials <- fake_credentials()
  })

  expect_equal(refresh_credentials(), fake_credentials()$access_token)

  expect_equal(logins, 1)
  expect_false(file.exists(get_credentials_file()))
})

test_that("a rejected refresh token falls back to an anonymous request", {
  # Rather than prompting a login before the request is even sent, since the
  # resource may be public
  mock <- local_mock_api()
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time()) + 60
  write_cached_credentials()
  real_req_perform <- httr2::req_perform
  local_mocked_bindings(
    req_perform = function(req, ...) {
      if (!endsWith(req$url, "/oauth/token")) {
        return(real_req_perform(req, ...))
      }
      httr2::response(
        status_code = 400,
        headers = list(`Content-Type` = "application/json"),
        body = charToRaw('{"error":"invalid_grant"}')
      )
    },
    .package = "httr2"
  )

  expect_equal(make_request(path = TABLE)$name, "mock")

  expect_equal(mock$authorized(), FALSE)
  expect_false(file.exists(get_credentials_file()))
})

test_that("a token without the default scope is sent as is", {
  # Rather than prompting a login for the default scope first; the server says
  # if the resource needs more
  mock <- local_mock_api()
  auth_vars$cached_credentials$access_token <- jwt_with_scope("data.data")

  make_request(path = TABLE)

  expect_equal(mock$authorized(), TRUE)
})

test_that("each environment has its own credentials", {
  local_mock_api()
  withr::defer(redivis$config(api_endpoint = NULL))
  # Those for redivis.com are kept directly in the config directory
  environment_dirs <- c(
    "https://redivis.com/api/v1" = "",
    "https://REDIVIS.com:443/api/v1" = "",
    "http://localhost:8080/api/v1" = "environments/localhost_8080",
    "https://local.host:8443/api/v1" = "environments/local.host_8443"
  )
  for (api_endpoint in names(environment_dirs)) {
    redivis$config(api_endpoint = api_endpoint)
    environment_dir <- environment_dirs[[api_endpoint]]
    expect_equal(
      get_credentials_file(),
      if (nzchar(environment_dir)) {
        file.path(get_redivis_dir(), environment_dir, "r_credentials")
      } else {
        file.path(get_redivis_dir(), "r_credentials")
      }
    )
  }
})
