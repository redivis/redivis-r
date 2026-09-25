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

test_that("a login saves credentials privately, creating their directory", {
  local_mock_api(authenticated = FALSE)
  local_real_login()
  expect_false(dir.exists(get_redivis_dir()))

  capture.output(perform_oauth_login(scope = NULL))

  saved <- jsonlite::fromJSON(get_credentials_file())
  expect_equal(saved$refresh_token, "refresh-token")
  expect_equal(format(file.info(get_credentials_file())$mode), "600")
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

  dir.create(get_redivis_dir())
  writeLines(jsonlite::toJSON(fake_credentials(), auto_unbox = TRUE), get_credentials_file())
  expect_true(has_credentials())
  expect_equal(auth_vars$cached_credentials$refresh_token, "refresh-token")

  auth_vars$cached_credentials <- NULL
  unlink(get_credentials_file())
  withr::local_envvar(REDIVIS_API_TOKEN = "token")
  expect_true(has_credentials())
})
