# Retry and authentication handling in make_request(). Runs against the mock
# API in helper-mock-api.R.

TABLE <- "/tables/a.b.mock"

UNAUTHENTICATED <- list(
  status = 401,
  body = list(status = 401, error = "invalid_token")
)
INSUFFICIENT_SCOPE <- list(
  status = 403,
  body = list(status = 403, error = "insufficient_scope", scope = "data.data")
)
PASS <- list(status = NULL)

# Stub out re-authentication, recording the scope requested by each attempt
local_logins <- function(env = parent.frame()) {
  logins <- new.env()
  logins$scopes <- list()
  local_mocked_bindings(
    refresh_credentials = function(scope = NULL, amr_values = NULL) {
      logins$scopes[[length(logins$scopes) + 1]] <- scope %||% NA
      auth_vars$cached_credentials <- fake_credentials()
      invisible(NULL)
    },
    .env = env
  )
  logins
}

# --- 503 retries -------------------------------------------------------------

test_that("idempotent requests retry 503s", {
  mock <- local_mock_api()

  for (method in c("GET", "HEAD", "PATCH")) {
    mock$set(fail_503 = 3)
    before <- length(mock$paths())
    make_request(method = method, path = TABLE, parse_response = FALSE)
    expect_equal(length(mock$paths()) - before, 4, label = method)
  }
})

test_that("streamed requests retry 503s", {
  mock <- local_mock_api()
  mock$set(fail_503 = 2)
  received <- raw()

  make_request(
    path = TABLE,
    stream_callback = function(chunk) {
      received <<- c(received, chunk)
      NULL
    }
  )

  expect_length(mock$paths(), 3)
  expect_equal(jsonlite::fromJSON(rawToChar(received))$name, "mock")
})

test_that("persistent 503s eventually raise", {
  mock <- local_mock_api()
  mock$set(fail_503 = 1000)

  expect_error(make_request(path = TABLE), class = "redivis_api_error")
  expect_length(mock$paths(), 11)
})

# --- authentication retries --------------------------------------------------

test_that("a request that succeeds after re-authenticating returns its result", {
  mock <- local_mock_api()
  local_logins()

  mock$set(response_script = list(UNAUTHENTICATED, PASS))
  expect_equal(make_request(path = TABLE)$name, "mock")

  mock$set(response_script = list(UNAUTHENTICATED, PASS))
  con <- make_request(path = TABLE, as_connection = TRUE)
  expect_s3_class(con, "httr2_response")
  close(con)

  mock$set(response_script = list(UNAUTHENTICATED, PASS))
  expect_equal(mock_table()$get()$properties$name, "mock")
})

test_that("a repeated auth failure stops after one attempt", {
  # e.g. a refreshed token the server still rejects
  mock <- local_mock_api()
  logins <- local_logins()
  mock$set(response_script = rep(list(UNAUTHENTICATED), 5))

  expect_error(
    make_request(path = TABLE),
    "invalid_token",
    class = "redivis_api_error"
  )
  expect_length(logins$scopes, 1)
  expect_length(mock$paths(), 2)
})

test_that("an anonymous request can log in, then upgrade its scope", {
  mock <- local_mock_api(authenticated = FALSE)
  logins <- local_logins()
  mock$set(response_script = list(UNAUTHENTICATED, INSUFFICIENT_SCOPE, PASS))

  expect_equal(make_request(path = TABLE)$name, "mock")
  expect_equal(logins$scopes, list(NA, "data.data"))
})

test_that("logging in counts as progress, even with the same error", {
  mock <- local_mock_api(authenticated = FALSE)
  logins <- local_logins()
  mock$set(response_script = list(UNAUTHENTICATED, UNAUTHENTICATED, PASS))

  make_request(path = TABLE)

  expect_length(logins$scopes, 2)
  expect_equal(mock$authorized(), c(FALSE, TRUE, TRUE))
})

test_that("cycling auth failures stop at the first repeat", {
  mock <- local_mock_api()
  logins <- local_logins()
  a <- list(status = 401, body = list(status = 401, error = "a"))
  b <- list(status = 401, body = list(status = 401, error = "b"))
  mock$set(response_script = list(a, b, a, PASS))

  expect_error(make_request(path = TABLE), class = "redivis_api_error")
  expect_length(logins$scopes, 2)
})

test_that("ever-changing auth failures are capped", {
  mock <- local_mock_api()
  logins <- local_logins()
  mock$set(
    response_script = lapply(1:20, function(i) {
      list(status = 401, body = list(status = 401, error = paste0("error-", i)))
    })
  )

  expect_error(make_request(path = TABLE), class = "redivis_api_error")
  expect_length(logins$scopes, MAX_AUTH_ATTEMPTS)
})

test_that("a 403 without an error field is reported as a 403", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 403,
      body = list(error_description = "nope")
    ))
  )

  expect_error(
    make_request(path = TABLE),
    class = "redivis_authorization_error"
  )
})

test_that("requests without credentials are sent anonymously", {
  mock <- local_mock_api(authenticated = FALSE)

  make_request(path = TABLE)

  expect_equal(mock$authorized(), FALSE)
})

# --- downloads ---------------------------------------------------------------

test_that("downloads don't retry errors a retry can't fix", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 404,
      body = list(status = 404, error = "not_found")
    ))
  )

  expect_error(
    perform_retryable_download(
      "/rawFiles/f1",
      download_path = file.path(withr::local_tempdir(), "f")
    ),
    class = "redivis_not_found_error"
  )
  expect_length(mock$paths(), 1)
})

test_that("downloads don't retry server errors", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 500,
      body = list(status = 500, error = "internal_error")
    ))
  )

  expect_error(
    perform_retryable_download(
      "/rawFiles/f1",
      download_path = file.path(withr::local_tempdir(), "f")
    ),
    class = "redivis_api_error"
  )
  expect_length(mock$paths(), 1)
})

# A directory download paths can't be created in
local_unwritable_dir <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  Sys.chmod(dir, mode = "0500")
  withr::defer(Sys.chmod(dir, mode = "0700"), envir = env)
  skip_if(file.access(dir, 2) == 0, "can't make a directory unwritable")
  dir
}

test_that("downloads don't retry local file errors", {
  mock <- local_mock_api()
  mock$set(raw_files = list(f1 = list(content = "contents", size = 8)))

  err <- expect_error(
    perform_retryable_download(
      "/rawFiles/f1",
      download_path = file.path(local_unwritable_dir(), "f")
    ),
    "Failed to write to",
    class = "redivis_error"
  )
  expect_false(inherits(err, "redivis_network_error"))
  expect_length(mock$paths(), 1)
})

parallel_download <- function() {
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  perform_parallel_download(
    uris = list("/exports/e1/download?filePart=0"),
    download_paths = file.path(dir, "part.parquet"),
    max_parallelization = 1
  )
  file.path(dir, "part.parquet")
}

test_that("parallel downloads upgrade to every scope the server asks for", {
  mock <- local_mock_api()
  logins <- local_logins()
  mock$set(
    response_script = list(
      list(
        status = 403,
        body = list(
          status = 403,
          error = "insufficient_scope",
          scope = "data.data other.scope"
        )
      )
    )
  )

  path <- parallel_download()

  expect_equal(logins$scopes, list(c("data.data", "other.scope")))
  expect_equal(nrow(arrow::read_parquet(path)), 500)
})

test_that("parallel downloads stop re-authenticating when a failure repeats", {
  mock <- local_mock_api()
  logins <- local_logins()
  mock$set(response_script = rep(list(UNAUTHENTICATED), 20))
  # A regression would loop forever; fail instead
  setTimeLimit(elapsed = 60, transient = TRUE)
  withr::defer(setTimeLimit(elapsed = Inf))

  expect_error(
    parallel_download(),
    "invalid_token",
    class = "redivis_api_error"
  )
  expect_length(logins$scopes, 1)
  expect_length(mock$paths(), 2)
})

test_that("parallel downloads raise HTTP errors", {
  # An error raised inside a curl callback is only printed, so parallel
  # downloads used to report success with the file missing
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 404,
      body = list(status = 404, error = "not_found")
    ))
  )

  expect_error(parallel_download(), class = "redivis_not_found_error")
})

test_that("parallel downloads stop once a file has failed for good", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 404,
      body = list(status = 404, error = "not_found")
    ))
  )
  dir <- withr::local_tempdir()

  expect_error(
    perform_parallel_download(
      uris = lapply(0:2, function(i) {
        paste0("/exports/e1/download?filePart=", i)
      }),
      download_paths = file.path(dir, paste0(0:2, ".parquet")),
      max_parallelization = 1,
      max_concurrency = 1
    ),
    class = "redivis_not_found_error"
  )
  # The other files were never requested
  expect_length(mock$paths(), 1)
  expect_length(list.files(dir), 0)
})

test_that("parallel downloads don't retry local file errors", {
  mock <- local_mock_api()

  err <- expect_error(
    perform_parallel_download(
      uris = list("/exports/e1/download?filePart=0"),
      download_paths = file.path(local_unwritable_dir(), "part.parquet"),
      max_parallelization = 1
    ),
    "Failed to write to",
    class = "redivis_error"
  )
  expect_false(inherits(err, "redivis_network_error"))
  expect_length(mock$paths(), 1)
})

test_that("parallel download retries use retry_sleep()", {
  mock <- local_mock_api()
  mock$set(fail_503 = 3)

  elapsed <- system.time(path <- parallel_download())[["elapsed"]]

  expect_equal(nrow(arrow::read_parquet(path)), 500)
  # Unmocked, the backoff alone would take over 6 seconds
  expect_lt(elapsed, 3)
})
