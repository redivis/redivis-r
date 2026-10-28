# Downloads: partial files, resumes, hash verification, and ranged reads. Runs
# against the mock API in helper-mock-api.R.

FILE_SIZE <- 5000

# The bytes the mock API serves for a file (see raw_file_contents in
# helper-mock-api.R)
raw_file_bytes <- function(content, size = FILE_SIZE) {
  withr::with_seed(
    sum(utf8ToInt(content)),
    as.raw(sample.int(256, size, replace = TRUE) - 1L)
  )
}

md5_base64 <- function(bytes) {
  jsonlite::base64_enc(as.vector(openssl::md5(bytes)))
}

local_raw_file <- function(mock, content = "contents", size = FILE_SIZE) {
  mock$set(raw_files = list(f1 = list(content = content, size = size)))
  raw_file_bytes(content, size)
}

read_bytes <- function(path) {
  readBin(path, "raw", file.size(path))
}

# --- Single downloads ----------------------------------------------------------

test_that("a download is verified against the file's hash", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  path <- file.path(withr::local_tempdir(), "f")

  perform_retryable_download(
    "/rawFiles/f1",
    download_path = path,
    size = FILE_SIZE,
    md5_hash = md5_base64(expected)
  )

  expect_equal(read_bytes(path), expected)
  expect_false(file.exists(get_partial_download_path(path)))
})

test_that("a download that doesn't match its hash is retried once, then fails without leaving a file", {
  mock <- local_mock_api()
  local_raw_file(mock)
  path <- file.path(withr::local_tempdir(), "f")

  expect_error(
    perform_retryable_download(
      "/rawFiles/f1",
      download_path = path,
      md5_hash = md5_base64(as.raw(1:3))
    ),
    "MD5 hash",
    class = "redivis_error"
  )
  expect_length(mock$paths("rawFiles"), 2)
  expect_false(file.exists(path))
  expect_false(file.exists(get_partial_download_path(path)))
})

test_that("a resume the server doesn't honor starts the download over", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  mock$set(ignore_raw_file_range = TRUE)
  path <- file.path(withr::local_tempdir(), "f")
  writeBin(as.raw(rep(0, 100)), get_partial_download_path(path))

  perform_retryable_download(
    "/rawFiles/f1",
    download_path = path,
    start_byte = 100,
    retry_count = 1
  )

  expect_length(mock$paths("rawFiles"), 1)
  expect_equal(read_bytes(path), expected)
})

test_that("a download that ends early resumes from its partial file", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  mock$set(truncate_raw_file_once = 1000)
  path <- file.path(withr::local_tempdir(), "f")
  writeBin(expected[1:100], get_partial_download_path(path))

  perform_retryable_download(
    "/rawFiles/f1",
    download_path = path,
    start_byte = 100,
    retry_count = 1,
    md5_hash = md5_base64(expected)
  )

  expect_equal(mock$raw_file_ranges(), c("bytes=100-", "bytes=1100-"))
  expect_equal(read_bytes(path), expected)
  expect_false(file.exists(get_partial_download_path(path)))
})

test_that("a failed download leaves no partial file", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  # The first response ends early, and its resume is rejected
  mock$set(
    truncate_raw_file_once = 1000,
    response_script = list(
      list(status = NULL),
      list(status = 403, body = list(status = 403, error = "forbidden"))
    )
  )
  path <- file.path(withr::local_tempdir(), "f")
  writeBin(expected[1:100], get_partial_download_path(path))

  expect_error(
    perform_retryable_download(
      "/rawFiles/f1",
      download_path = path,
      start_byte = 100,
      retry_count = 1
    ),
    class = "redivis_error"
  )
  expect_length(mock$paths("rawFiles"), 2)
  expect_false(file.exists(path))
  expect_false(file.exists(get_partial_download_path(path)))
})

# --- Parallel downloads --------------------------------------------------------

local_two_raw_files <- function(mock) {
  mock$set(
    raw_files = list(
      f1 = list(content = "one", size = 3000),
      f2 = list(content = "two", size = 4000)
    )
  )
  list(raw_file_bytes("one", 3000), raw_file_bytes("two", 4000))
}

test_that("parallel downloads verify their hashes, and leave no partial files", {
  mock <- local_mock_api()
  expected <- local_two_raw_files(mock)
  dir <- withr::local_tempdir()
  paths <- file.path(dir, c("one", "two"))

  perform_parallel_download(
    uris = list("/rawFiles/f1", "/rawFiles/f2"),
    download_paths = paths,
    sizes = list(3000, 4000),
    md5_hashes = lapply(expected, md5_base64),
    max_parallelization = 1
  )

  expect_equal(read_bytes(paths[[1]]), expected[[1]])
  expect_equal(read_bytes(paths[[2]]), expected[[2]])
  expect_setequal(list.files(dir), c("one", "two"))
})

test_that("a parallel download that doesn't match its hash fails after one retry, leaving no files", {
  mock <- local_mock_api()
  local_two_raw_files(mock)
  dir <- withr::local_tempdir()

  expect_error(
    perform_parallel_download(
      uris = list("/rawFiles/f1"),
      download_paths = file.path(dir, "one"),
      md5_hashes = list(md5_base64(as.raw(1:3))),
      max_parallelization = 1
    ),
    "MD5 hash",
    class = "redivis_error"
  )
  expect_length(mock$paths("rawFiles"), 2)
  expect_length(list.files(dir, all.files = TRUE, no.. = TRUE), 0)
})

test_that("parallel downloads of public files don't require logging in", {
  mock <- local_mock_api(authenticated = FALSE)
  expected <- local_two_raw_files(mock)
  dir <- withr::local_tempdir()

  perform_parallel_download(
    uris = list("/rawFiles/f1", "/rawFiles/f2"),
    download_paths = file.path(dir, c("one", "two")),
    max_parallelization = 1
  )

  expect_equal(read_bytes(file.path(dir, "one")), expected[[1]])
  expect_false(any(mock$authorized()))
})

test_that("parallel downloads rejected together only log in once", {
  mock <- local_mock_api(authenticated = FALSE)
  expected <- local_two_raw_files(mock)
  mock$set(require_auth = TRUE)
  logins <- 0
  local_mocked_bindings(
    refresh_credentials = function(scope = NULL, amr_values = NULL) {
      logins <<- logins + 1
      auth_vars$cached_credentials <- fake_credentials()
      invisible(NULL)
    }
  )
  dir <- withr::local_tempdir()

  perform_parallel_download(
    uris = list("/rawFiles/f1", "/rawFiles/f2"),
    download_paths = file.path(dir, c("one", "two")),
    max_parallelization = 1
  )

  expect_equal(logins, 1)
  expect_equal(read_bytes(file.path(dir, "two")), expected[[2]])
})

# --- Ranged reads --------------------------------------------------------------

test_that("file$read() reads byte ranges, including ones starting at 0", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  file <- File$new(id = "f1", name = "f", properties = list(size = FILE_SIZE))

  expect_equal(file$read(start_byte = 0, end_byte = 0), expected[1])
  expect_equal(file$read(end_byte = 9), expected[1:10])
  expect_equal(mock$raw_file_ranges(), c("bytes=0-0", "bytes=0-9"))
})

test_that("file$read() returns the range when the server ignores it", {
  mock <- local_mock_api()
  expected <- local_raw_file(mock)
  mock$set(ignore_raw_file_range = TRUE)
  file <- File$new(id = "f1", name = "f", properties = list(size = FILE_SIZE))

  expect_equal(file$read(start_byte = 10, end_byte = 19), expected[11:20])
  expect_equal(file$read(start_byte = 4990), expected[4991:5000])
  expect_equal(file$read(end_byte = 0), expected[1])
})

# --- Request retries -----------------------------------------------------------

test_that("GET requests are retried after connection failures, but POSTs aren't", {
  local_mock_api()
  # Nothing listens on port 1, so connections are refused
  withr::local_envvar(REDIVIS_API_ENDPOINT = "http://127.0.0.1:1/api/v1")
  retries <- 0
  local_mocked_bindings(retry_sleep = function(seconds) {
    retries <<- retries + 1
    invisible(NULL)
  })

  expect_error(make_request(path = "/tables/a.b.mock"), class = "httr2_failure")
  expect_equal(retries, 10)

  retries <- 0
  expect_error(
    make_request(method = "POST", path = "/tables/a.b.mock", payload = list()),
    class = "httr2_failure"
  )
  expect_equal(retries, 0)
})
