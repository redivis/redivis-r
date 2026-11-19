# Uploads: the temp uploads that notebooks' output tables still use, and the
# imports that Upload$create() and Table$add_files() go through. Runs against
# the mock API in helper-mock-api.R.

# The mock API's stand-in for a temp upload's signed URL
storage_url <- function() {
  paste0(sub("/api/v1$", "", Sys.getenv("REDIVIS_API_ENDPOINT")), "/storage/upload")
}

test_that("temp uploads don't send the user's credentials", {
  mock <- local_mock_api()

  perform_standard_upload(as.raw(1:10), temp_upload_url = storage_url())
  perform_resumable_upload(as.raw(1:10), temp_upload_url = storage_url())

  storage_requests <- grepl("^/storage/", mock$paths())
  expect_equal(sum(storage_requests), 3)
  expect_false(any(mock$authorized()[storage_requests]))
})

test_that("temp uploads don't require logging in", {
  mock <- local_mock_api(authenticated = FALSE)

  perform_standard_upload(as.raw(1:10), temp_upload_url = storage_url())

  expect_equal(mock$paths(), "/storage/upload")
})

test_that("a resumable upload whose session expired starts a new one", {
  mock <- local_mock_api()
  mock$set(expire_first_storage_session = TRUE)
  progress <- c()

  expect_output(
    perform_resumable_upload(
      as.raw(1:100),
      temp_upload_url = storage_url(),
      on_progress = function(bytes) progress <<- c(progress, bytes)
    ),
    "Retrying"
  )

  expect_equal(
    mock$paths(),
    c(
      "/storage/upload",
      "/storage/session-1",
      "/storage/session-1",
      "/storage/upload",
      "/storage/session-2"
    )
  )
  expect_equal(tail(progress, 1), 100)
})

# --- Upload$create() --------------------------------------------------------

# The requests made to the mock API for imports (rather than reads of uploads)
import_requests <- function(mock) {
  Filter(function(r) grepl("/imports", r$path, fixed = TRUE), mock$requests())
}

request_summary <- function(requests) {
  vapply(
    requests,
    function(r) paste(r$method, sub("^/api/v1", "", r$path)),
    character(1)
  )
}

UUID_PATTERN <- "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"

test_that("an upload's name is checked for conflicts before anything is uploaded", {
  mock <- local_mock_api()
  upload <- mock_table()$upload("data.csv")
  content <- as.raw(rep(1, 2^21))

  expect_error(
    upload$create(content = content, type = "delimited"),
    "already exists",
    class = "redivis_error"
  )
  expect_identical(
    mock_table()$upload("data.csv")$create(
      content = content,
      type = "delimited",
      if_not_exists = TRUE
    )$name,
    "data.csv"
  )
  expect_length(mock$paths("imports"), 0)
})

test_that("content is uploaded through an import, which is then waited on", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE, import_statuses = list("running", "running"))

  upload <- mock_table()$upload("data.csv")$create(
    content = charToRaw("a,b\n1,2"),
    type = "delimited",
    progress = FALSE
  )

  requests <- import_requests(mock)
  expect_equal(
    request_summary(requests),
    c(
      "POST /tables/a.b.mock/imports",
      paste0("POST /imports/i1/files/", encode_import_file_name("data.csv")),
      "POST /imports/i1/finalize",
      "GET /imports/i1",
      "GET /imports/i1",
      "GET /imports/i1"
    )
  )
  payload <- requests[[1]]$payload
  expect_match(payload$idempotencyKey, UUID_PATTERN)
  payload$idempotencyKey <- NULL
  # Unset options are left out, rather than sent as null
  expect_equal(
    payload,
    list(
      name = "data.csv",
      sourceType = "local",
      totalFileCount = 1,
      totalSize = 7,
      type = "delimited",
      hasHeaderRow = TRUE,
      skipBadRecords = FALSE,
      allowJaggedRows = FALSE
    )
  )
  expect_equal(
    requests[[2]]$content_md5,
    openssl::base64_encode(openssl::md5(charToRaw("a,b\n1,2")))
  )
  expect_true(all(vapply(requests, function(r) r$authorized, logical(1))))
  expect_equal(mock$imports()$files[["i1/data.csv"]]$size, 7)
  # The upload's properties come from the upload, once it's finished
  expect_equal(upload$properties$status, "completed")
  expect_equal(upload$name, "data.csv")
})

test_that("an upload takes the name the API gives its import", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE, import_name = "data (1).csv")

  upload <- mock_table()$upload("data.csv")$create(
    content = charToRaw("a,b\n1,2"),
    rename_on_conflict = TRUE,
    wait_for_finish = FALSE,
    progress = FALSE
  )

  expect_equal(upload$name, "data (1).csv")
  expect_equal(upload$uri, upload_uri(mock_table(), "data (1).csv"))
  expect_equal(upload$properties$kind, "import")
  expect_false("GET /imports/i1" %in% request_summary(import_requests(mock)))
})

test_that("a file is uploaded under its own name", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE)
  path <- file.path(withr::local_tempdir(), "my data.csv")
  writeBin(charToRaw("a,b\n1,2"), path)

  upload <- mock_table()$upload()$create(path, progress = FALSE)

  expect_equal(upload$name, "my data.csv")
  expect_equal(import_requests(mock)[[1]]$payload$name, "my data.csv")
  expect_named(mock$imports()$files, "i1/my data.csv")
})

test_that("a data frame is uploaded as parquet", {
  mock <- local_mock_api()

  mock_table()$upload()$create(data.frame(a = 1:3), progress = FALSE)

  payload <- import_requests(mock)[[1]]$payload
  expect_equal(payload$type, "parquet")
  expect_null(payload$name)
  expect_named(mock$imports()$files, "i1/imported")
})

test_that("creating an import is retried with the same idempotency key", {
  mock <- local_mock_api()
  mock$set(fail_503 = 1)

  mock_table()$upload()$create(
    content = charToRaw("a,b\n1,2"),
    type = "delimited",
    progress = FALSE
  )

  creates <- Filter(
    function(r) r$path == "/api/v1/tables/a.b.mock/imports",
    mock$requests()
  )
  expect_length(creates, 2)
  expect_equal(creates[[1]]$payload$idempotencyKey, creates[[2]]$payload$idempotencyKey)
  expect_length(mock$imports()$imports, 1)
})

test_that("a file upload is sent again after a failure", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(
      list(status = NULL),
      list(status = 500, body = list(status = 500, error = "internal_error"))
    )
  )

  mock_table()$upload()$create(
    content = charToRaw("a,b\n1,2"),
    type = "delimited",
    progress = FALSE
  )

  expect_length(mock$paths("/imports/i1/files/"), 2)
  expect_equal(mock$imports()$files[["i1/imported"]]$size, 7)
})

test_that("a transfer creates an import from its source, with its identity", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE)

  mock_table()$upload("t")$create(
    transfer_specification = list(
      sourceType = "gcs",
      sourcePath = "gs://bucket/data.csv",
      identity = "me@example.com"
    ),
    progress = FALSE
  )

  requests <- import_requests(mock)
  expect_equal(
    request_summary(requests),
    c("POST /tables/a.b.mock/imports", "GET /imports/i1")
  )
  payload <- requests[[1]]$payload
  expect_equal(payload$sourceType, "gcs")
  expect_equal(payload$sourcePaths, list("gs://bucket/data.csv"))
  expect_equal(payload$identity, "me@example.com")
  expect_null(payload$totalFileCount)

  mock_table()$upload("u")$create(
    transfer_specification = list(sourceType = "redivis", sourcePath = "a.b.c"),
    progress = FALSE
  )
  expect_false("identity" %in% names(import_requests(mock)[[3]]$payload))
})

test_that("remove_on_fail removes a failed upload, with or without raise_on_fail", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE, import_statuses = list("failed"))
  upload <- mock_table()$upload("u")$create(
    content = charToRaw("a,b\n1,2"),
    type = "delimited",
    raise_on_fail = FALSE,
    remove_on_fail = TRUE,
    progress = FALSE
  )
  expect_equal(upload$properties$status, "failed")
  expect_true("DELETE /tables/a.b.mock/uploads/u" %in% request_summary(mock$requests()))

  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE, import_statuses = list("failed"))
  expect_error(
    mock_table()$upload("u")$create(
      content = charToRaw("a,b\n1,2"),
      type = "delimited",
      remove_on_fail = TRUE,
      progress = FALSE
    ),
    "Bad data",
    class = "redivis_job_error"
  )
  expect_true("DELETE /tables/a.b.mock/uploads/u" %in% request_summary(mock$requests()))
})

test_that("a stream upload is created directly, and warns of no other option", {
  mock <- local_mock_api()
  mock$set(uploads_exist = FALSE)

  mock_table()$upload("s")$create(
    type = "stream",
    schema = list(list(name = "id", type = "integer")),
    progress = FALSE
  )
  expect_equal(mock$paths("/uploads"), c(
    "/api/v1/tables/a.b.mock/uploads/s",
    "/api/v1/tables/a.b.mock/uploads"
  ))
  expect_length(mock$paths("imports"), 0)

  # Schemas only apply to streams; the type may be left to be inferred
  expect_warning(
    mock_table()$upload("v.csv")$create(
      content = charToRaw("a\n1"),
      schema = list(list(name = "a", type = "integer")),
      progress = FALSE
    ),
    "schema option is ignored"
  )
})

# --- Uploading a file to an import -------------------------------------------

test_that("an import's file names are unpadded base64url", {
  expect_equal(encode_import_file_name("données/é?.csv"), "ZG9ubsOpZXMvw6k_LmNzdg")
  expect_equal(encode_import_file_name("a>b?c~"), "YT5iP2N-")
})

# Lowers the size at which files are sent in chunks, and the chunks' size
local_small_chunks <- function(chunk_bytes = 2^18, env = parent.frame()) {
  local_mocked_bindings(
    IMPORT_DIRECT_UPLOAD_MAX_BYTES = 1000,
    IMPORT_CHUNK_START_BYTES = chunk_bytes,
    IMPORT_CHUNK_MIN_BYTES = 2^18,
    IMPORT_CHUNK_MAX_BYTES = chunk_bytes,
    .env = env
  )
}

random_bytes <- function(n) as.raw(sample.int(256, n, replace = TRUE) - 1L)

session_requests <- function(mock) {
  Filter(function(r) endsWith(r$path, "/uploadSession"), mock$requests())
}

test_that("a large file is uploaded in chunks, without the user's credentials", {
  mock <- local_mock_api()
  local_small_chunks()
  content <- random_bytes(2 * 2^18 + 1000)
  progress <- c()

  upload <- mock_table()$upload()$create(
    content = content,
    type = "delimited",
    progress = FALSE
  )
  import <- list(uri = "/imports/i1")
  upload_import_file(
    import,
    "again.bin",
    content,
    on_progress = function(bytes) progress <<- c(progress, bytes)
  )

  requests <- session_requests(mock)
  puts <- Filter(function(r) r$method == "PUT", requests)
  expect_equal(
    vapply(puts, function(r) r$content_range, character(1))[1:4],
    c(
      "bytes */525288",
      "bytes 0-262143/525288",
      "bytes 262144-524287/525288",
      "bytes 524288-525287/525288"
    )
  )
  expect_false(any(vapply(puts, function(r) r$authorized, logical(1))))
  expect_true(all(vapply(
    Filter(function(r) r$method == "POST", requests),
    function(r) r$authorized,
    logical(1)
  )))
  imports <- mock$imports()
  expect_equal(imports$files[["i1/imported"]]$size, length(content))
  expect_equal(imports$files[["i1/again.bin"]]$size, length(content))
  expect_length(imports$chunk_violations, 0)
  expect_equal(tail(progress, 1), length(content))
  expect_false(is.unsorted(progress))
})

test_that("a chunked upload resumes from what the session committed", {
  mock <- local_mock_api()
  local_small_chunks(chunk_bytes = 2^19)
  import <- create_import(mock_table(), list(totalFileCount = 1))
  content <- random_bytes(2^20)
  mock$set(partial_commit_once = 2^18)

  upload_import_file(import, "f.bin", content)

  puts <- Filter(function(r) r$method == "PUT", session_requests(mock))
  expect_equal(
    vapply(puts, function(r) r$content_range, character(1)),
    c(
      "bytes */1048576",
      "bytes 0-524287/1048576",
      "bytes 262144-786431/1048576",
      "bytes 786432-1048575/1048576"
    )
  )
  expect_equal(mock$imports()$files[["i1/f.bin"]]$size, 2^20)
  expect_length(mock$imports()$chunk_violations, 0)
})

test_that("a failed chunk is retried from what the session committed", {
  mock <- local_mock_api()
  local_small_chunks(chunk_bytes = 2^19)
  import <- create_import(mock_table(), list(totalFileCount = 1))
  content <- random_bytes(2^20)
  mock$set(fail_chunk_once = TRUE)

  upload_import_file(import, "f.bin", content)

  puts <- Filter(function(r) r$method == "PUT", session_requests(mock))
  expect_equal(
    vapply(puts, function(r) r$content_range, character(1)),
    c(
      "bytes */1048576",
      "bytes 0-524287/1048576",
      # The failure halves the chunk size, which grows again as chunks succeed
      "bytes */1048576",
      "bytes 0-262143/1048576",
      "bytes 262144-786431/1048576",
      "bytes 786432-1048575/1048576"
    )
  )
  expect_equal(mock$imports()$files[["i1/f.bin"]]$size, 2^20)
})

test_that("a dead upload session is replaced", {
  mock <- local_mock_api()
  local_small_chunks()
  import <- create_import(mock_table(), list(totalFileCount = 1))
  content <- random_bytes(2^19)
  mock$set(kill_session_once = TRUE)

  upload_import_file(import, "f.bin", content)

  requests <- session_requests(mock)
  expect_equal(
    vapply(requests, function(r) r$method, character(1)),
    c("POST", "PUT", "POST", "PUT", "PUT", "PUT")
  )
  expect_equal(
    unique(vapply(requests[4:6], function(r) r$query$upload_id, character(1))),
    "s2"
  )
  expect_equal(mock$imports()$files[["i1/f.bin"]]$size, 2^19)
})

test_that("an upload fails when its dead session is all the import offers", {
  mock <- local_mock_api()
  local_small_chunks()
  import <- create_import(mock_table(), list(totalFileCount = 1))
  mock$set(kill_session_once = TRUE, reopen_dead_sessions = TRUE)

  expect_error(
    upload_import_file(import, "f.bin", random_bytes(2^19)),
    "upload session for this file has ended",
    class = "redivis_error"
  )
  expect_length(mock$imports()$files, 0)
})

test_that("an upload gives up after its sessions keep dying", {
  mock <- local_mock_api()
  local_small_chunks()
  import <- create_import(mock_table(), list(totalFileCount = 1))
  content <- random_bytes(2^19)

  # Each session fails its md5 check on completion, and the API replaces it
  expect_error(
    upload_import_file(
      import,
      "f.bin",
      content,
      md5 = openssl::base64_encode(as.raw(openssl::md5(raw(1))))
    ),
    "failed on 4 upload sessions",
    class = "redivis_error"
  )
  requests <- session_requests(mock)
  expect_equal(
    sum(vapply(requests, function(r) r$method == "POST", logical(1))),
    IMPORT_MAX_SESSION_REPLACEMENTS + 1 + 1
  )
  expect_length(mock$imports()$files, 0)
})

test_that("a failed import raises a job error", {
  mock <- local_mock_api()
  mock$set(import_statuses = list("failed"))
  import <- create_import(mock_table(), list(totalFileCount = 0))

  expect_error(
    wait_for_import(finalize_import(import)),
    "Bad data",
    class = "redivis_job_error"
  )
  mock$set(import_statuses = list("cancelled"))
  import <- create_import(mock_table(), list(totalFileCount = 0))
  expect_equal(
    wait_for_import(finalize_import(import), raise_on_fail = FALSE)$status,
    "cancelled"
  )
})

# --- Table$add_files() -------------------------------------------------------

test_that("files are added by a single import", {
  mock <- local_mock_api()
  path <- file.path(withr::local_tempdir(), "local.txt")
  writeBin(charToRaw("from disk"), path)
  table <- mock_table()

  result <- withVisible(table$add_files(
    files = list(
      list(name = "a.txt", data = "text content"),
      list(name = "dir/b.bin", data = as.raw(0:2)),
      path
    ),
    progress = FALSE,
    max_parallelization = 1
  ))

  expect_identical(result$value, table)
  expect_false(result$visible)
  summary <- request_summary(import_requests(mock))
  expect_equal(sum(summary == "POST /tables/a.b.mock/imports"), 1)
  expect_equal(sum(summary == "POST /imports/i1/finalize"), 1)
  expect_equal(sum(startsWith(summary, "POST /imports/i1/files/")), 3)
  expect_length(mock$paths("tempUploads"), 0)
  expect_length(mock$paths("rawFiles"), 0)
  payload <- import_requests(mock)[[1]]$payload
  payload$idempotencyKey <- NULL
  expect_equal(
    payload,
    list(sourceType = "local", totalFileCount = 3, totalSize = 24)
  )
  expect_setequal(
    names(mock$imports()$files),
    c("i1/a.txt", "i1/dir/b.bin", "i1/local.txt")
  )
})

test_that("a compressed file is hashed as stored, rather than decompressed", {
  mock <- local_mock_api()
  path <- file.path(withr::local_tempdir(), "data.csv.gz")
  con <- gzfile(path, "w")
  writeLines(rep("a,b,c", 100), con)
  close(con)

  # The mock refuses a file whose Content-MD5 doesn't match the bytes sent
  mock_table()$add_files(files = list(path), progress = FALSE, max_parallelization = 1)

  expect_named(mock$imports()$files, "i1/data.csv.gz")
})

test_that("parallel workers upload with this session's token, never refreshing the login", {
  testthat::skip_if_not(parallelly::supportsMulticore(), "needs forked workers")
  mock <- local_mock_api()
  # Every token is within 5 minutes of expiring, so any request made with the
  # saved login refreshes it first
  mock$set(token_lifetime = 60)
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time()) + 60
  files <- lapply(1:4, function(i) {
    list(name = paste0(i, ".txt"), data = paste("file", i))
  })

  mock_table()$add_files(files = files, progress = FALSE, max_parallelization = 2)

  file_requests <- which(grepl("^/api/v1/imports/i1/files/", mock$paths()))
  expect_length(file_requests, 4)
  expect_true(all(mock$authorized()[file_requests]))
  # Refreshes are numbered by the requests made before them. Any between the
  # first and last file uploads were made by the workers.
  refreshes <- mock$token_refreshes()
  expect_gt(length(refreshes), 0)
  expect_false(any(refreshes >= min(file_requests) & refreshes < max(file_requests)))
})

test_that("a directory's files are added under their relative paths", {
  mock <- local_mock_api()
  dir <- withr::local_tempdir()
  dir.create(file.path(dir, "nested"))
  writeBin(charToRaw("1"), file.path(dir, "top.txt"))
  writeBin(charToRaw("22"), file.path(dir, "nested", "inner.txt"))

  mock_table()$add_files(directory = dir, progress = FALSE, max_parallelization = 1)

  expect_setequal(
    names(mock$imports()$files),
    c("i1/top.txt", "i1/nested/inner.txt")
  )
})

test_that("adding files raises when their import fails", {
  mock <- local_mock_api()
  mock$set(import_statuses = list("failed"))

  expect_error(
    mock_table()$add_files(
      files = list(list(name = "a.txt", data = "text")),
      progress = FALSE,
      max_parallelization = 1
    ),
    "Bad data",
    class = "redivis_job_error"
  )
})

test_that("an sf data frame is written as GeoParquet, in WGS84", {
  skip_if_not_installed("sf")
  data <- sf::st_sf(
    id = 1:2,
    geometry = sf::st_sfc(
      sf::st_point(c(-122, 37)),
      sf::st_point(c(0.5, 51.5)),
      crs = 4326
    )
  )
  read_geometries <- function(table) {
    sf::st_as_sfc(structure(as.list(table$geometry$as_vector()), class = "WKB"))
  }

  table <- arrow::read_parquet(
    convert_data_to_parquet(sf::st_transform(data, 3857)),
    as_data_frame = FALSE
  )

  expect_equal(table$schema$names, c("id", "geometry"))
  expect_equal(table$schema$geometry$type$ToString(), "binary")
  expect_equal(
    jsonlite::fromJSON(table$metadata$geo, simplifyVector = FALSE),
    list(
      version = "1.0.0",
      primary_column = "geometry",
      columns = list(geometry = list(encoding = "WKB", geometry_types = list()))
    )
  )
  expect_equal(
    unname(sf::st_coordinates(read_geometries(table))),
    matrix(c(-122, 0.5, 37, 51.5), ncol = 2),
    tolerance = 1e-6
  )
})
