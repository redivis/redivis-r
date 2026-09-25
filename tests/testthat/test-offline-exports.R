# Reads and downloads that go through the export API. Runs against the mock
# API in helper-mock-api.R.

export_paths <- function(mock) {
  mock$paths()[endsWith(mock$paths(), "/exports")]
}

test_that("a large table reads via an export", {
  mock <- local_mock_api()
  mock$set(num_bytes = 2e9)

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(export_paths(mock), "/api/v1/tables/a.b.mock/exports")
  expect_length(mock$paths("/readSessions"), 0)
  expect_equal(sort(as.numeric(df$id)), 0:499)
})

test_that("a large query reads via an export", {
  mock <- local_mock_api()
  mock$set(query_bytes = 2e9)

  df <- Query$new("SELECT 1")$to_tibble(progress = FALSE)

  expect_equal(export_paths(mock), "/api/v1/queries/q1/exports")
  expect_equal(sort(as.numeric(df$id)), 0:499)
})

test_that("a large upload reads via an export", {
  mock <- local_mock_api()
  mock$set(num_bytes = 2e9)

  df <- Upload$new(name = "u1", table = mock_table())$to_tibble(progress = FALSE)

  expect_equal(export_paths(mock), "/api/v1/tables/a.b.mock/uploads/u1/exports")
  expect_equal(sort(as.numeric(df$id)), 0:499)
})

test_that("a multi-part export reads every part", {
  mock <- local_mock_api()
  mock$set(num_bytes = 2e9, export_file_count = 3)

  df <- mock_table()$to_tibble(progress = FALSE, max_parallelization = 1)

  expect_equal(sort(as.numeric(df$id)), 0:499)
})

test_that("a single-file table download writes the file's contents", {
  local_mock_api()
  dir <- withr::local_tempdir()

  path <- mock_table()$download(dir, format = "parquet", progress = FALSE)

  expect_equal(basename(path), "mock.parquet")
  expect_equal(nrow(arrow::read_parquet(path)), 500)
})

test_that("a multi-part table download writes each part to a directory", {
  mock <- local_mock_api()
  mock$set(export_file_count = 3)
  dir <- withr::local_tempdir()

  path <- mock_table()$download(
    dir,
    format = "parquet",
    progress = FALSE,
    max_parallelization = 1
  )

  expect_equal(basename(path), "mock")
  expect_equal(sort(list.files(path)), sprintf("%06d.parquet", 0:2))
  expect_equal(nrow(dplyr::collect(arrow::open_dataset(path))), 500)
})

test_that("a multi-part table download raises if a part fails", {
  mock <- local_mock_api()
  mock$set(
    export_file_count = 3,
    response_script = list(
      list(status = NULL), # table metadata
      list(status = NULL), # export
      list(status = 500, body = list(status = 500, error = "internal_error"))
    )
  )

  expect_error(
    mock_table()$download(withr::local_tempdir(), format = "parquet", progress = FALSE),
    class = "redivis_api_error"
  )
})

test_that("a query download is named by when the query finished", {
  local_mock_api()
  dir <- withr::local_tempdir()

  path <- Query$new("SELECT 1")$download(dir, format = "parquet", progress = FALSE)

  expect_match(
    basename(path),
    "^query_\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}_\\d{6}\\.parquet$"
  )
})

test_that("an upload download fetches the upload's properties", {
  local_mock_api()
  dir <- withr::local_tempdir()
  upload <- Upload$new(name = "u1", table = mock_table())

  path <- upload$download(dir, format = "parquet", progress = FALSE)

  expect_equal(basename(path), "my_upload.parquet")
})

test_that("an incomplete upload can't be read", {
  mock <- local_mock_api()
  mock$set(upload_status = "running")

  expect_error(
    Upload$new(name = "u1", table = mock_table())$to_tibble(progress = FALSE),
    "status: running",
    class = "redivis_value_error"
  )
})

test_that("a failed query raises its error", {
  mock <- local_mock_api()
  mock$set(query_status = "failed")

  expect_error(
    Query$new("SELECT 1")$to_tibble(progress = FALSE),
    "Syntax error",
    class = "redivis_job_error"
  )
})

test_that("a read stream can't be downloaded", {
  local_mock_api()
  stream <- mock_table()$to_read_streams(target_count = 1)[[1]]

  expect_error(stream$download(), class = "redivis_value_error")
})

test_that("a query prints before and after it's run", {
  local_mock_api()
  query <- Query$new("SELECT 1")

  expect_output(print(query), "<Query [uninitialized]>", fixed = TRUE)
  query$get()
  expect_output(print(query), "<Query q1>", fixed = TRUE)
})
