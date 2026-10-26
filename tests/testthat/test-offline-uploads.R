# Temp uploads and Upload$create(). Runs against the mock API in
# helper-mock-api.R, or a stubbed make_request().

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
  expect_length(mock$paths("tempUploads"), 0)
  expect_length(mock$paths("storage"), 0)
})

test_that("an upload's variables are listed without a limit by default", {
  local_mock_api()
  expect_length(mock_table()$upload("data.csv")$list_variables(), 3)
})

# Stub out the API for an upload that fails once created, recording each
# request as "METHOD path"
local_failing_upload <- function(env = parent.frame()) {
  requests <- new.env()
  requests$log <- character()
  local_mocked_bindings(
    make_request = function(method = "GET", path = "", ...) {
      requests$log <- c(requests$log, paste(method, path))
      if (method == "HEAD") {
        abort_redivis_not_found_error(description = "Not found")
      }
      list(
        uri = "/tables/a.b.t/uploads/u",
        status = "failed",
        errorMessage = "Bad data"
      )
    },
    poll_sleep = function(seconds) invisible(NULL),
    .env = env
  )
  requests
}

test_that("remove_on_fail removes a failed upload, with or without raise_on_fail", {
  requests <- local_failing_upload()
  Table$new(name = "a.b.t")$upload("u")$create(
    content = charToRaw("a,b\n1,2"),
    type = "delimited",
    raise_on_fail = FALSE,
    remove_on_fail = TRUE
  )
  expect_true("DELETE /tables/a.b.t/uploads/u" %in% requests$log)

  requests <- local_failing_upload()
  expect_error(
    Table$new(name = "a.b.t")$upload("u")$create(
      content = charToRaw("a,b\n1,2"),
      type = "delimited",
      remove_on_fail = TRUE
    ),
    "Bad data",
    class = "redivis_job_error"
  )
  expect_true("DELETE /tables/a.b.t/uploads/u" %in% requests$log)
})

test_that("raw content is sent as a file named after the upload", {
  sent <- NULL
  local_mocked_bindings(
    make_request = function(method = "GET", path = "", files = NULL, ...) {
      if (method == "HEAD") {
        abort_redivis_not_found_error(description = "Not found")
      }
      if (!is.null(files)) {
        # The file is removed once the upload is created
        sent <<- list(
          name = files$data$name,
          content = readBin(files$data$path, "raw", file.size(files$data$path))
        )
      }
      list(uri = "/tables/a.b.t/uploads/u", status = "completed")
    },
    poll_sleep = function(seconds) invisible(NULL)
  )

  Table$new(name = "a.b.t")$upload("data.csv")$create(
    content = charToRaw("a,b\n1,2"),
    progress = FALSE
  )

  expect_equal(sent, list(name = "data.csv", content = charToRaw("a,b\n1,2")))
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
