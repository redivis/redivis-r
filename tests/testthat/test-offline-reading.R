# Read streams, column matching, and file listings.

test_that("read streams reject variables and max_results", {
  stream <- ReadStream$new(id = "s0", table = Table$new("a.b.mock"))
  expect_error(
    stream$to_tibble(variables = "id"),
    "to_read_streams",
    class = "redivis_error"
  )
  expect_error(
    stream$to_tibble(max_results = 10),
    "max_results",
    class = "redivis_error"
  )
})

test_that("streamed columns are matched to variables regardless of case", {
  schema <- arrow::schema(id = arrow::int64(), name = arrow::utf8())
  batch <- arrow::record_batch(
    NAME = c("a", "b"),
    ID = arrow::Array$create(c(1, 2), type = arrow::int64())
  )

  for (coerce_schema in c(FALSE, TRUE)) {
    conform <- make_batch_conformer(batch$schema, schema, coerce_schema)
    conformed <- conform(batch)
    expect_equal(names(conformed), c("id", "name"))
    expect_equal(as.numeric(conformed$id$as_vector()), c(1, 2))
    expect_equal(conformed$name$as_vector(), c("a", "b"))
  }
})

test_that("geography variables are matched regardless of case", {
  local_mock_api()
  params <- get_table_request_params(mock_table(), geography_variable = "NAME")
  expect_equal(params$geography_variable, "name")

  params <- get_table_request_params(mock_table(), geography_variable = "nope")
  expect_null(params$geography_variable)
})

test_that("file listings are cached by the variables they were made with", {
  requests <- new.env()
  requests$log <- list()
  local_mocked_bindings(
    make_request = function(method = "GET", headers = list(), query = NULL, ...) {
      requests$log[[length(requests$log) + 1]] <- list(
        headers = headers,
        query = query
      )
      if (!is.null(headers[["If-Modified-Since"]])) {
        return(httr2::response(status_code = 304))
      }
      listing <- data.frame(
        file_id = "f1",
        file_name = "default.txt",
        alt_name = "alt.txt",
        size = 10
      )
      httr2::response(
        status_code = 200,
        body = arrow::write_to_raw(listing, format = "stream")
      )
    }
  )
  # A table no other test lists, since listings are cached globally
  reference <- paste0("owner.ds.", uuid::UUIDgenerate())
  file_names <- function(files) vapply(files, function(f) f$name, character(1))

  table <- Table$new(reference)
  expect_equal(file_names(table$list_files()), "default.txt")
  expect_equal(
    file_names(table$list_files(file_name_variable = "alt_name")),
    "alt.txt"
  )
  expect_null(requests$log[[2]]$headers[["If-Modified-Since"]])
  expect_equal(requests$log[[2]]$query$fileNameVariable, "alt_name")

  # Files are looked up with the variables of the last listing, which is cached
  expect_equal(table$file("alt.txt")$id, "f1")
  expect_equal(Table$new(reference)$file("default.txt")$id, "f1")
  expect_length(requests$log, 2)
})
