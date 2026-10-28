# How table reads are routed (listRows vs. read sessions) and recover from
# interruptions. Runs against the mock API in helper-mock-api.R.

TABLE <- "/api/v1/tables/a.b.mock"
ROWS <- paste0(TABLE, "/rows")
READ_SESSIONS <- paste0(TABLE, "/readSessions")

data_paths <- function(mock) {
  paths <- mock$paths()
  paths[grepl("/rows$|/readSessions$|/readStreams/", paths)]
}

list_rows_queries <- function(mock) {
  Filter(function(r) r$path == ROWS, mock$requests()) |>
    lapply(function(r) r$query)
}

# --- routing -----------------------------------------------------------------

test_that("a small table reads in a single listRows request", {
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(data_paths(mock), ROWS)
  expect_equal(list_rows_queries(mock), list(list(format = "arrow")))
  expect_equal(nrow(df), 500)
  expect_equal(names(df), c("id", "name", "val"))
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("max_results = 0 reads no rows", {
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(max_results = 0, progress = FALSE)

  expect_equal(data_paths(mock), ROWS)
  expect_equal(list_rows_queries(mock), list(list(format = "arrow", maxResults = "0")))
  expect_equal(nrow(df), 0)
  expect_equal(names(df), c("id", "name", "val"))
})

test_that("a large table reads via a read session", {
  mock <- local_mock_api()
  mock$set(num_bytes = 5e8)

  df <- mock_table()$to_tibble(progress = FALSE, max_parallelization = 1)

  expect_equal(data_paths(mock), c(READ_SESSIONS, "/api/v1/readStreams/s0"))
  expect_equal(sort(as.numeric(df$id)), 0:499)
})

test_that("the listRows size threshold is exclusive", {
  mock <- local_mock_api()

  mock$set(num_bytes = LIST_ROWS_MAX_BYTES - 1)
  mock_table()$to_arrow_table(progress = FALSE, max_parallelization = 1)
  expect_equal(data_paths(mock)[[1]], ROWS)

  mock$set(num_bytes = LIST_ROWS_MAX_BYTES)
  mock_table()$to_arrow_table(progress = FALSE, max_parallelization = 1)
  expect_equal(tail(data_paths(mock), 2)[[1]], READ_SESSIONS)
})

test_that("max_results scales the size estimate", {
  mock <- local_mock_api()
  # 5e8 bytes over 500 rows: 1e6 bytes per row
  mock$set(num_bytes = 5e8)
  cases <- list(
    list(max_results = 50, expected = ROWS), # ~5e7 bytes
    list(max_results = 400, expected = READ_SESSIONS), # ~4e8 bytes
    list(max_results = 5000, expected = READ_SESSIONS) # capped at the table size
  )

  for (case in cases) {
    before <- length(data_paths(mock))
    df <- mock_table()$to_tibble(
      max_results = case$max_results,
      progress = FALSE,
      max_parallelization = 1
    )
    expect_equal(data_paths(mock)[[before + 1]], case$expected)
    expect_equal(nrow(df), min(case$max_results, 500))
  }
})

test_that("max_results and variables are forwarded to listRows", {
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(
    max_results = 7,
    variables = c("val", "id"),
    progress = FALSE
  )

  expect_equal(
    list_rows_queries(mock),
    list(list(format = "arrow", selectedVariables = "val,id", maxResults = "7"))
  )
  expect_equal(nrow(df), 7)
  expect_equal(names(df), c("val", "id"))
})

test_that("every output type reads via listRows", {
  mock <- local_mock_api()
  table <- mock_table()

  expect_equal(table$to_arrow_table(progress = FALSE)$num_rows, 500)
  expect_equal(table$to_arrow_dataset(progress = FALSE)$num_rows, 500)
  expect_equal(nrow(table$to_data_frame(progress = FALSE)), 500)
  expect_s3_class(table$to_data_table(progress = FALSE), "data.table")

  reader <- table$to_arrow_batch_reader(progress = FALSE)
  rows <- 0
  while (!is.null(batch <- reader$read_next_batch())) {
    rows <- rows + batch$num_rows
  }
  expect_equal(rows, 500)

  expect_equal(data_paths(mock), rep(ROWS, 5))
})

test_that("a batch_preprocessor applies to listRows reads", {
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(
    progress = FALSE,
    batch_preprocessor = function(batch) batch[as.vector(batch$id) < 10, ]
  )

  expect_equal(as.numeric(df$id), 0:9)
})

test_that("dataset tables read via listRows", {
  mock <- local_mock_api()
  mock$set(container_kind = "dataset") # schema coercion applies

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(data_paths(mock), ROWS)
  expect_equal(nrow(df), 500)
})

# --- queries -----------------------------------------------------------------

QUERIES <- "/api/v1/queries"
QUERY <- "/api/v1/queries/q1"
QUERY_VARIABLES <- paste0(QUERY, "/variables")
QUERY_ROWS <- paste0(QUERY, "/rows")
QUERY_READ_SESSIONS <- paste0(QUERY, "/readSessions")

# The listRows options each query was created with
query_list_rows <- function(mock) {
  Filter(function(r) r$path == QUERIES, mock$requests()) |>
    lapply(function(r) r$payload$listRows)
}

test_that("a read that runs a query reads the rows it responds with", {
  mock <- local_mock_api()

  df <- Query$new("SELECT 1")$to_tibble(progress = FALSE)

  expect_equal(
    query_list_rows(mock),
    list(list(format = "arrow", eosSentinel = TRUE))
  )
  expect_equal(mock$paths(), c(QUERIES, QUERY_VARIABLES))
  expect_equal(names(df), c("id", "name", "val"))
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("a query's inline rows are capped by max_results", {
  mock <- local_mock_api()

  df <- Query$new("SELECT 1")$to_tibble(max_results = 5, progress = FALSE)

  # A JSON number, not a string
  expect_identical(query_list_rows(mock)[[1]]$maxResults, 5L)
  expect_equal(mock$paths(), c(QUERIES, QUERY_VARIABLES))
  expect_equal(as.numeric(df$id), 0:4)
})

test_that("a query's inline rows are narrowed to the selected variables", {
  mock <- local_mock_api()

  df <- Query$new("SELECT 1")$to_tibble(
    variables = c("VAL", "id"),
    progress = FALSE
  )
  expect_equal(query_list_rows(mock)[[1]]$selectedVariables, list("VAL", "id"))
  expect_equal(names(df), c("val", "id"))
  expect_equal(nrow(df), 500)

  # A single variable is still sent as an array
  Query$new("SELECT 1")$to_tibble(variables = "name", progress = FALSE)
  expect_equal(query_list_rows(mock)[[2]]$selectedVariables, list("name"))

  # Batch readers otherwise pass batches through unchanged
  reader <- Query$new("SELECT 1")$to_arrow_batch_reader(variables = "val")
  batch <- reader$read_next_batch()
  expect_equal(names(batch), "val")
  expect_equal(mock$paths("/rows"), character())
})

test_that("an empty query result reads inline", {
  mock <- local_mock_api()
  mock$set(num_rows = 0)

  df <- Query$new("SELECT 1")$to_tibble(progress = FALSE)

  expect_equal(mock$paths(), c(QUERIES, QUERY_VARIABLES))
  expect_equal(nrow(df), 0)
  expect_equal(names(df), c("id", "name", "val"))
})

test_that("truncated inline rows are requested again", {
  mock <- local_mock_api()
  # Cut off at a batch boundary, so only the missing sentinel reveals it
  mock$set(truncate_inline_rows_once = 200)

  df <- Query$new("SELECT 1")$to_tibble(progress = FALSE)

  expect_equal(mock$paths(), c(QUERIES, QUERY_VARIABLES, QUERY_ROWS))
  rows_request <- Filter(function(r) r$path == QUERY_ROWS, mock$requests())
  expect_equal(
    rows_request[[1]]$query,
    list(format = "arrow", eosSentinel = "true")
  )
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("a query that responds without its rows is read as before", {
  mock <- local_mock_api()
  # An API that ignores listRows
  mock$set(query_inline_rows = FALSE)

  df <- Query$new("SELECT 1")$to_tibble(
    progress = FALSE,
    max_parallelization = 1
  )

  expect_length(query_list_rows(mock), 1)
  expect_equal(
    mock$paths(),
    c(QUERIES, QUERY_VARIABLES, QUERY_READ_SESSIONS, "/api/v1/readStreams/s0")
  )
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("a query that's still running when created is waited for", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(list(
      status = 200,
      body = list(
        id = "q1",
        kind = "query",
        uri = "/queries/q1",
        status = "running"
      )
    ))
  )

  df <- Query$new("SELECT 1")$to_tibble(
    progress = FALSE,
    max_parallelization = 1
  )

  expect_equal(
    mock$paths(),
    c(
      QUERIES,
      QUERY,
      QUERY_VARIABLES,
      QUERY_READ_SESSIONS,
      "/api/v1/readStreams/s0"
    )
  )
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("queries created other than by a read don't ask for their rows", {
  mock <- local_mock_api()

  Query$new("SELECT 1")$get()
  Query$new("SELECT 1")$list_variables()
  Query$new("SELECT 1")$to_read_streams(target_count = 1)

  expect_equal(query_list_rows(mock), list(NULL, NULL, NULL))
  expect_length(mock$paths("/rows"), 0)
})

test_that("a query that's been fetched reads via a read session", {
  mock <- local_mock_api()
  query <- Query$new("SELECT 1")
  query$get()

  df <- query$to_tibble(progress = FALSE, max_parallelization = 1)

  expect_equal(
    mock$paths(),
    c(
      QUERIES,
      QUERY,
      QUERY_VARIABLES,
      QUERY_READ_SESSIONS,
      "/api/v1/readStreams/s0"
    )
  )
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("a query read inline can be read again", {
  mock <- local_mock_api()
  query <- Query$new("SELECT 1")
  query$to_tibble(progress = FALSE)

  df <- query$to_tibble(progress = FALSE, max_parallelization = 1)

  # The query is fetched for the size the inline response lacked
  expect_equal(
    mock$paths(),
    c(
      QUERIES,
      QUERY_VARIABLES,
      QUERY,
      QUERY_VARIABLES,
      QUERY_READ_SESSIONS,
      "/api/v1/readStreams/s0"
    )
  )
  expect_equal(as.numeric(df$id), 0:499)
  expect_equal(query$properties$outputNumBytes, 1000)
})

test_that("a large query read inline is then read via an export", {
  mock <- local_mock_api()
  query <- Query$new("SELECT 1")
  query$to_tibble(max_results = 1, progress = FALSE)
  # The full result is too large to stream, which only the fetched query says
  mock$set(query_bytes = 2e9)

  query$to_tibble(progress = FALSE)

  expect_true("/api/v1/queries/q1/exports" %in% mock$paths())
})

# --- table metadata freshness ------------------------------------------------

test_that("a new table object fetches its metadata once", {
  mock <- local_mock_api()

  mock_table()$to_tibble(progress = FALSE)

  expect_equal(sum(mock$paths() == TABLE), 1)
})

test_that("a reused table object refetches its metadata before reading", {
  mock <- local_mock_api()
  table <- mock_table()
  table$to_tibble(progress = FALSE)
  # The table shrinks, e.g. after a replace-merge upload
  mock$set(num_rows = 300)

  df <- table$to_tibble(progress = FALSE)

  expect_equal(sum(mock$paths() == TABLE), 2)
  expect_equal(nrow(df), 300)
  # A stale row count would make the complete read look truncated
  expect_equal(data_paths(mock), rep(ROWS, 2))
})

# --- interrupted listRows reads ----------------------------------------------

test_that("a truncated listRows response is requested again", {
  mock <- local_mock_api()
  mock$set(truncate_rows_once = 120)

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(data_paths(mock), rep(ROWS, 2))
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("a truncated listRows response never duplicates batches in a reader", {
  # Unlike a read stream, listRows can't be resumed, so the whole response is
  # verified before the reader hands out any batch
  mock <- local_mock_api()
  mock$set(truncate_rows_once = 120)

  reader <- mock_table()$to_arrow_batch_reader(progress = FALSE)
  ids <- c()
  while (!is.null(batch <- reader$read_next_batch())) {
    ids <- c(ids, as.vector(batch$id))
  }

  expect_equal(as.numeric(ids), 0:499)
})

test_that("a truncated listRows response on disk leaves no duplicates", {
  mock <- local_mock_api()
  mock$set(truncate_rows_once = 120)

  ds <- mock_table()$to_arrow_dataset(progress = FALSE)

  expect_equal(sort(as.numeric(as.data.frame(ds)$id)), 0:499)
})

test_that("a listRows request made after a truncation retries its own 503s", {
  mock <- local_mock_api()
  mock$set(
    truncate_rows_once = 120,
    response_script = list(
      list(status = NULL), # table metadata
      list(status = NULL), # variables
      list(status = NULL), # rows, truncated
      list(status = 503, body = list(status = 503, error = "unavailable")),
      list(status = 503, body = list(status = 503, error = "unavailable"))
    )
  )

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(data_paths(mock), rep(ROWS, 4))
  expect_equal(as.numeric(df$id), 0:499)
})

test_that("listRows server errors aren't retried", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(
      list(status = NULL), # table metadata
      list(status = NULL), # variables
      list(status = 500, body = list(status = 500, error = "internal_error"))
    )
  )

  expect_error(mock_table()$to_tibble(progress = FALSE), class = "redivis_api_error")
  expect_length(mock$paths("/rows"), 1)
})

test_that("rows listRows can't serve are read through a read session", {
  read_session_required <- list(
    list(status = NULL), # table metadata
    list(status = NULL), # variables
    list(
      status = 400,
      body = list(
        status = 400,
        error = "read_session_required",
        error_description = "Use a read session"
      )
    )
  )

  for (read in list(
    function(table) table$to_tibble(progress = FALSE),
    function(table) {
      reader <- table$to_arrow_batch_reader(progress = FALSE)
      batches <- list()
      while (!is.null(batch <- reader$read_next_batch())) {
        batches[[length(batches) + 1]] <- batch
      }
      as.data.frame(do.call(arrow::arrow_table, batches))
    }
  )) {
    mock <- local_mock_api()
    mock$set(response_script = read_session_required)

    result <- read(mock_table())

    expect_equal(nrow(result), 500)
    expect_equal(sort(as.numeric(result$id)), 0:499)
    expect_equal(data_paths(mock)[1:2], c(ROWS, READ_SESSIONS))
  }
})

test_that("listRows API errors aren't retried", {
  mock <- local_mock_api()
  mock$set(
    response_script = list(
      list(status = NULL), # table metadata
      list(status = NULL), # variables
      list(status = 404, body = list(status = 404, error = "not_found"))
    )
  )

  expect_error(
    mock_table()$to_tibble(progress = FALSE),
    class = "redivis_not_found_error"
  )
  expect_length(mock$paths("/rows"), 1)
})

test_that("batch readers pass batches through when no types need converting", {
  mock <- local_mock_api()
  mock$set(omit_column = "val")

  for (num_bytes in c(1000, 5e8)) { # listRows, then a read session
    mock$set(num_bytes = num_bytes)
    reader <- mock_table()$to_arrow_batch_reader(progress = FALSE)
    rows <- 0
    while (!is.null(batch <- reader$read_next_batch())) {
      expect_equal(names(batch), c("id", "name"))
      rows <- rows + batch$num_rows
    }
    expect_equal(rows, 500)
  }
  expect_equal(data_paths(mock), c(ROWS, READ_SESSIONS, "/api/v1/readStreams/s0"))
})

# --- read streams ------------------------------------------------------------

test_that("read streams read their slice of the table", {
  mock <- local_mock_api()
  streams <- mock_table()$to_read_streams(target_count = 2)

  expect_equal(nrow(streams[[1]]$to_tibble(progress = FALSE)), 250)
  expect_equal(streams[[2]]$to_arrow_table(progress = FALSE)$num_rows, 250)
  expect_length(mock$paths("/exports"), 0)
})

test_that("read streams don't refetch their parent", {
  mock <- local_mock_api()
  streams <- mock_table()$to_read_streams(target_count = 2)
  for (stream in streams) {
    stream$to_arrow_table(progress = FALSE)
  }

  # Once, when creating the read session
  expect_equal(sum(mock$paths() == TABLE), 1)
})

# --- anonymous access --------------------------------------------------------

test_that("a public table reads anonymously", {
  mock <- local_mock_api(authenticated = FALSE)

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_equal(nrow(df), 500)
  expect_false(any(mock$authorized()))
})
