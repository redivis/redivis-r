# SQL queries, run against the uploaded sample data

test_that("a query reads the tables it references", {
  table <- live_tiny_table()
  query <- redivis$query(sprintf(
    "SELECT Key, COUNT(*) AS n, SUM(Numeric) AS total FROM `%s` GROUP BY Key",
    table$qualified_reference
  ))

  actual <- query$to_tibble(progress = FALSE)

  expected <- tiny_expected()
  grouped <- data.frame(
    Key = sort(unique(expected$Key)),
    n = as.vector(table(expected$Key)),
    total = as.vector(tapply(expected$Numeric, expected$Key, sum))
  )
  expect_rows_equal(actual, grouped, by = "Key")
})

test_that("a query can be scoped to a dataset", {
  live_tiny_table()
  query <- live_sample_dataset()$query(
    sprintf("SELECT %s FROM tiny WHERE Boolean", tiny_columns_sql())
  )

  expected <- tiny_expected()
  expect_rows_equal(query$to_tibble(progress = FALSE), expected[expected$Boolean, ])
})

test_that("a query has properties and variables", {
  query <- redivis$query(sprintf(
    "SELECT id, Key FROM `%s` WHERE id <= 10",
    live_tiny_table()$qualified_reference
  ))

  variables <- query$list_variables()

  expect_equal(variable_types(variables), c(id = "integer", Key = "string"))
  expect_equal(query$properties$status, "completed")
  expect_equal(query$get()$properties$outputNumRows, 10)
  expect_equal(query$variable("Key")$get()$properties$type, "string")
})

test_that("literals of each type are read as the corresponding R type", {
  live_api()
  query <- redivis$query("
    SELECT
      1 AS int_col,
      1.5 AS float_col,
      'text' AS string_col,
      TRUE AS bool_col,
      DATE '2020-01-02' AS date_col,
      DATETIME '2020-01-02 03:04:05' AS datetime_col,
      TIME '03:04:05' AS time_col
    UNION ALL
    SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL
  ")

  df <- query$to_tibble(progress = FALSE)
  df <- df[order(is.na(df$int_col)), ]

  expect_equal(df$int_col, c(1, NA))
  expect_equal(df$float_col, c(1.5, NA))
  expect_equal(df$string_col, c("text", NA))
  expect_equal(df$bool_col, c(TRUE, NA))
  expect_equal(df$date_col, as.Date(c("2020-01-02", NA)))
  expect_equal(
    format(df$datetime_col, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    c("2020-01-02 03:04:05", NA)
  )
  expect_s3_class(df$time_col, "hms")
  expect_equal(as.numeric(df$time_col), c(3 * 3600 + 4 * 60 + 5, NA))
})

test_that("a query can return no rows", {
  query <- redivis$query(sprintf(
    "SELECT id, Key FROM `%s` WHERE FALSE",
    live_tiny_table()$qualified_reference
  ))

  df <- query$to_tibble(progress = FALSE)

  expect_equal(nrow(df), 0)
  expect_equal(names(df), c("id", "Key"))
})

test_that("an invalid query raises an error", {
  live_api()
  query <- redivis$query("SELECT * FROM a_table_that_does_not_exist")

  expect_error(query$to_tibble(progress = FALSE), class = "redivis_error")
})

test_that("a query can be read in read streams", {
  query <- redivis$query(sprintf(
    "SELECT %s FROM `%s`",
    tiny_columns_sql(),
    live_tiny_table()$qualified_reference
  ))

  streams <- query$to_read_streams(2)

  combined <- do.call(
    rbind,
    lapply(streams, function(stream) stream$to_data_frame(progress = FALSE))
  )
  expect_rows_equal(combined, tiny_expected())
})

test_that("a query's results can be downloaded", {
  query <- redivis$query(sprintf(
    "SELECT %s FROM `%s`",
    tiny_columns_sql(),
    live_tiny_table()$qualified_reference
  ))

  paths <- query$download(withr::local_tempdir(), progress = FALSE)

  expect_rows_equal(
    read_csv_file(paths[[1]]),
    tiny_expected()
  )
})

test_that("a query of a file index table lists its files", {
  query <- redivis$query(sprintf(
    "SELECT * FROM `%s`",
    live_files_table()$qualified_reference
  ))

  files <- query$list_files()

  expect_setequal(
    vapply(files, function(f) f$path, character(1)),
    names(raw_files())
  )
  file <- query$to_directory()$get("readme.txt")
  expect_equal(file$read(), raw_files()[["readme.txt"]])
})
