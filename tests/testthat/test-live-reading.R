# Reading tables and uploads into each of the supported formats, verified
# against the data that was uploaded

test_that("to_tibble() reads a table", {
  df <- live_tiny_table()$to_tibble(progress = FALSE)

  expect_s3_class(df, "tbl_df")
  expect_rows_equal(df, tiny_expected())
})

test_that("to_data_frame() reads a plain data frame", {
  df <- live_tiny_table()$to_data_frame(progress = FALSE)

  expect_identical(class(df), "data.frame")
  expect_rows_equal(df, tiny_expected())
})

test_that("to_data_table() reads a data.table", {
  df <- live_tiny_table()$to_data_table(progress = FALSE)

  expect_s3_class(df, "data.table")
  expect_rows_equal(df, tiny_expected())
})

test_that("to_arrow_table() reads an arrow table", {
  table <- live_tiny_table()$to_arrow_table(progress = FALSE)

  expect_s3_class(table, "Table")
  expect_equal(table$schema$names, names(tiny_expected()))
  expect_equal(table$schema$id$type$ToString(), "int64")
  expect_equal(table$schema$Boolean$type$ToString(), "bool")
  expect_rows_equal(as.data.frame(table), tiny_expected())
})

test_that("to_arrow_dataset() reads an arrow dataset", {
  skip_if_not_installed("dplyr")
  dataset <- live_tiny_table()$to_arrow_dataset(progress = FALSE)

  expect_s3_class(dataset, "Dataset")
  expect_equal(nrow(dataset), nrow(tiny_expected()))
  expect_rows_equal(dplyr::collect(dataset), tiny_expected())
})

test_that("to_arrow_batch_reader() reads record batches", {
  reader <- live_tiny_table()$to_arrow_batch_reader(progress = FALSE)
  withr::defer(reader$close())

  expect_equal(reader$schema$names, names(tiny_expected()))
  batches <- list()
  while (!is.null(batch <- reader$read_next_batch())) {
    expect_s3_class(batch, "RecordBatch")
    batches[[length(batches) + 1]] <- batch
  }
  expect_rows_equal(
    as.data.frame(do.call(arrow::arrow_table, batches)),
    tiny_expected()
  )
})

test_that("max_results limits the rows read", {
  table <- live_tiny_table()

  expect_equal(nrow(table$to_tibble(10, progress = FALSE)), 10)
  expect_equal(nrow(table$to_arrow_table(max_results = 0, progress = FALSE)), 0)
})

test_that("selected variables are read in the order given", {
  # Variable names are matched case-insensitively
  df <- live_tiny_table()$to_tibble(variables = c("text", "ID"), progress = FALSE)

  expect_equal(names(df), c("Text", "id"))
  expect_rows_equal(df, tiny_expected()[c("Text", "id")])
})

test_that("a batch preprocessor transforms each batch", {
  df <- live_tiny_table()$to_tibble(
    batch_preprocessor = function(batch) batch[batch$Boolean, c("id", "Key")],
    progress = FALSE
  )

  expected <- tiny_expected()
  expect_rows_equal(df, expected[expected$Boolean, c("id", "Key")])
})

test_that("a table can be read by its qualified reference", {
  table <- redivis$table(live_tiny_table()$qualified_reference)

  expect_rows_equal(table$to_tibble(progress = FALSE), tiny_expected())
})

test_that("an upload can be read", {
  upload <- live_tiny_table()$upload("tiny.csv")

  expect_rows_equal(upload$to_tibble(progress = FALSE), tiny_expected())
})

for (target_count in c(1, 3)) {
  test_that(sprintf("a table can be read in %d read stream(s)", target_count), {
    streams <- live_tiny_table()$to_read_streams(target_count)

    expect_gte(length(streams), 1)
    expect_lte(length(streams), target_count)
    combined <- do.call(
      rbind,
      lapply(streams, function(stream) stream$to_data_frame(progress = FALSE))
    )
    expect_rows_equal(combined, tiny_expected())
  })
}

test_that("read streams read the variables chosen when they're created", {
  streams <- live_tiny_table()$to_read_streams(2, variables = c("Key", "id"))

  combined <- do.call(
    arrow::concat_tables,
    lapply(streams, function(stream) stream$to_arrow_table(progress = FALSE))
  )
  expect_equal(combined$schema$names, c("Key", "id"))
  expect_rows_equal(as.data.frame(combined), tiny_expected()[c("Key", "id")])
  expect_error(
    streams[[1]]$to_tibble(variables = "id", progress = FALSE),
    class = "redivis_value_error"
  )
  expect_error(streams[[1]]$download(), class = "redivis_value_error")
})

test_that("typed values round trip", {
  df <- live_typed_table()$to_tibble(progress = FALSE)

  expect_rows_equal(df, typed_data())
  expect_type(df$id, "integer")
  expect_s3_class(df$date_col, "Date")
  expect_s3_class(df$datetime_col, "POSIXct")
})

test_that("typed values are read with their arrow types", {
  schema <- live_typed_table()$to_arrow_table(progress = FALSE)$schema

  expect_equal(
    lapply(schema$fields, function(field) field$type$ToString()),
    list("int64", "double", "string", "string", "bool", "date32[day]", "timestamp[us]")
  )
})

test_that("values can be read with their labels", {
  skip_if_not_installed("haven")
  table <- local_live_table()
  table$upload("small.csv")$create(
    small_csv,
    metadata = list(
      name = list(label = "The name", valueLabels = list(a = "The letter a")),
      value = list(label = "The value")
    ),
    progress = FALSE
  )

  df <- table$to_tibble(labelled = TRUE, progress = FALSE)

  expect_s3_class(df$name, "haven_labelled")
  expect_equal(attr(df$name, "label"), "The name")
  expect_equal(attr(df$name, "labels"), c("The letter a" = "a"))
  expect_equal(attr(df$value, "label"), "The value")
  expect_null(attr(df$id, "label"))
})

for (format in c("csv", "parquet")) {
  test_that(sprintf("a table can be downloaded as %s", format), {
    path <- withr::local_tempdir()

    paths <- live_tiny_table()$download(path, format = format, progress = FALSE)

    expect_equal(basename(paths), paste0("tiny.", format))
    downloaded <- if (format == "csv") {
      read_csv_file(paths)
    } else {
      arrow::read_parquet(paths)
    }
    expect_rows_equal(downloaded, tiny_expected())
  })
}

test_that("a table can be downloaded to a file path", {
  path <- file.path(withr::local_tempdir(), "out", "my_table.csv")
  table <- live_tiny_table()

  expect_equal(table$download(path, progress = FALSE), path)
  expect_equal(nrow(utils::read.csv(path)), nrow(tiny_expected()))

  expect_error(
    table$download(path, progress = FALSE),
    "already exists",
    class = "redivis_value_error"
  )
  expect_equal(table$download(path, overwrite = TRUE, progress = FALSE), path)
})

test_that("an upload can be downloaded", {
  paths <- live_tiny_table()$upload("tiny.csv")$download(
    withr::local_tempdir(),
    progress = FALSE
  )

  expect_rows_equal(
    read_csv_file(paths),
    tiny_expected()
  )
})
