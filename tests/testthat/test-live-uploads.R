# Uploading data to tables, in each of the forms that upload$create() accepts

test_that("uploading a file path names the upload after the file", {
  table <- local_live_table()

  upload <- table$upload()$create(tiny_csv_path(), progress = FALSE)

  expect_equal(upload$name, "tiny.csv")
  expect_equal(upload$properties$status, "completed")
  expect_equal(upload$properties$numRows, nrow(tiny_expected()))
  expect_equal(table$get()$properties$numRows, nrow(tiny_expected()))
})

test_that("a connection can be uploaded", {
  table <- local_live_table()
  con <- file(tiny_csv_path(), "rb")
  withr::defer(close(con))

  table$upload("tiny.csv")$create(con, type = "delimited", progress = FALSE)

  expect_rows_equal(table$to_tibble(progress = FALSE), tiny_expected())
})

test_that("a raw vector can be uploaded", {
  table <- local_live_table()

  table$upload("small.csv")$create(small_csv, progress = FALSE)

  df <- sorted_by(table$to_tibble(progress = FALSE))
  expect_equal(df$id, 1:3)
  expect_equal(df$name, c("a", "b", "c"))
  expect_equal(df$value, c(1.5, 2.5, NA))
})

dataframe_contents <- function() {
  data.frame(
    id = 1:3,
    name = c("a", "b", NA),
    value = c(1.5, NA, -3),
    flag = c(TRUE, FALSE, TRUE),
    day = as.Date(c("2020-01-01", "2021-06-15", NA)),
    stringsAsFactors = FALSE
  )
}

dataframe_factories <- list(
  data.frame = dataframe_contents,
  tibble = function() tibble::as_tibble(dataframe_contents()),
  data.table = function() data.table::as.data.table(dataframe_contents()),
  arrow_table = function() arrow::arrow_table(dataframe_contents()),
  arrow_record_batch = function() arrow::record_batch(dataframe_contents()),
  arrow_dataset = function() {
    path <- withr::local_tempdir(.local_envir = testthat::teardown_env())
    arrow::write_dataset(dataframe_contents(), path)
    arrow::open_dataset(path)
  }
)

for (kind in names(dataframe_factories)) {
  test_that(sprintf("a %s can be uploaded", kind), {
    table <- local_live_table(kind)

    upload <- table$upload(kind)$create(
      dataframe_factories[[kind]](),
      progress = FALSE
    )

    expect_equal(upload$properties$type, "parquet")
    expect_rows_equal(table$to_tibble(progress = FALSE), dataframe_contents())
  })
}

sf_points <- function() {
  sf::st_sf(
    id = 1:2,
    name = c("a", "b"),
    geometry = sf::st_sfc(
      sf::st_point(c(-122, 37)),
      sf::st_point(c(0.5, 51.5)),
      crs = 4326
    )
  )
}

test_that("an sf data frame is uploaded with a geography variable", {
  skip_if_not_installed("sf")
  table <- local_live_table()

  upload <- table$upload("points")$create(sf_points(), progress = FALSE)

  expect_equal(upload$properties$type, "parquet")
  expect_equal(
    variable_types(table$list_variables()),
    c(id = "integer", name = "string", geometry = "geography")
  )
  actual <- table$to_sf_tibble(progress = FALSE)
  actual <- actual[order(actual$id), ]
  expect_equal(
    unname(sf::st_coordinates(actual)),
    matrix(c(-122, 0.5, 37, 51.5), ncol = 2)
  )
})

test_that("an sf data frame's coordinates are uploaded in WGS84", {
  skip_if_not_installed("sf")
  table <- local_live_table()

  table$upload("points")$create(
    sf::st_transform(sf_points(), 3857),
    progress = FALSE
  )

  actual <- table$to_sf_tibble(progress = FALSE)
  actual <- actual[order(actual$id), ]
  expect_equal(
    unname(sf::st_coordinates(actual)),
    matrix(c(-122, 0.5, 37, 51.5), ncol = 2),
    tolerance = 1e-6
  )
})

large_csv <- function(num_rows) {
  charToRaw(paste0(
    "id,text\n",
    paste0(
      seq_len(num_rows) - 1,
      ",row number ",
      seq_len(num_rows) - 1,
      " of the large upload\n",
      collapse = ""
    )
  ))
}

test_that("content over 1MB is sent through temporary storage", {
  table <- local_live_table()
  content <- large_csv(40000)
  expect_gt(length(content), 2^20)

  upload <- table$upload("large.csv")$create(content, progress = FALSE)

  expect_equal(upload$properties$numRows, 40000)
  expect_equal(table$get()$properties$numRows, 40000)
})

test_that("a file over 1MB is sent through temporary storage", {
  table <- local_live_table()
  path <- withr::local_tempfile(fileext = ".csv")
  writeBin(large_csv(40000), path)

  upload <- table$upload()$create(path, progress = FALSE)

  expect_equal(upload$name, basename(path))
  expect_equal(upload$properties$numRows, 40000)
})

test_that("a file of 32MB or more is uploaded resumably", {
  skip_unless_slow()
  table <- local_live_table()
  path <- withr::local_tempfile(fileext = ".csv")
  writeBin(large_csv(1e6), path)
  expect_gte(file.size(path), 2^25)

  upload <- table$upload()$create(path, progress = FALSE)

  expect_equal(upload$properties$numRows, 1e6)
})

test_that("a Redivis table can be transferred in", {
  table <- local_live_table()

  table$upload("transferred")$create(
    transfer_specification = list(
      sourceType = "redivis",
      sourcePath = live_tiny_table()$qualified_reference
    ),
    progress = FALSE
  )

  expect_rows_equal(table$to_tibble(progress = FALSE), tiny_expected())
})

test_that("rows can be streamed to an upload", {
  table <- local_live_table()
  upload <- table$upload("stream")$create(
    type = "stream",
    schema = list(
      list(name = "id", type = "integer"),
      list(name = "name", type = "string"),
      list(name = "when", type = "dateTime")
    ),
    progress = FALSE
  )

  upload$insert_rows(data.frame(
    id = 1:2,
    name = c("a", NA),
    when = c("2020-01-01T00:00:00", NA)
  ))
  upload$insert_rows('[{"id": 3, "name": "c", "when": "2021-06-15T12:30:00"}]')

  expect_equal(
    variable_types(upload$list_variables()),
    c(id = "integer", name = "string", when = "dateTime")
  )
  df <- sorted_by(upload$to_tibble(progress = FALSE))
  expect_equal(df$id, 1:3)
  expect_equal(is.na(df$name), c(FALSE, TRUE, FALSE))
  expect_error(upload$insert_rows(list(id = 4)), class = "redivis_value_error")
})

test_that("a missing file path is an error", {
  table <- local_live_table()

  expect_error(
    table$upload("missing.csv")$create("no/such/file.csv", progress = FALSE),
    "No file found",
    class = "redivis_value_error"
  )
})
