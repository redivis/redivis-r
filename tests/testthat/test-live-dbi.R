# The DBI and dbplyr interface, run against the uploaded sample data. See
# test-offline-dbi.R for its SQL translation.

local_sample_con <- function(env = parent.frame()) {
  live_tiny_table()
  live_typed_table()
  con <- live_sample_dataset()$connect_dbi()
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}

test_that("a dataset connection lists its tables and fields", {
  con <- local_sample_con()

  expect_true(all(c("tiny", "typed", "geo", "files") %in% DBI::dbListTables(con)))
  expect_equal(DBI::dbListFields(con, "tiny"), names(tiny_variable_types))
  expect_true(DBI::dbExistsTable(con, "tiny"))
  expect_false(DBI::dbExistsTable(con, "not_a_table"))
})

test_that("dbGetQuery() runs a query", {
  con <- local_sample_con()

  df <- DBI::dbGetQuery(con, "SELECT id, Key FROM tiny WHERE id <= 5")

  expect_rows_equal(df, tiny_expected()[1:5, c("id", "Key")])
})

test_that("a result can be fetched, and describes its columns", {
  con <- local_sample_con()

  result <- DBI::dbSendQuery(con, "SELECT id, Key FROM tiny")
  withr::defer(DBI::dbClearResult(result))

  expect_equal(DBI::dbColumnInfo(result)$name, c("id", "Key"))
  expect_equal(nrow(DBI::dbFetch(result)), nrow(tiny_expected()))
  expect_true(DBI::dbHasCompleted(result))
})

test_that("a global connection resolves qualified references", {
  live_api()
  con <- redivis$connect_dbi()
  withr::defer(DBI::dbDisconnect(con))

  df <- DBI::dbGetQuery(
    con,
    sprintf("SELECT COUNT(*) AS n FROM `%s`", live_tiny_table()$qualified_reference)
  )

  expect_equal(df$n, nrow(tiny_expected()))
})

test_that("dplyr verbs are run on Redivis", {
  skip_if_not_installed("dplyr")
  con <- local_sample_con()
  tiny <- dplyr::tbl(con, "tiny")

  result <- tiny |>
    dplyr::filter(Boolean, Year >= 2000) |>
    dplyr::group_by(Key) |>
    dplyr::summarise(n = dplyr::n(), total = sum(Numeric, na.rm = TRUE)) |>
    dplyr::collect()

  expected <- tiny_expected()
  expected <- expected[expected$Boolean & expected$Year >= 2000, ]
  expect_rows_equal(
    result,
    data.frame(
      Key = sort(unique(expected$Key)),
      n = as.vector(table(expected$Key)),
      total = as.vector(tapply(expected$Numeric, expected$Key, sum))
    ),
    by = "Key"
  )
  expect_equal(dplyr::pull(dplyr::count(tiny), n), nrow(tiny_expected()))
})

test_that("translated functions run on Redivis", {
  skip_if_not_installed("dplyr")
  con <- local_sample_con()

  result <- dplyr::tbl(con, "typed") |>
    dplyr::filter(id == 1L) |>
    dplyr::mutate(
      year = year(date_col),
      wday = wday(date_col),
      month_start = floor_date(date_col, "month"),
      plus_30 = add_days(date_col, 30),
      days_to_2021 = date_count_between(date_col, !!as.Date("2021-01-01"), "day"),
      built = date_build(2020, 1, 15),
      label = paste(id, string_col, sep = "-"),
      padded = str_pad(as.character(id), 3, pad = "0"),
      has_digit = str_detect(as.character(float_col), "\\d+"),
      replaced = str_replace("banana", "an", "X"),
      starts = str_starts(string_col, "pl|x"),
      rounded = round(float_col / 7, 2),
      substring = str_sub("abcdef", 2),
      floor_div = -7L %/% 3L,
      mod_neg_dividend = -7L %% 3L,
      mod_neg_divisor = 7L %% -3L,
      mod_double = 7.5 %% 2,
      after_2000 = datetime_col > !!as.POSIXct("2000-01-01", tz = "UTC")
    ) |>
    dplyr::collect()

  expect_equal(nrow(result), 1)
  expect_equal(result$year, 2020)
  expect_equal(result$wday, 4) # 2020-01-01 was a Wednesday; Sunday is 1
  expect_equal(result$month_start, as.Date("2020-01-01"))
  expect_equal(result$plus_30, as.Date("2020-01-31"))
  expect_equal(result$days_to_2021, 366)
  expect_equal(result$built, as.Date("2020-01-15"))
  expect_equal(result$label, "1-plain")
  expect_equal(result$padded, "001")
  expect_true(result$has_digit)
  expect_equal(result$replaced, "bXana")
  expect_true(result$starts)
  expect_equal(result$rounded, round(1.5 / 7, 2))
  expect_equal(result$substring, "bcdef")
  expect_equal(result$floor_div, -7L %/% 3L)
  expect_equal(result$mod_neg_dividend, -7L %% 3L)
  expect_equal(result$mod_neg_divisor, 7L %% -3L)
  expect_equal(result$mod_double, 7.5 %% 2)
  expect_true(result$after_2000)
})

test_that("translated aggregates run on Redivis", {
  skip_if_not_installed("dplyr")
  con <- local_sample_con()

  summary <- dplyr::tbl(con, "tiny") |>
    dplyr::summarise(
      median_year = quantile(Year, 0.5),
      keys = str_flatten(Key, ",")
    ) |>
    dplyr::collect()

  expected <- tiny_expected()
  expect_equal(summary$median_year, stats::median(expected$Year), tolerance = 1)
  expect_setequal(strsplit(summary$keys, ",")[[1]], expected$Key)
})
