# The DBI connector's SQL translation, which needs no server. See
# test-live-dbi.R for queries run against Redivis.

offline_con <- function() {
  DBI::dbConnect(RedivisDBI())
}

expect_translation <- function(expr, expected) {
  con <- offline_con()
  actual <- dbplyr::translate_sql(!!rlang::enexpr(expr), con = con, window = FALSE)
  expect_equal(as.character(actual), expected)
}

test_that("literals are escaped for GoogleSQL", {
  con <- offline_con()
  expect_equal(
    as.character(DBI::dbQuoteString(con, c("it's", "a\\d", NA))),
    c("'it\\'s'", "'a\\\\d'", "NULL")
  )
  expect_translation(x == !!as.Date("2020-01-02"), "`x` = DATE '2020-01-02'")
  expect_translation(
    x > !!as.POSIXct("2020-01-02 03:04:05", tz = "UTC"),
    "`x` > DATETIME '2020-01-02 03:04:05.000000'"
  )
})

test_that("whole-number arguments are rendered as INT64", {
  expect_translation(round(x, 2), "ROUND(`x`, 2)")
  expect_translation(substr(x, 1, 3), "SUBSTR(`x`, 1, 3 - 1 + 1)")
  expect_translation(str_sub(x, 2), "SUBSTR(`x`, 2)")
  expect_translation(str_pad(x, 3, pad = "0"), "LPAD(`x`, 3, '0')")
})

test_that("integer division and modulo round toward negative infinity, like R", {
  expect_translation(x %/% 7, "FLOOR(`x` / 7.0)")
  expect_translation(x %% 7, "(`x` - 7.0 * FLOOR(`x` / 7.0))")
  expect_translation(x %% 7 * 2, "((`x` - 7.0 * FLOOR(`x` / 7.0))) * 2.0")
})

test_that("string functions translate", {
  expect_translation(
    paste(a, b, sep = "-"),
    "ARRAY_TO_STRING([CAST(`a` AS STRING), CAST(`b` AS STRING)], '-')"
  )
  expect_translation(str_detect(x, "\\d+"), "REGEXP_CONTAINS(`x`, '\\\\d+')")
  expect_translation(str_detect(x, "a", negate = TRUE), "NOT REGEXP_CONTAINS(`x`, 'a')")
  expect_translation(str_starts(x, "a|b"), "REGEXP_CONTAINS(`x`, '^(?:a|b)')")
  expect_translation(str_ends(x, "a"), "REGEXP_CONTAINS(`x`, '(?:a)$')")
  expect_translation(startsWith(x, "a"), "STARTS_WITH(`x`, 'a')")
  expect_translation(
    str_replace(x, "an", "X"),
    "REGEXP_REPLACE(`x`, '(?s)^(.*?)(?:an)', '\\\\1X')"
  )
  expect_translation(str_remove_all(x, "a"), "REGEXP_REPLACE(`x`, 'a', '')")
  expect_error(
    dbplyr::translate_sql(str_replace(x, "(a)", "\\1"), con = offline_con()),
    class = "redivis_value_error"
  )
})

test_that("date functions translate", {
  expect_translation(year(d), "EXTRACT(YEAR FROM `d`)")
  expect_translation(get_month(d), "EXTRACT(MONTH FROM `d`)")
  expect_translation(wday(d), "EXTRACT(DAYOFWEEK FROM `d`)")
  expect_translation(floor_date(d, "months"), "DATE_TRUNC(`d`, MONTH)")
  expect_translation(today(), "CURRENT_DATE()")
  expect_translation(Sys.time(), "CURRENT_DATETIME()")
  expect_translation(add_days(d, 30), "DATE_ADD(`d`, INTERVAL 30 DAY)")
  expect_translation(date_count_between(a, b, "day"), "DATE_DIFF(`b`, `a`, DAY)")
  expect_translation(difftime(a, b, units = "days"), "DATE_DIFF(`a`, `b`, DAY)")
  expect_translation(date_build(2020, 1, 15), "DATE(2020, 1, 15)")
  expect_translation(as.Date(x, format = "%Y%m%d"), "PARSE_DATE('%Y%m%d', `x`)")
  expect_translation(as.POSIXct(x), "CAST(`x` AS DATETIME)")
  expect_error(
    dbplyr::translate_sql(difftime(a, b, units = "weeks"), con = offline_con()),
    class = "redivis_value_error"
  )
  expect_translation(date_count_between(a, b, "day", n = 1L), "DATE_DIFF(`b`, `a`, DAY)")
  expect_error(
    dbplyr::translate_sql(date_count_between(a, b, "day", n = 1.5), con = offline_con()),
    class = "redivis_value_error"
  )
})

test_that("aggregates translate", {
  expect_translation(
    quantile(x, 0.25),
    "APPROX_QUANTILES(`x`, 1000)[OFFSET(250)]"
  )
  expect_translation(str_flatten(x, ","), "STRING_AGG(`x`, ',')")
})

test_that("connections are read-only", {
  con <- offline_con()
  expect_error(DBI::dbWriteTable(con, "t", data.frame(x = 1)), "read-only")
  expect_error(DBI::dbRemoveTable(con, "t"), "read-only")
  expect_error(DBI::dbCreateTable(con, "t", c(x = "INT64")), "read-only")
})
