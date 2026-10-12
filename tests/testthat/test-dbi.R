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

# ---- Live ------------------------------------------------------------------

# Workflow with the CMS synthetic OMOP dataset as a datasource. Override with
# REDIVIS_TEST_OMOP_WORKFLOW when testing against another API endpoint.
omop_source <- "cms_synthetic_patient_data_omop"
live_omop_con <- function() {
  workflow <- Sys.getenv("REDIVIS_TEST_OMOP_WORKFLOW", "imathews.omop_test:xf7r")
  redivis::redivis$workflow(workflow)$connect_dbi()
}
omop_person <- function(con) {
  dplyr::tbl(con, dbplyr::in_schema(omop_source, "person"))
}

test_that("workflow connections list and read datasource tables", {
  skip_if_no_live_api()
  con <- live_omop_con()

  tables <- DBI::dbListTables(con)
  expect_true("person" %in% tables)
  expect_true("condition_occurrence" %in% tables)

  expect_true("person_id" %in% DBI::dbListFields(con, "person"))
  expect_true(DBI::dbExistsTable(con, "person"))
  expect_false(DBI::dbExistsTable(con, "not_a_table"))

  n_bare <- dplyr::tbl(con, "person") |> dplyr::count() |> dplyr::pull(n)
  n_schema <- omop_person(con) |> dplyr::count() |> dplyr::pull(n)
  expect_gt(n_bare, 0)
  expect_equal(n_bare, n_schema)
})

test_that("translations run on Redivis", {
  skip_if_no_live_api()
  con <- live_omop_con()

  result <- omop_person(con) |>
    dplyr::filter(!is.na(year_of_birth), !is.na(month_of_birth)) |>
    head(1) |>
    dplyr::mutate(
      birth_date = date_build(year_of_birth, month_of_birth, 1),
      birth_datetime = as.POSIXct(birth_date),
      birth_year = year(birth_datetime),
      birth_wday = wday(birth_date),
      birth_month_start = floor_date(birth_date, "month"),
      plus_30 = add_days(birth_date, 30),
      days_to_2020 = date_count_between(birth_date, !!as.Date("2020-01-01"), "day"),
      built = date_build(2020, 1, 15),
      label = paste(person_id, gender_source_value, sep = "-"),
      padded = str_pad(as.character(month_of_birth), 2, pad = "0"),
      has_digit = str_detect(as.character(person_id), "\\d+"),
      replaced = str_replace("banana", "an", "X"),
      starts = str_starts(gender_source_value, "1|2"),
      rounded = round(year_of_birth / 7, 2),
      substring = str_sub("abcdef", 2),
      floor_div = -7L %/% 3L,
      mod_neg_dividend = -7L %% 3L,
      mod_neg_divisor = 7L %% -3L,
      mod_double = 7.5 %% 2,
      mod_precedence = 7L %% 3L * 2L,
      after_2000 = birth_datetime > !!as.POSIXct("2000-01-01", tz = "UTC"),
      today = today()
    ) |>
    dplyr::collect()

  expect_equal(nrow(result), 1)
  expect_equal(result$replaced, "bXana")
  expect_equal(result$substring, "bcdef")
  expect_equal(result$floor_div, -7L %/% 3L)
  expect_equal(result$mod_neg_dividend, -7L %% 3L)
  expect_equal(result$mod_neg_divisor, 7L %% -3L)
  expect_equal(result$mod_double, 7.5 %% 2)
  expect_equal(result$mod_precedence, 7L %% 3L * 2L)
  expect_true(result$has_digit)
  expect_equal(result$built, as.Date("2020-01-15"))
  expect_equal(result$plus_30, result$birth_date + 30)

  summary <- omop_person(con) |>
    dplyr::summarise(
      q25 = quantile(year_of_birth, 0.25),
      genders = str_flatten(gender_source_value, ",")
    ) |>
    dplyr::collect()
  expect_equal(nrow(summary), 1)
})
