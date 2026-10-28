# Reading with labelled = TRUE attaches variable and value labels, haven-style.
# Runs against the mock API in helper-mock-api.R.

VARIABLES <- "/api/v1/tables/a.b.mock/variables"

variables_queries <- function(mock) {
  Filter(function(r) r$path == VARIABLES, mock$requests()) |>
    lapply(function(r) r$query)
}

test_that("labelled = TRUE returns haven::labelled columns", {
  skip_if_not_installed("haven")
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(progress = FALSE, labelled = TRUE)

  expect_equal(variables_queries(mock)[[1]]$includeValueLabels, "true")

  expect_s3_class(df$id, "haven_labelled")
  expect_equal(attr(df$id, "label"), "Row id")
  expect_equal(attr(df$id, "labels"), c(first = 0L, second = 1L))
  expect_equal(as.numeric(df$id), 0:499)
  expect_equal(
    as.character(haven::as_factor(df$id)[1:3]),
    c("first", "second", "2")
  )

  expect_s3_class(df$name, "haven_labelled")
  expect_null(attr(df$name, "label", exact = TRUE))
  expect_equal(attr(df$name, "labels"), c("The first row" = "row-0"))
  expect_equal(sum(df$name == "row-0"), 1)

  # Only a variable label: a plain column with a label attribute
  expect_false(inherits(df$val, "haven_labelled"))
  expect_equal(attr(df$val, "label"), "Half the id")
})

test_that("labelled = TRUE works for data frames and data tables", {
  skip_if_not_installed("haven")
  local_mock_api()

  df <- mock_table()$to_data_frame(progress = FALSE, labelled = TRUE)
  expect_s3_class(df$id, "haven_labelled")

  dt <- mock_table()$to_data_table(progress = FALSE, labelled = TRUE)
  expect_s3_class(dt, "data.table")
  expect_s3_class(dt$id, "haven_labelled")
  expect_equal(attr(dt$val, "label"), "Half the id")
})

test_that("labelled = TRUE only labels the selected variables", {
  skip_if_not_installed("haven")
  local_mock_api()

  df <- mock_table()$to_tibble(
    progress = FALSE,
    variables = c("val", "id"),
    labelled = TRUE
  )

  expect_equal(names(df), c("val", "id"))
  expect_s3_class(df$id, "haven_labelled")
  expect_equal(attr(df$val, "label"), "Half the id")
})

test_that("reads without labelled don't request or attach labels", {
  mock <- local_mock_api()

  df <- mock_table()$to_tibble(progress = FALSE)

  expect_null(variables_queries(mock)[[1]]$includeValueLabels)
  expect_null(attributes(df$id))
  expect_null(attributes(df$val))
})

# Value labels as the API returns them: a list of {value, label}
value_labels <- function(...) {
  labels <- list(...)
  Map(
    function(value, label) list(value = value, label = label),
    names(labels),
    unname(labels),
    USE.NAMES = FALSE
  )
}

# Labels a single column x, as apply_variable_labels would for a data frame
label_column <- function(x, label, value_labels, as = tibble::tibble) {
  variables <- list(list(name = "x", label = label, valueLabels = value_labels))
  apply_variable_labels(as(x = x), variables)$x
}

test_that("labelled columns match haven::labelled for every output type", {
  skip_if_not_installed("haven")
  skip_if_not_installed("data.table")

  for (as in list(tibble::tibble, data.frame, data.table::data.table)) {
    expect_identical(
      label_column(c(1L, 2L), "Var", value_labels("1" = "one"), as = as),
      haven::labelled(c(1L, 2L), labels = c(one = 1L), label = "Var")
    )
    expect_identical(
      label_column(c("a", "b"), NULL, value_labels(a = "Apple"), as = as),
      haven::labelled(c("a", "b"), labels = c(Apple = "a"))
    )
    expect_identical(
      label_column(c(1, 2), "Var", NULL, as = as),
      structure(c(1, 2), label = "Var")
    )
  }
})

test_that("labelling a data.table doesn't modify other copies of a column", {
  skip_if_not_installed("haven")
  skip_if_not_installed("data.table")
  x <- c(1, 2)
  dt <- data.table::data.table(x = x)

  apply_variable_labels(dt, list(list(name = "x", label = "Var")))

  expect_equal(attr(dt$x, "label"), "Var")
  expect_null(attributes(x))
})

test_that("labels are converted to the column's type", {
  skip_if_not_installed("haven")
  skip_if_not_installed("data.table")

  # Non-integral labels on an integer column widen it to double
  for (as in list(tibble::tibble, data.table::data.table)) {
    x <- label_column(c(1L, 2L), NULL, value_labels("1.5" = "half"), as = as)
    expect_type(unclass(x), "double")
    expect_s3_class(x, "haven_labelled")
    expect_equal(attr(x, "labels"), c(half = 1.5))
  }

  # Labels that don't parse as numbers are dropped from numeric columns
  x <- label_column(
    c(1, 2),
    NULL,
    value_labels("1" = "one", "n/a" = "missing")
  )
  expect_equal(attr(x, "labels"), c(one = 1))

  # If none of them do, the column only gets the variable label
  x <- label_column(c(1, 2), "Var", value_labels("n/a" = "missing"))
  expect_false(inherits(x, "haven_labelled"))
  expect_null(attr(x, "labels"))
  expect_equal(attr(x, "label"), "Var")

  # Types haven can't label only get the variable label
  dates <- as.Date(c("2020-01-01", "2020-01-02"))
  x <- label_column(dates, "Date", value_labels("2020-01-01" = "New year"))
  expect_s3_class(x, "Date")
  expect_false(inherits(x, "haven_labelled"))
  expect_equal(attr(x, "label"), "Date")
})
