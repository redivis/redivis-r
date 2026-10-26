# Upload options: parsing, metadata, merge strategies, name conflicts, and
# failures

test_that("quoted newlines are kept", {
  table <- local_live_table()
  content <- charToRaw(
    'id,text\n1,"first\nsecond"\n2,"a ""quoted"" word"\n3,plain\n'
  )

  table$upload("newlines.csv")$create(
    content,
    has_quoted_newlines = TRUE,
    progress = FALSE
  )

  expect_equal(
    sorted_by(table$to_tibble(progress = FALSE))$text,
    c("first\nsecond", 'a "quoted" word', "plain")
  )
})

test_that("delimited files can be parsed with options", {
  table <- local_live_table()

  table$upload("data.psv")$create(
    charToRaw("1|a|NA\n2|NA|2.5\n"),
    type = "delimited",
    delimiter = "|",
    has_header_row = FALSE,
    null_markers = list("NA"),
    progress = FALSE
  )

  variables <- table$list_variables()
  expect_length(variables, 3)
  df <- sorted_by(table$to_tibble(progress = FALSE), variables[[1]]$name)
  expect_equal(is.na(df[[2]]), c(FALSE, TRUE))
  expect_equal(is.na(df[[3]]), c(TRUE, FALSE))
})

test_that("metadata sets the variables' metadata", {
  table <- local_live_table()

  table$upload("small.csv")$create(
    small_csv,
    metadata = list(
      name = list(
        label = "Name label",
        description = "Name description",
        valueLabels = list(a = "The letter a")
      )
    ),
    progress = FALSE
  )

  variable <- table$variable("name")$get()
  expect_equal(variable$properties$label, "Name label")
  expect_equal(variable$properties$description, "Name description")
  expect_equal(
    variable$properties$valueLabels,
    list(list(value = "a", label = "The letter a"))
  )
})

test_that("a version's uploads are all kept", {
  table <- local_live_table()

  table$upload("first.csv")$create(small_csv, progress = FALSE)
  table$upload("second.csv")$create(small_csv, progress = FALSE)

  expect_equal(table$get()$properties$numRows, 6)
  expect_setequal(
    vapply(table$list_uploads(), function(u) u$name, character(1)),
    c("first.csv", "second.csv")
  )
  expect_equal(sort(table$to_tibble(progress = FALSE)$id), c(1, 1, 2, 2, 3, 3))
})

# Uploads one version of a table, releases it, and uploads to the next version
upload_two_versions <- function(upload_merge_strategy) {
  dataset <- local_live_dataset(.local_envir = parent.frame())
  table <- dataset$table("t")$create(upload_merge_strategy = upload_merge_strategy)
  table$upload("first.csv")$create(small_csv, progress = FALSE)
  dataset$release()

  next_table <- dataset$create_next_version()$table("t")
  next_table$upload("second.csv")$create(
    charToRaw("id,name,value\n9,z,9.5\n"),
    progress = FALSE
  )
  next_table
}

test_that("an append table's next version adds to its rows", {
  table <- upload_two_versions("append")

  expect_equal(sort(table$to_tibble(progress = FALSE)$id), c(1, 2, 3, 9))
})

test_that("a replace table's next version replaces its rows", {
  table <- upload_two_versions("replace")

  expect_equal(table$to_tibble(progress = FALSE)$id, 9)
})

test_that("an upload's name can conflict with an existing one", {
  table <- local_live_table()
  original <- table$upload("data.csv")$create(small_csv, progress = FALSE)

  expect_error(
    table$upload("data.csv")$create(small_csv, progress = FALSE),
    "already exists",
    class = "redivis_value_error"
  )

  existing <- table$upload("data.csv")$create(
    small_csv,
    if_not_exists = TRUE,
    progress = FALSE
  )
  expect_equal(existing$uri, original$uri)
  expect_length(table$list_uploads(), 1)

  renamed <- table$upload("data.csv")$create(
    small_csv,
    rename_on_conflict = TRUE,
    progress = FALSE
  )
  expect_false(identical(renamed$properties$name, "data.csv"))
  expect_length(table$list_uploads(), 2)

  table$upload("data.csv")$create(
    charToRaw("id,name,value\n9,z,9.5\n"),
    replace_on_conflict = TRUE,
    progress = FALSE
  )
  expect_length(table$list_uploads(), 2)
  expect_equal(table$get()$properties$numRows, 4)

  expect_error(
    table$upload("data.csv")$create(
      small_csv,
      rename_on_conflict = TRUE,
      replace_on_conflict = TRUE
    ),
    class = "redivis_value_error"
  )
})

test_that("a failed upload raises a job error", {
  table <- local_live_table()

  expect_error(
    table$upload("jagged.csv")$create(jagged_csv, progress = FALSE),
    class = "redivis_job_error"
  )
  expect_true(table$upload("jagged.csv")$exists())
})

test_that("a failed upload can be removed, without raising", {
  table <- local_live_table()

  upload <- table$upload("jagged.csv")$create(
    jagged_csv,
    remove_on_fail = TRUE,
    raise_on_fail = FALSE,
    progress = FALSE
  )

  expect_equal(upload$properties$status, "failed")
  expect_false(upload$exists())
})

test_that("jagged rows can be allowed", {
  table <- local_live_table()

  table$upload("jagged.csv")$create(
    jagged_csv,
    allow_jagged_rows = TRUE,
    progress = FALSE
  )

  df <- sorted_by(table$to_tibble(progress = FALSE), "a")
  expect_equal(df$a, c(1, 3, 4))
  expect_equal(is.na(df$b), c(FALSE, TRUE, FALSE))
})

test_that("bad records can be skipped", {
  table <- local_live_table()

  table$upload("jagged.csv")$create(
    jagged_csv,
    skip_bad_records = TRUE,
    progress = FALSE
  )

  expect_equal(sort(table$to_tibble(progress = FALSE)$a), c(1, 4))
})

test_that("an upload can be fetched, inspected, and deleted", {
  table <- local_live_table()
  upload <- table$upload("small.csv")
  expect_false(upload$exists())

  upload$create(small_csv, progress = FALSE)

  expect_true(upload$exists())
  expect_equal(upload$get()$properties$numRows, 3)
  expect_equal(
    names(variable_types(upload$list_variables())),
    c("id", "name", "value")
  )
  expect_equal(upload$variable("name")$get()$properties$type, "string")

  upload$delete()
  expect_false(upload$exists())
  expect_equal(table$get()$properties$numRows, 0)
})
