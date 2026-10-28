# Table and variable metadata

test_that("a table can be created", {
  table <- local_live_table(
    description = "A table",
    upload_merge_strategy = "replace"
  )

  expect_true(table$exists())
  expect_equal(table$properties$description, "A table")
  expect_equal(table$properties$uploadMergeStrategy, "replace")
  expect_false(table$properties$isFileIndex)
  expect_equal(table$properties$numRows, 0)
})

test_that("a table can be updated", {
  table <- local_live_table()
  new_name <- paste0(table$name, "_renamed")

  table$update(
    name = new_name,
    description = "Updated",
    upload_merge_strategy = "replace"
  )

  expect_equal(table$name, new_name)
  fetched <- table$get()
  expect_equal(fetched$properties$description, "Updated")
  expect_equal(fetched$properties$uploadMergeStrategy, "replace")
})

test_that("a table can be deleted", {
  table <- live_scratch_dataset()$table(live_unique_name("delete"))$create()

  table$delete()

  expect_false(table$exists())
})

test_that("a table can be fetched by its qualified reference", {
  tiny <- live_tiny_table()

  table <- redivis$table(tiny$qualified_reference)$get()

  expect_equal(table$properties$id, tiny$properties$id)
  expect_equal(table$properties$numRows, nrow(tiny_expected()))
  expect_equal(table$properties$container$kind, "dataset")
})

test_that("a table lists its uploads", {
  uploads <- live_tiny_table()$list_uploads()

  expect_length(uploads, 1)
  expect_equal(uploads[[1]]$name, "tiny.csv")
  expect_equal(uploads[[1]]$properties$status, "completed")
})

test_that("a table lists its variables", {
  variables <- live_tiny_table()$list_variables()

  expect_equal(variable_types(variables), tiny_variable_types)
  expect_length(live_tiny_table()$list_variables(max_results = 2), 2)
})

test_that("each type of variable is detected", {
  expect_equal(
    variable_types(live_typed_table()$list_variables()),
    typed_variable_types
  )
})

test_that("a variable has statistics", {
  expected <- tiny_expected()$Key

  statistics <- live_tiny_table()$variable("Key")$get_statistics()

  expect_equal(statistics$status, "completed")
  expect_equal(statistics$count, length(expected))
  expect_equal(statistics$numDistinct, length(unique(expected)))
})

test_that("a variable can be updated", {
  table <- local_live_table()
  table$upload("data.csv")$create(charToRaw("code,value\na,1\nb,2\n"), progress = FALSE)

  variable <- table$variable("code")$update(
    label = "A code",
    description = "Identifies the row",
    value_labels = list(list(value = "a", label = "The letter a"))
  )

  expect_equal(variable$properties$label, "A code")
  fetched <- table$variable("code")$get()
  expect_equal(fetched$properties$description, "Identifies the row")
  expect_equal(
    fetched$properties$valueLabels,
    list(list(value = "a", label = "The letter a"))
  )
})

test_that("a table's variables can be updated in bulk", {
  table <- local_live_table()
  table$upload("data.csv")$create(charToRaw("code,value\na,1\nb,2\n"), progress = FALSE)

  table$update_variables(list(
    list(name = "code", label = "Code label"),
    list(name = "value", description = "Value description")
  ))

  expect_equal(table$variable("code")$get()$properties$label, "Code label")
  expect_equal(
    table$variable("value")$get()$properties$description,
    "Value description"
  )
})

for (name in c("Key", "key", "KEY")) {
  test_that(sprintf("variables are looked up case-insensitively (%s)", name), {
    expect_equal(live_tiny_table()$variable(name)$get()$properties$name, "Key")
  })
}
