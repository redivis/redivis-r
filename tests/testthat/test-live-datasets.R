# Creating, updating, versioning, and deleting datasets

test_that("a dataset can be created", {
  live <- live_api()

  dataset <- local_live_dataset(description = "A description")

  expect_true(dataset$exists())
  expect_equal(dataset$properties$description, "A description")
  expect_equal(dataset$properties$publicAccessLevel, "none")
  expect_true(startsWith(dataset$qualified_reference, paste0(live$owner$name, ".")))
  # A new dataset starts out with an unreleased version, which it references
  expect_equal(dataset$version_tag, "next")
  expect_false(dataset$properties$version$isReleased)
})

test_that("a dataset can be fetched by its reference", {
  dataset <- local_live_dataset()

  fetched <- redivis$dataset(dataset$qualified_reference)$get()

  expect_equal(fetched$properties$id, dataset$properties$id)
  expect_equal(fetched$name, dataset$name)
})

test_that("a dataset can be updated", {
  dataset <- local_live_dataset()

  dataset$update(
    description = "Updated",
    methodology_markdown = "# Methodology",
    usage_markdown = "Usage notes",
    labels_to_add = c("rtest", "other"),
    public_access_level = "overview"
  )
  expect_equal(dataset$properties$description, "Updated")
  expect_equal(dataset$properties$methodologyMarkdown, "# Methodology")
  expect_equal(dataset$properties$usageMarkdown, "Usage notes")
  expect_setequal(unlist(dataset$properties$labels), c("rtest", "other"))
  expect_equal(dataset$properties$publicAccessLevel, "overview")

  dataset$update(labels_to_remove = list("other"))
  expect_equal(unlist(dataset$properties$labels), "rtest")

  # Fetching it again reflects what was saved
  expect_equal(dataset$get()$properties$description, "Updated")
})

test_that("labels can be added and removed with the deprecated methods", {
  dataset <- local_live_dataset()

  expect_warning(dataset$add_labels(list("a", "b")), "deprecated")
  expect_warning(dataset$remove_labels(list("a")), "deprecated")

  expect_equal(unlist(dataset$get()$properties$labels), "b")
})

test_that("a dataset can be renamed", {
  dataset <- local_live_dataset()
  new_name <- paste0(dataset$name, "_renamed")

  dataset$update(name = new_name)

  expect_equal(dataset$name, new_name)
  expect_match(dataset$qualified_reference, new_name, fixed = TRUE)
  expect_equal(dataset$get()$name, new_name)
})

test_that("a dataset can be deleted", {
  live <- live_api()
  dataset <- live$owner$dataset(live_unique_name("delete"))$create()
  expect_true(dataset$exists())

  dataset$delete()

  expect_false(dataset$exists())
})

test_that("a next version can't be created when one exists", {
  dataset <- local_live_dataset()

  expect_error(
    dataset$create_next_version(),
    "already exists",
    class = "redivis_value_error"
  )

  next_version <- dataset$create_next_version(if_not_exists = TRUE)
  expect_equal(next_version$version_tag, "next")
  expect_equal(next_version$properties$id, dataset$properties$id)
})

test_that("a dataset can be released, and its versions listed", {
  dataset <- local_live_dataset()
  add_small_table(dataset)

  # The next version can't be released until this one has finished being released
  dataset$release(release_notes = "First release", wait_for_finalization = TRUE)

  expect_equal(dataset$version_tag, "v1_0")
  expect_true(dataset$properties$version$isReleased)
  version <- dataset$version()$get()
  expect_equal(version$properties$tag, "v1.0")
  expect_equal(version$properties$releaseNotes, "First release")
  expect_false(version$properties$isFinalizing)
  expect_null(dataset$previous_version())

  next_version <- dataset$create_next_version()
  expect_equal(next_version$version_tag, "next")
  # A version needs changes to be released. Adding rows is a minor change.
  next_version$table("t")$upload("more.csv")$create(small_csv, progress = FALSE)
  next_version$release(wait_for_finalization = TRUE)
  expect_equal(next_version$version_tag, "v1_1")

  tags <- vapply(next_version$list_versions(), function(v) v$properties$tag, character(1))
  expect_true(all(c("v1.0", "v1.1") %in% tags))
  expect_equal(next_version$previous_version()$version_tag, "v1_0")
  expect_equal(dataset$next_version()$version_tag, "v1_1")

  # Released versions remain readable by their tags
  reference <- sub(":[^:]*$", "", dataset$qualified_reference)
  expect_true(redivis$dataset(paste0(reference, ":v1.0"))$exists())
  expect_true(redivis$dataset(reference, version = "1.0")$exists())

  # A historic version's data is removed in the background
  first_version <- Find(
    function(v) identical(v$properties$tag, "v1.0"),
    next_version$list_versions()
  )
  first_version$delete(wait_for_finalization = TRUE)
  expect_true(first_version$properties$isDeleted)
  expect_false(first_version$properties$isFinalizing)
})

test_that("a dataset can be unreleased", {
  dataset <- local_live_dataset()
  add_small_table(dataset)
  dataset$release(wait_for_finalization = TRUE)

  dataset$unrelease(wait_for_finalization = TRUE)
  expect_equal(dataset$version_tag, "next")
  version <- dataset$version()$get()
  expect_false(version$properties$isReleased)
  expect_false(version$properties$isFinalizing)
})

test_that("a version can be updated", {
  dataset <- local_live_dataset()
  add_small_table(dataset)
  dataset$release()

  version <- dataset$version()$update(label = "Initial", release_notes = "Notes")

  expect_equal(version$properties$label, "Initial")
  expect_equal(version$get()$properties$releaseNotes, "Notes")
})

test_that("a dataset lists its tables", {
  dataset <- local_live_dataset()
  dataset$table("first")$create()
  dataset$table("second")$create(is_file_index = TRUE)

  tables <- dataset$list_tables()

  expect_setequal(vapply(tables, function(t) t$name, character(1)), c("first", "second"))
  for (table in tables) {
    expect_s3_class(table, "Table")
  }
})

test_that("a dataset's variables can be updated in bulk", {
  dataset <- local_live_dataset()
  table <- add_small_table(dataset)

  dataset$update_variables(list(
    list(tableName = "t", name = "name", label = "Name label")
  ))

  expect_equal(table$variable("name")$get()$properties$label, "Name label")
})
