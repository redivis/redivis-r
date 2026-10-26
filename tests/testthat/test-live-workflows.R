# Workflows, and the datasources, transforms, and parameters in them. Each test
# creates its own workflow, which is deleted once it finishes.

test_that("a workflow can be created, fetched, and deleted", {
  live <- live_api()
  workflow <- live$owner$workflow(live_unique_name("lifecycle"))
  withr::defer(
    tryCatch(workflow$delete(), redivis_not_found_error = function(e) NULL)
  )
  expect_false(workflow$exists())

  workflow$create(description = "A workflow")

  expect_true(workflow$exists())
  expect_equal(workflow$properties$kind, "workflow")
  expect_equal(workflow$properties$publicAccessLevel, "none")
  expect_true(startsWith(workflow$qualified_reference, paste0(live$owner$name, ".")))
  fetched <- redivis$workflow(workflow$qualified_reference)$get()
  expect_equal(fetched$properties$id, workflow$properties$id)
  expect_equal(fetched$name, workflow$name)
  expect_equal(
    fetched$properties$owner$kind,
    if (inherits(live$owner, "Organization")) "organization" else "user"
  )

  workflow$delete()

  expect_false(workflow$exists())
})

test_that("a workflow can be created as public", {
  workflow <- local_live_workflow(is_public = TRUE)

  expect_equal(workflow$get()$properties$publicAccessLevel, "view")
})

test_that("an owner lists the workflows it owns", {
  live <- live_api()
  workflow <- local_live_workflow()

  # Listings catch up with new workflows after a moment
  expect_true(eventually(function() {
    ids <- vapply(live$owner$list_workflows(), function(w) w$properties$id, character(1))
    workflow$properties$id %in% ids
  }))
})

test_that("a new workflow is empty", {
  workflow <- local_live_workflow()

  expect_length(workflow$list_tables(), 0)
  expect_length(workflow$list_tables(include_datasources = TRUE), 0)
  expect_length(workflow$list_datasources(), 0)
  expect_length(workflow$list_transforms(), 0)
  expect_length(workflow$list_notebooks(), 0)
  expect_length(workflow$list_parameters(), 0)
})

test_that("a missing workflow doesn't exist", {
  live <- live_api()

  expect_false(live$user$workflow(live_unique_name("missing"))$exists())
  expect_error(
    live$user$workflow(live_unique_name("missing"))$get(),
    class = "redivis_not_found_error"
  )
})

test_that("a query can be scoped to a workflow", {
  workflow <- local_live_workflow()

  df <- workflow$query("SELECT 1 AS x")$to_tibble(progress = FALSE)

  expect_equal(df$x, 1)
})

test_that("a dataset can be added to a workflow as a datasource, and removed", {
  workflow <- local_live_workflow()
  dataset <- live_sample_dataset()

  datasource <- workflow$datasource(dataset)$create()

  expect_true(datasource$exists())
  expect_equal(datasource$source_dataset()$properties$id, dataset$properties$id)
  expect_error(datasource$source_workflow(), class = "redivis_value_error")
  expect_equal(
    vapply(workflow$list_datasources(), function(d) d$uri, character(1)),
    datasource$uri
  )

  # The workflow's tables include those of its datasources
  tables <- workflow$list_tables(include_datasources = TRUE)
  expect_true("tiny" %in% vapply(tables, function(t) t$name, character(1)))

  datasource$delete()

  expect_false(datasource$exists())
  expect_length(workflow$list_datasources(), 0)
  expect_length(workflow$list_tables(include_datasources = TRUE), 0)
})

test_that("a datasource can be referenced by its source dataset", {
  workflow <- local_live_workflow()
  dataset <- live_sample_dataset()
  workflow$datasource(dataset)$create()

  datasource <- workflow$datasource(dataset$qualified_reference)

  expect_true(datasource$exists())
  datasource$delete()
  expect_false(datasource$exists())
})

test_that("removing a datasource removes the transforms that depend on it", {
  workflow <- local_live_workflow()
  datasource <- workflow$datasource(live_sample_dataset())$create()
  transform <- workflow$transform(live_unique_name("transform"))$create(
    source_table = live_tiny_table(),
    query = "SELECT id FROM _source_"
  )
  expect_true(transform$exists())

  datasource$delete()

  expect_false(transform$exists())
  expect_length(workflow$list_transforms(), 0)
})

test_that("a transform can be created, run, and deleted", {
  workflow <- local_live_workflow()
  tiny <- live_tiny_table()
  workflow$datasource(live_sample_dataset())$create()

  transform <- workflow$transform(live_unique_name("transform"))$create(
    source_table = tiny,
    query = "SELECT id, Key FROM _source_ WHERE Boolean"
  )
  expect_true(transform$exists())
  expect_equal(transform$source_table()$properties$id, tiny$properties$id)
  expect_equal(
    vapply(workflow$list_transforms(), function(t) t$name, character(1)),
    transform$name
  )

  transform$run()

  expect_equal(transform$properties$lastRunJob$status, "completed")
  expected <- tiny_expected()
  expect_rows_equal(
    transform$output_table()$to_tibble(progress = FALSE),
    expected[expected$Boolean, c("id", "Key")]
  )

  transform$delete()

  expect_false(transform$exists())
  expect_length(workflow$list_transforms(), 0)
})

test_that("a parameter can be created, updated, and deleted", {
  workflow <- local_live_workflow()
  name <- live_unique_name("param")

  parameter <- workflow$parameter(name)$create(type = "string", values = list("a", "b"))

  expect_true(parameter$exists())
  expect_equal(unlist(parameter$get_values()), c("a", "b"))
  expect_equal(
    vapply(workflow$list_parameters(), function(p) p$name, character(1)),
    name
  )

  parameter$update(values = list("c"))
  expect_equal(unlist(parameter$get_values()), "c")

  parameter$delete()
  expect_false(parameter$exists())
})

test_that("a workflow can be queried with DBI", {
  workflow <- local_live_workflow()
  con <- workflow$connect_dbi()
  withr::defer(DBI::dbDisconnect(con))

  expect_equal(DBI::dbGetQuery(con, "SELECT 1 AS x")$x, 1)
})
