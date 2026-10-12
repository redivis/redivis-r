test_that("dataset list", {
  skip_if_no_live_api()
  organization <- redivis::organization("Demo")
  datasets = organization$list_datasets(max_results=10)
  expect_equal(length(datasets), 10)
})

test_that("dataset tables list", {
  skip_if_no_live_api()
  organization <- redivis::organization("Demo")
  tables = organization$dataset('GHCN Daily Weather Data')$list_tables()
  expect_equal(length(tables), 5)
})

test_that("workflow tables list", {
  skip_if_no_live_api()
  user <- redivis::user("imathews")
  tables = user$workflow('example_workflow_climate_analysis')$list_tables()
  expect_equal(length(tables), 2)
})

test_that("dataset get", {
  skip_if_no_live_api()
  organization <- redivis::organization("Demo")
  dataset = organization$dataset('GHCN Daily Weather Data')$get()
  expect_true(!is.null(dataset$properties))
})

test_that("workflow get", {
  skip_if_no_live_api()
  user <- redivis::user("imathews")
  workflow = user$workflow('example_workflow_climate_analysis')$get()
  expect_true(!is.null(workflow$properties))
})
