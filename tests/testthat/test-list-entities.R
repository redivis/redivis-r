
test_that("dataset list", {
  organization <- redivis::organization("Demo")
  datasets = organization$list_datasets(max_results=10)
  expect_equal(length(datasets), 10)
})

test_that("dataset tables list", {
  organization <- redivis::organization("Demo")
  tables = organization$dataset('GHCN Daily Weather Data')$list_tables()
  expect_equal(length(tables), 5)
})

test_that("project tables list", {
  user <- redivis::user("imathews")
  tables = user$project('example_project_climate_analysis')$list_tables()
  expect_equal(length(tables), 2)
})

test_that("dataset get", {
  organization <- redivis::organization("Demo")
  dataset = organization$dataset('GHCN Daily Weather Data')$get()
  expect_true(!is.null(dataset$properties))
})

test_that("project get", {
  user <- redivis::user("imathews")
  project = user$project('example_project_climate_analysis')$get()
  expect_true(!is.null(project$properties))
})
