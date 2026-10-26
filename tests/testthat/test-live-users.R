# The authenticated user, and listing what users and organizations own

test_that("current_user() is the authenticated user", {
  live <- live_api()

  user <- redivis$current_user()

  expect_s3_class(user, "User")
  expect_equal(user$name, live$user$name)
})

test_that("a user lists their datasets", {
  live <- live_api()

  datasets <- live$user$list_datasets(max_results = 3)

  expect_lte(length(datasets), 3)
  for (dataset in datasets) {
    expect_s3_class(dataset, "Dataset")
    expect_equal(dataset$properties$kind, "dataset")
    expect_true(startsWith(dataset$qualified_reference, paste0(live$user$name, ".")))
  }
})

test_that("a user lists their workflows", {
  live <- live_api()

  workflows <- live$user$list_workflows(max_results = 3)

  expect_lte(length(workflows), 3)
  for (workflow in workflows) {
    expect_s3_class(workflow, "Workflow")
    expect_true(startsWith(workflow$qualified_reference, paste0(live$user$name, ".")))
  }
  expect_warning(live$user$list_projects(max_results = 1), "renamed")
})

test_that("an owner lists the datasets it owns", {
  live <- live_api()
  # Only released datasets are listed
  dataset <- local_live_dataset()
  add_small_table(dataset)
  dataset$release()

  # Listings catch up with new datasets after a moment
  expect_true(eventually(function() {
    ids <- vapply(live$owner$list_datasets(), function(d) d$properties$id, character(1))
    dataset$properties$id %in% ids
  }))
})
