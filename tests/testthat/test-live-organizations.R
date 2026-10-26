# Organizations, and the datasets and workflows they own

test_that("an organization exists", {
  organization <- live_test_organization()

  expect_true(organization$exists())
  expect_false(redivis$organization(live_unique_name("missing"))$exists())
})

test_that("an organization lists its datasets", {
  organization <- live_test_organization()

  datasets <- organization$list_datasets(max_results = 5)

  expect_lte(length(datasets), 5)
  for (dataset in datasets) {
    expect_s3_class(dataset, "Dataset")
    expect_equal(dataset$organization$name, organization$name)
    expect_equal(dataset$properties$owner$kind, "organization")
  }
})

test_that("an organization's datasets can be created and found by label", {
  organization <- live_test_organization()
  live <- live_api()
  if (!identical(live$owner$name, organization$name)) {
    skip("needs the test datasets to be owned by the organization")
  }
  # Labels are limited to 32 characters
  label <- paste0("rtest_", substr(gsub("-", "", uuid::UUIDgenerate()), 1, 12))
  # Only released datasets are listed
  dataset <- local_live_dataset()
  dataset$update(labels = list(label))
  add_small_table(dataset)
  dataset$release()

  labelled_ids <- function() {
    datasets <- organization$list_datasets()
    labelled <- Filter(function(d) label %in% unlist(d$properties$labels), datasets)
    vapply(labelled, function(d) d$properties$id, character(1))
  }

  # Listings catch up with new datasets after a moment
  expect_true(eventually(function() identical(labelled_ids(), dataset$properties$id)))
})

test_that("an organization lists its workflows", {
  organization <- live_test_organization()

  workflows <- organization$list_workflows(max_results = 3)

  expect_lte(length(workflows), 3)
  for (workflow in workflows) {
    expect_s3_class(workflow, "Workflow")
    expect_equal(workflow$organization$name, organization$name)
  }
})
