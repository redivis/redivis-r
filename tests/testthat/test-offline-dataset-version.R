# Validation of the version argument when referencing a dataset. No API calls.

test_that("valid versions are normalized to a version tag", {
  cases <- list(
    c("current", "current"),
    c("next", "next"),
    c("v1.0", "v1.0"),
    c("v1_0", "v1_0"),
    c("V1.0", "V1.0"),
    c("1.0", "v1.0"),
    c("7_2", "v7_2"),
    c("13.4", "v13.4")
  )
  for (case in cases) {
    dataset <- redivis$organization("org")$dataset("dataset", version = case[1])
    expect_equal(dataset$version_tag, case[2])
    expect_equal(dataset$qualified_reference, paste0("org.dataset:", case[2]))
  }
})

test_that("invalid versions raise an error", {
  invalid <- c("v6", "8", "v", "vv1._", "foo", "7.3.1", "v1.0.0", "1.", "Current")
  for (version in invalid) {
    expect_error(
      redivis$organization("org")$dataset("dataset", version = version),
      "Invalid version",
      class = "redivis_value_error"
    )
  }
})

test_that("a version in the dataset name is still parsed", {
  dataset <- redivis$organization("org")$dataset("dataset:abc1:v2_1")
  expect_equal(dataset$version_tag, "v2_1")
  expect_equal(dataset$qualified_reference, "org.dataset:abc1:v2_1")
})

test_that("no version leaves the reference unversioned", {
  dataset <- redivis$organization("org")$dataset("dataset")
  expect_null(dataset$version_tag)
  expect_equal(dataset$qualified_reference, "org.dataset")
})
