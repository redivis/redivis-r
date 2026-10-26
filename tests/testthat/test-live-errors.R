# How errors from the API surface to callers

test_that("a missing dataset doesn't exist, and can't be fetched", {
  live <- live_api()
  dataset <- live$owner$dataset(live_unique_name("missing"))

  expect_false(dataset$exists())
  error <- expect_error(dataset$get(), class = "redivis_not_found_error")
  expect_equal(error$status_code, 404)
})

test_that("a missing table doesn't exist, and can't be fetched", {
  table <- live_sample_dataset()$table("does_not_exist")

  expect_false(table$exists())
  expect_error(table$get(), class = "redivis_not_found_error")
})

test_that("a missing variable doesn't exist, and can't be fetched", {
  variable <- live_tiny_table()$variable("does_not_exist")

  expect_false(variable$exists())
  expect_error(variable$get(), class = "redivis_not_found_error")
})

test_that("an unknown endpoint is a not found error", {
  live_api()

  expect_error(
    redivis$make_api_request(path = "/not_an_endpoint"),
    class = "redivis_not_found_error"
  )
})

test_that("an invalid request is an API error", {
  error <- expect_error(
    live_scratch_dataset()$table("")$create(),
    class = "redivis_api_error"
  )

  expect_gte(error$status_code, 400)
  expect_lt(error$status_code, 500)
})
