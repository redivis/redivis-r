
test_that("querying works", {
  expect_equal(2 * 2, 4)
})


test_that("scoped querying works", {
  Sys.setenv(REDIVIS_DEFAULT_PROJECT="imathews.ghcn precipitation")
  table <- redivis::table(name="merge output")

  results <- table$to_tibble(100)
  print(results)
  expect_equal(nrow(results), 100)
})
