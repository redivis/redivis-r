
test_that("streaming table works", {
  organization <- redivis::organization("Demo")
  dataset <- organization$dataset('Global Historical Climatology Network Daily Weather Data')
  table <- dataset$table("daily observations")
  results <- table$to_tibble(100)
  print(results)
  expect_equal(nrow(results), 100)
})

test_that("streaming table specific variables works", {
  organization <- redivis::organization("Demo")
  dataset <- organization$dataset('Global Historical Climatology Network Daily Weather Data')
  table <- dataset$table("daily observations")
  results <- table$to_tibble(100, variables=c('DATE', 'ID'))
  print(results)
  expect_equal(nrow(results), 100)
})
