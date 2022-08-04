
# test_that("streaming table works", {
#   organization <- redivis::organization("Demo")
#   dataset <- organization$dataset('GHCN Daily Weather Data')
#   table <- dataset$table("daily observations")
#   count <- 10000
#   results <- table$to_tibble(count, variables=list('id'))
#   print(results)
#   expect_equal(nrow(results), count)
# })
#
# test_that("streaming table specific variables works", {
#   organization <- redivis::organization("Demo")
#   dataset <- organization$dataset('GHCN Daily Weather Data')
#   table <- dataset$table("daily observations")
#   results <- table$to_tibble(100, variables=c('DATE', 'ID'))
#   print(results)
#   expect_equal(nrow(results), 100)
# })
#
# test_that("geo data works", {
#   organization <- redivis::user("imathews")
#   dataset <- organization$dataset('A test dataset')
#   table <- dataset$table("a test table")
#   count <- 50
#   results <- table$to_tibble(count)
#   print(results)
#   expect_equal(nrow(results), count)
# })
