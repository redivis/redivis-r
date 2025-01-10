
# test_that("querying works", {
#   expect_equal(2 * 2, 4)
# })
#
#
# test_that("scoped table works", {
#   Sys.setenv(REDIVIS_DEFAULT_PROJECT="imathews.ghcn precipitation")
#   table <- redivis::table(name="merge output")
#
#   results <- table$to_tibble(100)
#   print(results)
#   expect_equal(nrow(results), 100)
# })
#
# test_that("scoped querying works", {
#   Sys.setenv(REDIVIS_DEFAULT_WORKFLOW="imathews.ghcn precipitation")
#   # query <- redivis::user('imathews')$project('ghcn_precipitation')$query("SELECT * FROM merge_output LIMIT 100")
#   query <- redivis::query("SELECT * FROM merge_output LIMIT 100")
#
#   results <- query$to_tibble(100)
#   print(results)
#   expect_equal(nrow(results), 100)
# })

# test_that("temp", {
#   table <- redivis::user("imathews")$project("demo project")$table("Table 16")
#   # query = redivis::query("
#   #       SELECT * EXCEPT(__isUpload) FROM imathews.demo_project.nyc_taxis limit 10000
#   # ")
#   df = table$to_tibble(1000)
#
#   print(df)
# })
