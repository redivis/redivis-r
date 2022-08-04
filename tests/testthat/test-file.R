#
# test_that("file download works", {
#   file <- redivis::file("s335-8ey8zt7bx.qKmzpdttY2ZcaLB0wbRB7A")
#   file$download("/Users/ian/Desktop/tmp", overwrite=TRUE)
# })
#
#
# test_that("file read works", {
#   file <- redivis::file("s335-8ey8zt7bx.qmY2-g62geqJrZhPlmJElw")
#   data <- file$read(as_text=TRUE)
# })
#
#
# test_that("file stream works", {
#   file <- redivis::file("s335-8ey8zt7bx.qmY2-g62geqJrZhPlmJElw")
#   data <- file$stream(function(x) {
#     print(length(x))
#   })
# })

test_that("table download works", {
  table <- redivis::user("demo")$dataset("Example non-tabular files")$table("tiffs")
  table$download_files("/Users/ian/Desktop/tmp/test", overwrite=TRUE)
})
