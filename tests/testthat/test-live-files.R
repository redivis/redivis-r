# Raw files in file index tables: uploading, listing, reading, and downloading
# them

test_that("a file index table lists its files", {
  files <- live_files_table()$list_files()
  expected <- raw_files()

  paths <- vapply(files, function(f) f$path, character(1))
  expect_setequal(paths, names(expected))
  for (file in files) {
    content <- expected[[file$path]]
    expect_equal(file$size, length(content), label = file$path)
    expect_equal(file$hash, as.raw(openssl::md5(content)), label = file$path)
  }
})

test_that("listing files can be limited", {
  expect_length(live_files_table()$list_files(2), 2)
})

test_that("files are arranged in directories", {
  root <- live_files_table()$to_directory()
  names_of <- function(nodes) sort(vapply(nodes, function(n) n$name, character(1)))

  expect_equal(
    names_of(root$list()),
    sort(c("data", "readme.txt", "with space & symbols.txt"))
  )
  expect_equal(
    names_of(root$list(mode = "directories", recursive = TRUE)),
    c("data", "nested")
  )
  nested <- root$get("data/nested")
  expect_s3_class(nested, "Directory")
  expect_equal(names_of(nested$list()), "random.bin")
  expect_equal(nested$get("../numbers.csv")$name, "numbers.csv")
  expect_null(root$get("data/missing.txt"))
})

for (path in names(raw_files())) {
  test_that(sprintf("a file can be read (%s)", path), {
    expect_equal(live_files_table()$file(path)$read(), raw_files()[[path]])
  })
}

test_that("a file can be read as text", {
  expected <- rawToChar(raw_files()[["readme.txt"]])
  Encoding(expected) <- "UTF-8"

  expect_equal(live_files_table()$file("readme.txt")$read(as_text = TRUE), expected)
})

test_that("a byte range of a file can be read", {
  content <- raw_files()[["data/nested/random.bin"]]
  file <- live_files_table()$file("data/nested/random.bin")

  expect_equal(file$read(start_byte = 100, end_byte = 199), content[101:200])
  expect_equal(
    file$read(start_byte = length(content) - 10),
    utils::tail(content, 10)
  )
})

test_that("a missing file or a directory can't be read", {
  table <- live_files_table()

  expect_error(table$file("missing.txt"), class = "redivis_not_found_error")
  expect_error(table$file("data"), "is a directory", class = "redivis_value_error")
})

test_that("a file can be opened as a text connection", {
  skip_if_not(native_available(), "the native component isn't loaded")
  con <- open(live_files_table()$file("data/numbers.csv"))
  withr::defer(close(con))

  expect_equal(
    readLines(con),
    strsplit(rawToChar(raw_files()[["data/numbers.csv"]]), "\n")[[1]]
  )
})

test_that("a file can be opened as a binary connection, and read in chunks", {
  skip_if_not(native_available(), "the native component isn't loaded")
  content <- raw_files()[["data/nested/random.bin"]]
  con <- live_files_table()$file("data/nested/random.bin")$open("rb")
  withr::defer(close(con))

  chunks <- list()
  repeat {
    chunk <- readBin(con, "raw", 100000)
    if (length(chunk) == 0) break
    chunks[[length(chunks) + 1]] <- chunk
  }

  expect_equal(do.call(c, chunks), content)
})

test_that("a file can be downloaded to a directory or a path", {
  file <- live_files_table()$file("data/nested/random.bin")
  directory <- withr::local_tempdir()

  file$download(paste0(directory, "/"), progress = FALSE)
  expect_equal(
    read_files(directory)[["random.bin"]],
    raw_files()[["data/nested/random.bin"]]
  )

  target <- file.path(directory, "renamed.bin")
  file$download(target, progress = FALSE)
  expect_equal(
    readBin(target, "raw", file.size(target)),
    raw_files()[["data/nested/random.bin"]]
  )
})

test_that("a directory can be downloaded", {
  directory <- withr::local_tempdir()

  live_files_table()$to_directory()$download(directory, progress = FALSE)

  downloaded <- read_files(directory)
  expect_equal(downloaded[order(names(downloaded))], raw_files()[order(names(raw_files()))])
})

test_that("a subdirectory can be downloaded", {
  directory <- withr::local_tempdir()

  live_files_table()$to_directory()$get("data")$download(directory, progress = FALSE)

  expect_setequal(names(read_files(directory)), c("nested/random.bin", "numbers.csv"))
})

test_that("files can be added from memory", {
  table <- local_live_table(is_file_index = TRUE)

  table$add_files(
    files = list(
      list(name = "a.txt", data = "text content"),
      list(name = "dir/b.bin", data = as.raw(0:2))
    ),
    progress = FALSE
  )

  expect_equal(table$file("a.txt")$read(), charToRaw("text content"))
  expect_equal(table$file("dir/b.bin")$read(), as.raw(0:2))
})

test_that("files can be added from paths", {
  table <- local_live_table(is_file_index = TRUE)
  path <- file.path(withr::local_tempdir(), "local.txt")
  writeBin(charToRaw("from disk"), path)

  table$add_files(
    files = list(path, list(path = path, name = "copy.txt")),
    progress = FALSE
  )

  expect_setequal(
    vapply(table$list_files(), function(f) f$path, character(1)),
    c("copy.txt", "local.txt")
  )
  expect_equal(table$get()$properties$numRows, 2)
})

test_that("adding files needs either files or a directory", {
  table <- local_live_table(is_file_index = TRUE)

  expect_error(table$add_files(progress = FALSE), class = "redivis_value_error")
})
