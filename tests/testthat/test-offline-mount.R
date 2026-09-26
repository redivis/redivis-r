# Reads through a mounted directory's cache: cached locally in blocks, with
# sequential reads streamed in one request. These drive the C code directly,
# via C_fuse_cache_open, so they need no FUSE installation. Runs against the
# mock API in helper-mock-api.R.

BLOCK <- 1024
# Kernel reads are much smaller than a block
READ <- 256
SIZE <- 10 * BLOCK + 123

# The bytes the mock API serves for a file (see raw_file_contents in
# helper-mock-api.R)
file_bytes <- function(content, size = SIZE) {
  withr::with_seed(
    sum(utf8ToInt(content)),
    as.raw(sample.int(256, size, replace = TRUE) - 1L)
  )
}

cache_key <- function(content, size = SIZE) {
  digest::digest(file_bytes(content, size), algo = "md5", serialize = FALSE)
}

# The cache of a directory of files, given as list(name = list(content, size)),
# without mounting it
local_mount_cache <- function(
  mock,
  files = list(data.bin = list(content = "data")),
  cache_dir = withr::local_tempdir(.local_envir = env),
  max_cache_size = NULL,
  temporary_cache = FALSE,
  auth_token = get_auth_token(),
  env = parent.frame()
) {
  table <- mock_table()
  root <- Directory$new(path = "/", table = table)
  raw_files <- list()
  for (name in names(files)) {
    size <- files[[name]]$size %||% SIZE
    id <- paste0("id-", name)
    raw_files[[id]] <- list(content = files[[name]]$content, size = size)
    md5 <- digest::digest(
      file_bytes(files[[name]]$content, size),
      algo = "md5",
      serialize = FALSE,
      raw = TRUE
    )
    file <- File$new(
      id = id,
      name = name,
      table = table,
      directory = root,
      properties = list(size = size, md5_hash = jsonlite::base64_enc(md5))
    )
    assign(name, file, envir = root$children)
  }
  mock$set(raw_files = raw_files)

  manifest <- fuse_manifest(root)
  .Call(
    "C_fuse_cache_open",
    cache_dir,
    temporary_cache,
    if (is.null(max_cache_size)) NA_real_ else as.double(max_cache_size),
    manifest$rel_paths,
    manifest$sizes,
    manifest$file_ids,
    manifest$keys,
    generate_api_url(""),
    auth_token,
    FALSE,
    as.double(BLOCK),
    PACKAGE = "redivis"
  )
}

cache_file <- function(cache, path, op, offset = 0, length = 0) {
  .Call("C_fuse_cache_file", cache, path, op, offset, length, PACKAGE = "redivis")
}

# Reads through a handle of its own, unless open = FALSE
cache_read <- function(cache, offset, length, path = "data.bin", open = TRUE) {
  if (open) {
    cache_file(cache, path, "open")
    on.exit(cache_file(cache, path, "release"))
  }
  cache_file(cache, path, "read", offset, length)
}

cache_read_all <- function(cache, start = 0, path = "data.bin") {
  cache_file(cache, path, "open")
  on.exit(cache_file(cache, path, "release"))
  chunks <- list()
  offset <- start
  repeat {
    chunk <- cache_file(cache, path, "read", offset, READ)
    if (length(chunk) == 0) {
      break
    }
    chunks[[length(chunks) + 1]] <- chunk
    offset <- offset + length(chunk)
  }
  do.call(c, c(list(raw(0)), chunks))
}

cached_names <- function(cache_dir) {
  sort(list.files(cache_dir))
}

cached_copy <- function(content) {
  paste0(cache_key(content), c(".blocks", ".data"))
}

test_that("a sequential read is a single request", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)

  expect_identical(cache_read_all(cache), file_bytes("data"))
  expect_equal(mock$raw_file_ranges(), "bytes=0-")
})

test_that("a sequential read resumes exactly where a cut-short response ended", {
  mock <- local_mock_api()
  mock$set(truncate_raw_file_once = 3 * BLOCK + 10)
  cache <- local_mount_cache(mock)

  expect_identical(cache_read_all(cache), file_bytes("data"))
  expect_equal(mock$raw_file_ranges(), c("bytes=0-", "bytes=3082-"))
})

test_that("a transient failure is retried", {
  mock <- local_mock_api()
  mock$set(fail_503 = 1)
  cache <- local_mount_cache(mock)

  expect_identical(cache_read_all(cache), file_bytes("data"))
  expect_length(mock$paths("/rawFiles/"), 2)
})

test_that("a client error fails the read without retrying", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)
  mock$set(response_script = list(list(status = 403, body = list(error = "forbidden"))))

  expect_error(cache_read(cache, 0, READ), "read failed")
  expect_length(mock$paths("/rawFiles/"), 1)
})

test_that("a reread is served from the cache", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)
  cache_read_all(cache)

  expect_identical(
    cache_read(cache, 5 * BLOCK + 7, 300),
    file_bytes("data")[5 * BLOCK + 7 + seq_len(300)]
  )
  expect_identical(cache_read_all(cache), file_bytes("data"))
  expect_length(mock$raw_file_ranges(), 1)
})

test_that("random reads fetch only the blocks they need", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)

  # e.g. a parquet reader, starting with the footer
  expect_identical(cache_read(cache, SIZE - 50, 50), tail(file_bytes("data"), 50))
  # A read straddling two blocks
  expect_identical(
    cache_read(cache, 4 * BLOCK - 10, 20),
    file_bytes("data")[4 * BLOCK - 10 + seq_len(20)]
  )
  expect_equal(
    mock$raw_file_ranges(),
    c("bytes=10240-10362", "bytes=3072-5119")
  )
})

test_that("a server ignoring the range is read from the right place", {
  mock <- local_mock_api()
  mock$set(ignore_raw_file_range = TRUE)
  cache <- local_mount_cache(mock)

  expect_identical(
    cache_read(cache, 4 * BLOCK - 10, 20),
    file_bytes("data")[4 * BLOCK - 10 + seq_len(20)]
  )
  expect_identical(cache_read_all(cache), file_bytes("data"))
})

test_that("a read continuing from cached data streams the rest", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)
  cache_read(cache, 2 * BLOCK, READ)

  expect_identical(
    cache_read_all(cache, start = 2 * BLOCK),
    file_bytes("data")[-seq_len(2 * BLOCK)]
  )
  expect_equal(mock$raw_file_ranges(), c("bytes=2048-3071", "bytes=3072-"))
})

test_that("reads slightly out of order stay on one stream", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)
  cache_file(cache, "data.bin", "open")
  for (offset in c(0, 2 * BLOCK, BLOCK, 3 * BLOCK, 6 * BLOCK)) {
    expect_identical(
      cache_read(cache, offset, READ, open = FALSE),
      file_bytes("data")[offset + seq_len(READ)]
    )
  }
  cache_file(cache, "data.bin", "release")

  expect_equal(mock$raw_file_ranges(), "bytes=0-")
})

test_that("files with the same contents share a cached copy", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache <- local_mount_cache(
    mock,
    list(a.bin = list(content = "data"), b.bin = list(content = "data")),
    cache_dir
  )

  expect_identical(cache_read_all(cache, path = "a.bin"), file_bytes("data"))
  expect_identical(cache_read_all(cache, path = "b.bin"), file_bytes("data"))
  expect_equal(mock$raw_file_ranges(), "bytes=0-")
  expect_equal(cached_names(cache_dir), cached_copy("data"))
})

# A temporary cache made in temp_dir
local_temporary_cache <- function(mock, temp_dir, env = parent.frame()) {
  local_mount_cache(mock, cache_dir = temp_dir, temporary_cache = TRUE, env = env)
}

close_cache <- function(cache) {
  .Call("C_fuse_unmount", cache, PACKAGE = "redivis")
}

# A temporary cache as left by a session killed while mounted
make_orphaned_cache_dir <- function(temp_dir, name, lock_contents = "12345\n") {
  path <- file.path(temp_dir, paste0("redivis_mount_cache_", name))
  dir.create(path)
  writeBin(file_bytes("data"), file.path(path, paste0(cache_key("data"), ".data")))
  if (!is.null(lock_contents)) {
    cat(lock_contents, file = file.path(path, ".lock"))
  }
  path
}

test_that("a temporary cache is removed on unmount", {
  mock <- local_mock_api()
  temp_dir <- withr::local_tempdir()
  cache <- local_temporary_cache(mock, temp_dir)
  expect_identical(cache_read_all(cache), file_bytes("data"))
  cache_dirs <- list.files(temp_dir, full.names = TRUE)
  expect_length(cache_dirs, 1)
  expect_setequal(
    list.files(cache_dirs, all.files = TRUE, no.. = TRUE),
    c(".lock", cached_copy("data"))
  )

  close_cache(cache)

  expect_equal(list.files(temp_dir, all.files = TRUE, no.. = TRUE), character(0))
})

test_that("temporary caches of exited sessions are removed", {
  mock <- local_mock_api()
  temp_dir <- withr::local_tempdir()
  live <- local_temporary_cache(mock, temp_dir)
  live_dir <- list.files(temp_dir)
  orphaned <- make_orphaned_cache_dir(temp_dir, "orphaned")
  # Not yet locked by the session making it
  being_made <- make_orphaned_cache_dir(temp_dir, "being_made", lock_contents = "")
  unlocked <- make_orphaned_cache_dir(temp_dir, "unlocked", lock_contents = NULL)
  writeLines("not a cache", file.path(temp_dir, "notes.txt"))

  before <- list.files(temp_dir)
  new <- local_temporary_cache(mock, temp_dir)
  new_dir <- setdiff(list.files(temp_dir), before)

  expect_length(new_dir, 1)
  expect_setequal(
    list.files(temp_dir),
    c(live_dir, basename(being_made), basename(unlocked), new_dir, "notes.txt")
  )
  close_cache(live)
  close_cache(new)
})

test_that("a cache_dir is reused by a later mount", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  first <- local_mount_cache(mock, cache_dir = cache_dir)
  cache_read(first, 0, READ)
  cache_read(first, 7 * BLOCK, READ)
  requests_before <- length(mock$raw_file_ranges())

  second <- local_mount_cache(mock, cache_dir = cache_dir)
  expect_identical(cache_read(second, 0, READ), file_bytes("data")[seq_len(READ)])
  expect_identical(
    cache_read(second, 7 * BLOCK, READ),
    file_bytes("data")[7 * BLOCK + seq_len(READ)]
  )
  expect_length(mock$raw_file_ranges(), requests_before)
})

test_that("a changed file isn't served from a stale cache", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache_read_all(local_mount_cache(mock, cache_dir = cache_dir))

  changed <- local_mount_cache(
    mock,
    list(data.bin = list(content = "changed")),
    cache_dir
  )
  expect_identical(cache_read_all(changed), file_bytes("changed"))
})

test_that("an inconsistent block map is discarded", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache_read_all(local_mount_cache(mock, cache_dir = cache_dir))
  data_file <- file.path(cache_dir, paste0(cache_key("data"), ".data"))
  writeBin(readBin(data_file, "raw", BLOCK), data_file)

  fresh <- local_mount_cache(mock, cache_dir = cache_dir)
  expect_identical(cache_read_all(fresh), file_bytes("data"))
  expect_equal(mock$raw_file_ranges(), c("bytes=0-", "bytes=0-"))
})

test_that("an empty file reads without a request", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock, list(data.bin = list(content = "data", size = 0)))

  expect_identical(cache_read(cache, 0, READ), raw(0))
  expect_null(mock$raw_file_ranges())
})

# --- max cache size ---------------------------------------------------------

abc <- list(
  a.bin = list(content = "a"),
  b.bin = list(content = "b"),
  c.bin = list(content = "c")
)

test_that("the least recently used files are evicted past the max size", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache <- local_mount_cache(mock, abc, cache_dir, max_cache_size = 2.5 * SIZE)
  for (name in names(abc)) {
    cache_read_all(cache, path = name)
  }

  expect_equal(cached_names(cache_dir), sort(c(cached_copy("b"), cached_copy("c"))))
  requests_before <- length(mock$raw_file_ranges())
  expect_identical(cache_read_all(cache, path = "a.bin"), file_bytes("a"))
  expect_length(mock$raw_file_ranges(), requests_before + 1)
})

test_that("open files aren't evicted", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache <- local_mount_cache(mock, abc, cache_dir, max_cache_size = 2.5 * SIZE)
  cache_file(cache, "a.bin", "open")
  cache_read(cache, 0, SIZE, path = "a.bin", open = FALSE)
  cache_read_all(cache, path = "b.bin")
  cache_read_all(cache, path = "c.bin")

  expect_true(all(cached_copy("a") %in% cached_names(cache_dir)))
  expect_false(any(cached_copy("b") %in% cached_names(cache_dir)))
  requests_before <- length(mock$raw_file_ranges())
  expect_identical(cache_read(cache, 0, SIZE, path = "a.bin", open = FALSE), file_bytes("a"))
  expect_length(mock$raw_file_ranges(), requests_before)
  cache_file(cache, "a.bin", "release")
})

test_that("a file closed while the cache is over its max size is evicted", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  ab <- abc[c("a.bin", "b.bin")]
  cache <- local_mount_cache(mock, ab, cache_dir, max_cache_size = 1.5 * SIZE)
  for (name in names(ab)) {
    cache_file(cache, name, "open")
    cache_read(cache, 0, SIZE, path = name, open = FALSE)
  }

  cache_file(cache, "a.bin", "release")

  expect_equal(cached_names(cache_dir), cached_copy("b"))
  cache_file(cache, "b.bin", "release")
})

test_that("a cache left by an earlier mount is trimmed to the max size", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  first <- local_mount_cache(mock, abc, cache_dir)
  for (i in seq_along(abc)) {
    cache_read_all(first, path = names(abc)[[i]])
    # Last used in the order read, whatever the filesystem's timestamp resolution
    Sys.setFileTime(
      file.path(cache_dir, paste0(cache_key(abc[[i]]$content), ".data")),
      as.POSIXct(1000 + i, origin = "1970-01-01")
    )
  }

  local_mount_cache(mock, abc, cache_dir, max_cache_size = 1.5 * SIZE)

  expect_equal(cached_names(cache_dir), cached_copy("c"))
})

test_that("a file larger than the cache is streamed without caching it", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  cache <- local_mount_cache(mock, cache_dir = cache_dir, max_cache_size = SIZE - 1)

  expect_identical(cache_read_all(cache), file_bytes("data"))
  expect_equal(mock$raw_file_ranges(), "bytes=0-")
  expect_identical(cache_read(cache, SIZE - 50, 50), tail(file_bytes("data"), 50))
  expect_identical(
    cache_read(cache, 4 * BLOCK - 10, 20),
    file_bytes("data")[4 * BLOCK - 10 + seq_len(20)]
  )
  expect_equal(cached_names(cache_dir), character(0))
})

test_that("other files in the cache_dir are never evicted", {
  mock <- local_mock_api()
  cache_dir <- withr::local_tempdir()
  others <- c("notes.txt", "results.blocks.bak", "results.data")
  for (name in others) {
    writeBin(file_bytes("data"), file.path(cache_dir, name))
  }

  cache <- local_mount_cache(mock, cache_dir = cache_dir, max_cache_size = 0)
  cache_read_all(cache)

  expect_equal(cached_names(cache_dir), others)
})

# --- authentication ---------------------------------------------------------

test_that("a mount's token can be replaced", {
  mock <- local_mock_api()
  mock$set(require_auth = TRUE)
  cache <- local_mount_cache(mock, auth_token = "")
  expect_error(cache_read(cache, 0, READ), "read failed")

  .Call("C_fuse_set_auth_token", cache, get_auth_token(), PACKAGE = "redivis")

  expect_identical(cache_read(cache, 0, READ), file_bytes("data")[seq_len(READ)])
})

test_that("a mount is supplied a refreshed token once its own is past half its life", {
  mock <- local_mock_api()
  mock$set(require_auth = TRUE)
  cache <- local_mount_cache(mock, auth_token = "")
  auth_vars$cached_credentials$expires_at <- as.numeric(Sys.time()) + 60

  refresh_mount_token(cache)

  expect_gt(auth_vars$cached_credentials$expires_at, as.numeric(Sys.time()) + 1800)
  expect_identical(cache_read(cache, 0, READ), file_bytes("data")[seq_len(READ)])
})

test_that("a fresh token is left alone", {
  mock <- local_mock_api()
  cache <- local_mount_cache(mock)
  credentials <- auth_vars$cached_credentials

  refresh_mount_token(cache)

  expect_identical(auth_vars$cached_credentials, credentials)
})

# --- keys and arguments -----------------------------------------------------

test_that("files are keyed by their hash, or their id and size without one", {
  table <- mock_table()
  hashed <- File$new(
    id = "abcd-1234",
    table = table,
    properties = list(size = 5, md5_hash = "1B2M2Y8AsgTpgAmY7PhCfg==")
  )
  unhashed <- File$new(id = "abcd/1234", table = table, properties = list(size = 123456789))

  expect_equal(fuse_cache_key(hashed), "d41d8cd98f00b204e9800998ecf8427e")
  expect_equal(fuse_cache_key(unhashed), "abcd_1234-123456789")
})

test_that("mount() rejects a negative max_cache_size", {
  skip_on_os("windows")
  root <- Directory$new(path = "/", table = mock_table())

  expect_error(
    root$mount(tempfile(), max_cache_size = -1),
    "max_cache_size"
  )
})
