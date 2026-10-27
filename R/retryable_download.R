perform_retryable_download <- function(
  uri,
  download_path = NULL,
  query = NULL,
  overwrite = FALSE,
  size = NULL,
  md5_hash = NULL,
  start_byte = 0,
  retry_count = 0,
  did_restart_for_hash = FALSE
) {
  args <- list(
    uri = uri,
    download_path = download_path,
    query = query,
    overwrite = overwrite,
    size = size,
    md5_hash = md5_hash,
    start_byte = start_byte,
    retry_count = retry_count,
    did_restart_for_hash = did_restart_for_hash
  )

  if (!is.null(size) && !is.null(md5_hash)) {
    exact_file_exists <- check_download_filename(
      download_path,
      overwrite,
      retry_count,
      size,
      md5_hash
    )
    if (exact_file_exists) {
      return(download_path)
    }
  }

  partial_path <- get_partial_download_path(download_path)
  con <- NULL
  # Once the download succeeds, the partial file has been moved to
  # download_path, so this only removes one left by a failure. A retry runs
  # (and resumes from the partial file) before this call exits.
  on.exit(
    {
      if (!is.null(con)) {
        try(close(con), silent = TRUE)
      }
      unlink(partial_path)
    },
    add = TRUE
  )

  tryCatch(
    {
      exact_file_exists <- FALSE
      pb_multiplier <- 0
      pb <- progressr::progressor(steps = 100)
      last_progress_time <- proc.time()[["elapsed"]]
      progress_bytes_written <- 0
      supports_range_requests <- FALSE
      # A response that's decompressed in transit can't be checked against the
      # stored file's size and hash
      is_encoded <- FALSE
      bytes_written <- 0

      headers_callback <- function(response_headers) {
        should_check_filename <- is.null(size) || is.null(md5_hash)
        accept_ranges <- response_headers[["accept-ranges"]]
        if (!is.null(accept_ranges) && tolower(accept_ranges) != "none") {
          supports_range_requests <<- TRUE
        }
        content_range <- response_headers[["content-range"]]
        # A response that doesn't resume where we left off starts the download
        # over
        if (
          start_byte > 0 &&
            get_content_range_start(content_range) != start_byte
        ) {
          start_byte <<- 0
        }
        total_size <- get_total_size(
          content_range,
          response_headers[["content-length"]]
        )
        if (!is.null(total_size)) {
          size <<- total_size
        }
        # Prefer the hash the server reports, but keep the one we were given if
        # it doesn't report one
        md5_hash <<- get_md5_header(response_headers) %||% md5_hash
        content_encoding <- response_headers[["content-encoding"]]
        is_encoded <<- !is.null(content_encoding) &&
          tolower(content_encoding) != "identity"
        if (should_check_filename) {
          exact_file_exists <<- check_download_filename(
            download_path,
            overwrite,
            retry_count,
            size,
            md5_hash
          )
        }
        if (!exact_file_exists) {
          dir.create(
            dirname(download_path),
            showWarnings = FALSE,
            recursive = TRUE
          )
        }
        if (!is.null(size) && size > 0) {
          pb_multiplier <<- 100 / size
        }
      }

      stream_callback = function(chunk) {
        if (exact_file_exists) {
          return(FALSE) # Stop download
        } else if (is.null(con)) {
          con <<- with_local_file_errors(download_path, base::file(
            partial_path,
            if (start_byte == 0) "wb" else "ab"
          ))
        }

        bytes_written <<- bytes_written + length(chunk)
        progress_bytes_written <<- progress_bytes_written + length(chunk)
        if (proc.time()[["elapsed"]] - last_progress_time > 0.2) {
          pb(amount = progress_bytes_written * pb_multiplier)
          progress_bytes_written <<- 0
          last_progress_time <<- proc.time()[["elapsed"]]
        }
        with_local_file_errors(download_path, writeBin(chunk, con))
        return(NULL)
      }

      # Request the file as stored, rather than compressed in transit: only
      # then can it be checked against its size and hash, or resumed, since a
      # range is a range of the bytes sent
      headers <- list(`Accept-Encoding` = "identity")
      if (start_byte > 0) {
        headers[["Range"]] <- paste0(
          "bytes=",
          format(start_byte, scientific = FALSE),
          "-"
        )
      }

      make_request(
        method = "GET",
        path = uri,
        query = query,
        parse_response = FALSE,
        stream_callback = stream_callback,
        headers = headers,
        download_overwrite = isTRUE(overwrite) || retry_count > 0,
        headers_callback = headers_callback
      )

      if (exact_file_exists) {
        return(download_path)
      }

      if (is.null(con)) {
        # An empty response never opened the file
        con <- with_local_file_errors(download_path, base::file(
          partial_path,
          if (start_byte == 0) "wb" else "ab"
        ))
      }
      close(con)
      con <- NULL

      downloaded_size <- start_byte + bytes_written
      if (!is_encoded && !is.null(size) && downloaded_size != size) {
        # The connection ended early; resume from what we have
        stop(sprintf(
          "The download ended after %s of %s bytes",
          format(downloaded_size, scientific = FALSE),
          format(size, scientific = FALSE)
        ))
      }

      if (
        !is_encoded &&
          !is.null(md5_hash) &&
          !identical(get_file_md5(partial_path), decode_md5(md5_hash))
      ) {
        if (did_restart_for_hash) {
          abort_redivis_error(paste0(
            "The downloaded file's MD5 hash doesn't match the expected hash: ",
            download_path
          ))
        }
        # Try once more from scratch, in case the data was corrupted in transit
        unlink(partial_path)
        args$start_byte <- 0
        args$did_restart_for_hash <- TRUE
        return(do.call(perform_retryable_download, args))
      }

      with_local_file_errors(
        download_path,
        file.rename(partial_path, download_path)
      )
    },
    error = function(e) {
      # e.g. no access to the file, or a file already at download_path: fail
      # now, rather than retrying for a minute and reporting a network error
      if (!is_retryable_error(e)) {
        stop(e)
      }
      if (retry_count < 10) {
        if (!is.null(con)) {
          try(close(con), silent = TRUE)
          con <<- NULL
        }
        retry_sleep(retry_count)
        args$retry_count <- retry_count + 1
        if (supports_range_requests && file.exists(partial_path)) {
          args$start_byte <- file.size(partial_path)
        } else {
          # Server doesn't support range requests; delete partial file and restart
          unlink(partial_path)
          args$start_byte <- 0
        }
        return(do.call(perform_retryable_download, args))
      } else {
        abort_redivis_network_error(
          paste0(
            "A network error occurred. Download failed after ",
            retry_count,
            " retries."
          ),
          original_exception = e
        )
      }
    }
  )

  download_path
}

# The first byte of a ranged response, from its Content-Range header, or 0 if
# the response isn't ranged
get_content_range_start <- function(content_range) {
  start <- stringr::str_match(content_range %||% "", "^bytes\\s+(\\d+)-")[, 2]
  if (is.na(start)) 0 else as.numeric(start)
}

# The size of the whole file, from the Content-Range header (or Content-Length,
# for a complete response)
get_total_size <- function(content_range, content_length) {
  total <- if (!is.null(content_range)) {
    sub(".*/", "", content_range)
  } else {
    content_length
  }
  if (!is.null(total) && grepl("^\\d+$", total)) as.numeric(total) else NULL
}

# The file's base64 MD5 hash, from its Content-Digest or x-goog-hash header
get_md5_header <- function(headers) {
  # Prefer content-digest (values wrapped in colons, e.g. "md5=:uE0r1xmbDXTJAGiWL6xlHw==:")
  # over x-goog-hash (no colons, e.g. "md5=uE0r1xmbDXTJAGiWL6xlHw==")
  hash_header <- headers[["content-digest"]] %||% headers[["x-goog-hash"]]
  if (is.null(hash_header)) {
    return(NULL)
  }
  md5 <- regmatches(
    hash_header,
    regexpr("(?<=md5=)[^,]+", hash_header, perl = TRUE)
  )
  if (length(md5) == 0) NULL else trimws(gsub(":", "", md5))
}

decode_md5 <- function(md5_hash) {
  if (is.character(md5_hash)) jsonlite::base64_dec(md5_hash) else md5_hash
}

get_file_md5 <- function(path) {
  digest::digest(path, algo = "md5", file = TRUE, raw = TRUE)
}

perform_parallel_download <- function(
  uris,
  download_paths,
  sizes = NULL,
  md5_hashes = NULL,
  overwrite = FALSE,
  max_parallelization,
  total_bytes = NULL,
  max_concurrency = NULL
) {
  max_parallelization <- resolve_config(
    "max_parallelization",
    max_parallelization,
    parallelly::availableCores()
  )
  avg_file_size <- if (!is.null(total_bytes) && length(uris) > 0) {
    total_bytes / length(uris)
  } else {
    0
  }

  # Determine number of workers (cores) to use, following the pattern in parallel_stream_arrow
  worker_count <- max(
    1,
    min(
      8,
      ceiling(length(uris) / 1000),
      parallelly::availableCores(),
      max_parallelization
    )
  )

  if (!is.null(max_concurrency) && max_concurrency < 1) {
    abort_redivis_value_error(
      "max_concurrency must be greater than or equal to 1."
    )
  }

  pb <- NULL
  on_progress <- NULL
  if (!is.null(total_bytes)) {
    pb_multiplier <- 100 / total_bytes
    pb <- progressr::progressor(steps = 100)
    on_progress <- function(bytes = NULL) {
      pb(amount = bytes * pb_multiplier)
    }
  }

  if (worker_count <= 1) {
    # Single worker: run directly without spawning futures
    perform_parallel_download_worker(
      uris = uris,
      download_paths = download_paths,
      sizes = sizes,
      md5_hashes = md5_hashes,
      overwrite = overwrite,
      on_progress = on_progress,
      avg_file_size = avg_file_size,
      max_concurrency = max_concurrency
    )
    return(invisible(NULL))
  }

  # Set up future plan, matching the pattern in parallel_stream_arrow / perform_table_parallel_file_upload
  # Parallely is returning True here in positron for Mac, but multicore still doesn't work
  if (parallelly::supportsMulticore() && Sys.info()[["sysname"]] != "Darwin") {
    oplan <- future::plan(future::multicore, workers = worker_count)
  } else {
    oplan <- future::plan(future::multisession, workers = worker_count)
  }

  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(future::plan(oplan), add = TRUE)
  # Partition URIs into chunks, one per worker
  indices <- seq_along(uris)
  chunks <- split(indices, cut(indices, breaks = worker_count, labels = FALSE))

  # Build per-chunk task objects so each future only receives the data it needs,
  # avoiding serialization of the full vectors into every worker
  tasks <- lapply(chunks, function(chunk_indices) {
    list(
      uris = uris[chunk_indices],
      download_paths = download_paths[chunk_indices],
      sizes = if (!is.null(sizes)) sizes[chunk_indices] else NULL,
      md5_hashes = if (!is.null(md5_hashes)) md5_hashes[chunk_indices] else NULL
    )
  })

  # Need a local variable for parallelization to work
  .perform_parallel_download_worker <- perform_parallel_download_worker

  furrr::future_map(tasks, function(task) {
    .perform_parallel_download_worker(
      uris = task$uris,
      download_paths = task$download_paths,
      sizes = task$sizes,
      md5_hashes = task$md5_hashes,
      overwrite = overwrite,
      on_progress = on_progress,
      avg_file_size = avg_file_size,
      max_concurrency = if (is.null(max_concurrency)) {
        max_concurrency
      } else {
        ceiling(max_concurrency / worker_count)
      }
    )
  })

  invisible(NULL)
}

perform_parallel_download_worker <- function(
  uris,
  download_paths,
  sizes = NULL,
  md5_hashes = NULL,
  overwrite = FALSE,
  max_concurrency = 48,
  on_progress = NULL,
  avg_file_size
) {
  if (length(uris) == 0L) {
    return(invisible(NULL))
  }

  handles <- vector("list", length(uris))

  if (is.null(max_concurrency)) {
    max_concurrency <- 48
  }

  # limit open files
  # NOTE: Parallelization above 48 seems to yield diminishing returns, even for a large number of small files
  max_concurrency <- max(min(100, max_concurrency, length(uris)), 1)

  pool <- curl::new_pool(
    total_con = max_concurrency,
    host_con = max_concurrency,
    max_streams = 24, # Allow multiplexing, up to 24 streams per connection
    multiplex = TRUE
  )

  # track open connections so we can clean up on exit
  file_connections <- vector("list", length(uris))
  # Downloads write to a partial file alongside their destination, which is
  # only moved there once complete and verified. Any left on exit are from
  # downloads that failed.
  partial_paths <- get_partial_download_path(unlist(download_paths))
  started_downloads <- logical(length(uris))

  on.exit(
    {
      for (con in file_connections) {
        if (!is.null(con)) {
          tryCatch(
            {
              path <- summary(con)$description
              close(con)
              unlink(path)
            },
            error = function(e) {}
          )
        }
      }
      unlink(partial_paths[started_downloads])
    },
    add = TRUE
  )

  # a queue of downloads still to schedule (for retries)
  pending <- list()

  # keep track of simultaneously‐running downloads
  active_downloads <- 0L
  active_bytes <- 0

  get_estimated_size <- function(index) {
    if (!is.null(sizes) && index <= length(sizes)) {
      sizes[[index]]
    } else {
      avg_file_size
    }
  }
  TARGET_ACTIVE_BYTES <- 1e9 # 1 GB

  # helper to schedule (or re‐schedule) a download
  download_error <- NULL
  record_download_error <- function(error) {
    if (is.null(download_error)) {
      download_error <<- error
    }
  }

  schedule_download <- function(
    index,
    retry_count = 0L,
    start_byte = 0L,
    auth_failures = character(),
    did_restart_for_hash = FALSE
  ) {
    max_retries <- 10L
    download_path <- download_paths[[index]]
    partial_path <- partial_paths[[index]]
    url <- generate_api_url(uris[[index]])
    did_check_existing <- FALSE

    # Pre-network check: if we have size + md5_hash, check before fetching headers
    if (
      retry_count == 0L &&
        start_byte == 0L &&
        !is.null(sizes) &&
        index <= length(sizes) &&
        !is.null(md5_hashes) &&
        index <= length(md5_hashes)
    ) {
      did_check_existing <- TRUE
      exact_file_exists <- check_download_filename(
        filename = download_path,
        overwrite = overwrite,
        retry_count = 0L,
        size = sizes[[index]],
        md5_hash = md5_hashes[[index]]
      )
      if (exact_file_exists) {
        if (!is.null(on_progress)) {
          on_progress(sizes[[index]])
        }
        return(invisible(NULL))
      }
    }

    if (retry_count > 0) {
      retry_sleep(retry_count)
    }

    # build a fresh handle
    h <- curl::new_handle()
    handles[[index]] <<- h

    # Public files can be downloaded without credentials, so they're only sent
    # when available
    headers <- get_authorization_header(as_list = TRUE, allow_anonymous = TRUE)
    request_authorization <- headers$Authorization
    # Request the file as stored, rather than compressed in transit (see
    # perform_retryable_download)
    headers[["Accept-Encoding"]] <- "identity"
    if (start_byte > 0) {
      headers$Range <- paste0(
        "bytes=",
        format(start_byte, scientific = FALSE),
        "-"
      )
    }
    curl::handle_setheaders(h, .list = headers)
    curl::handle_setopt(h, url = url)
    curl::handle_setopt(h, buffersize = 131072L) # 128 KB
    # Abort a stalled transfer (under 1 byte/sec for 120s) so that fail_cb
    # retries it, rather than multi_run() waiting on it forever
    curl::handle_setopt(h, low_speed_limit = 1L, low_speed_time = 120L)

    # local state for this download
    file_con <- NULL
    content_length <- NULL
    md5_hash <- if (!is.null(md5_hashes) && index <= length(md5_hashes)) {
      md5_hashes[[index]]
    }
    known_size <- if (!is.null(sizes) && index <= length(sizes)) {
      sizes[[index]]
    }
    # A response that's decompressed in transit can't be checked against the
    # stored file's size and hash
    is_encoded <- FALSE
    bytes_written <- 0
    last_progress_time <- proc.time()[["elapsed"]]
    progress_bytes_written <- 0
    did_error <- FALSE
    did_short_circuit <- FALSE
    estimated_size <- get_estimated_size(index)
    supports_range_requests <- FALSE

    # Fails the download for good on a local error, e.g. an unwritable
    # download_path. Raising it instead would abort the transfer as a curl
    # write error, which fail_cb would retry as a network failure.
    fail_locally <- function(error) {
      did_error <<- TRUE
      active_downloads <<- active_downloads - 1L
      active_bytes <<- active_bytes - estimated_size
      if (!is.null(file_con)) {
        tryCatch(close(file_con), error = function(e) {})
        file_connections[[index]] <<- NULL
      }
      record_download_error(error)
      FALSE
    }

    # callback: write chunks
    write_cb <- function(chunk, final) {
      if (did_error || did_short_circuit) {
        return(FALSE)
      }
      # open file on first chunk
      if (is.null(file_con)) {
        # Note: at this point the response headers have been processed,
        # so we can query the status code via curl::handle_data()
        res <- curl::handle_data(h)
        status <- res$status_code

        if (status == 0) {
          did_error <<- TRUE
          # curl‐level error; let fail_cb handle it
          return(FALSE)
        }
        if (status == 503L && retry_count < max_retries) {
          did_error <<- TRUE
          active_downloads <<- active_downloads - 1L
          active_bytes <<- active_bytes - estimated_size
          # queue a retry
          resume_byte <- if (
            supports_range_requests && file.exists(partial_path)
          ) {
            file.size(partial_path)
          } else {
            0
          }
          pending[[length(pending) + 1]] <<- list(
            index = index,
            retry_count = retry_count + 1L,
            start_byte = resume_byte,
            auth_failures = auth_failures,
            did_restart_for_hash = did_restart_for_hash
          )
          return(FALSE)
        }
        if (status >= 400L) {
          did_error <<- TRUE
          active_downloads <<- active_downloads - 1L
          active_bytes <<- active_bytes - estimated_size

          # Check for reauth-able errors (401, or 403 with insufficient_scope)
          if (
            (status == 401L || status == 403L) &&
              is.na(Sys.getenv("REDIVIS_API_TOKEN", unset = NA)) &&
              is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset = NA))
          ) {
            resp_body <- tryCatch(
              rawToChar(chunk),
              error = function(e) NULL
            )
            response_content <- NULL
            is_json <- FALSE
            if (!is.null(resp_body) && nzchar(resp_body)) {
              tryCatch(
                {
                  response_content <- jsonlite::fromJSON(
                    resp_body,
                    simplifyVector = FALSE
                  )
                  is_json <- TRUE
                },
                error = function(e) {}
              )
            }

            auth_failure <- auth_failure_signature(
              status,
              if (is_json) response_content,
              authenticated = !is.null(request_authorization)
            )
            # As in make_request(), only re-authenticate while it makes
            # progress; otherwise a token the server keeps rejecting would be
            # refreshed and retried forever
            if (
              (status == 401L ||
                (status == 403L &&
                  is_json &&
                  identical(response_content$error, "insufficient_scope"))) &&
                !(auth_failure %in% auth_failures) &&
                length(auth_failures) < MAX_AUTH_ATTEMPTS
            ) {
              # Concurrent downloads are rejected together, so only the first
              # to see it re-authenticates, and the rest retry with the
              # credentials it got
              current_authorization <- get_authorization_header(
                as_list = TRUE,
                allow_anonymous = TRUE
              )$Authorization
              if (identical(current_authorization, request_authorization)) {
                refresh_credentials(
                  scope = if (!is.null(response_content$scope)) {
                    strsplit(response_content$scope, " ")[[1]]
                  },
                  amr_values = response_content$amr_values
                )
              }
              # Queue a retry with fresh credentials
              pending[[length(pending) + 1]] <<- list(
                index = index,
                retry_count = 0L,
                start_byte = 0L,
                auth_failures = c(auth_failures, auth_failure),
                did_restart_for_hash = did_restart_for_hash
              )
              return(FALSE)
            }
          }

          record_download_error(list(
            status = status,
            body = tryCatch(rawToChar(chunk), error = function(e) NULL)
          ))
          return(FALSE)
        }
        headers <- parse_curl_headers(res)

        accept_ranges <- headers[["accept-ranges"]]
        if (!is.null(accept_ranges) && tolower(accept_ranges) != "none") {
          supports_range_requests <<- TRUE
        }

        # A response that doesn't resume where we left off starts the download
        # over
        if (
          start_byte > 0 &&
            (status != 206L ||
              get_content_range_start(headers[["content-range"]]) != start_byte)
        ) {
          start_byte <<- 0
        }

        content_length <<- get_total_size(
          headers[["content-range"]],
          headers[["content-length"]]
        ) %||%
          known_size
        content_encoding <- headers[["content-encoding"]]
        is_encoded <<- !is.null(content_encoding) &&
          tolower(content_encoding) != "identity"

        # Prefer the hash the server reports, but keep the one we were given if
        # it doesn't report one
        md5_hash <<- get_md5_header(headers) %||% md5_hash

        if (!did_check_existing) {
          exact_file_exists <- check_download_filename(
            filename = download_path,
            overwrite = overwrite,
            retry_count = retry_count,
            size = content_length,
            md5_hash = md5_hash
          )

          if (exact_file_exists) {
            if (!is.null(on_progress)) {
              on_progress(content_length)
            }
            did_short_circuit <<- TRUE # prevent further chunk processing
            active_downloads <<- active_downloads - 1L
            active_bytes <<- active_bytes - estimated_size
            stop(structure(
              list(
                message = "[OK] Skipping file, already present",
                call = NULL,
                index = index
              ),
              class = c("redivis_short_circuit", "condition")
            ))
          }
        }

        retry_count <<- 0
        dir.create(
          dirname(download_path),
          showWarnings = FALSE,
          recursive = TRUE
        )
        started_downloads[[index]] <<- TRUE
        opened <- tryCatch(
          with_local_file_errors(download_path, base::file(
            partial_path,
            if (start_byte == 0) "wb" else "ab"
          )),
          redivis_error = function(e) e
        )
        if (inherits(opened, "error")) {
          return(fail_locally(opened))
        }
        file_con <<- opened
        file_connections[[index]] <<- file_con
      }

      # write the chunk
      if (length(chunk)) {
        bytes_written <<- bytes_written + length(chunk)
        write_error <- tryCatch(
          with_local_file_errors(download_path, writeBin(chunk, file_con)),
          redivis_error = function(e) e
        )
        if (inherits(write_error, "error")) {
          return(fail_locally(write_error))
        }
        # NOTE: progress updates don't work within the curl handler; we can't do this
        progress_bytes_written <<- progress_bytes_written + length(chunk)
        if (proc.time()[["elapsed"]] - last_progress_time > 1) {
          if (!is.null(on_progress)) {
            on_progress(progress_bytes_written)
          }
          progress_bytes_written <<- 0
          last_progress_time <<- proc.time()[["elapsed"]]
        }
      }

      # on final chunk, clean up if we're done
      downloaded_size <- start_byte + bytes_written
      if (
        final &&
          (is_encoded ||
            is.null(content_length) ||
            downloaded_size == content_length)
      ) {
        close(file_con)
        file_connections[[index]] <<- NULL
        file_con <<- NULL
        if (!is.null(on_progress) && progress_bytes_written > 0) {
          on_progress(progress_bytes_written)
        }
        if (
          !is_encoded &&
            !is.null(md5_hash) &&
            !identical(get_file_md5(partial_path), decode_md5(md5_hash))
        ) {
          active_downloads <<- active_downloads - 1L
          active_bytes <<- active_bytes - estimated_size
          if (did_restart_for_hash) {
            record_download_error(tryCatch(
              abort_redivis_error(paste0(
                "The downloaded file's MD5 hash doesn't match the expected hash: ",
                download_path
              )),
              redivis_error = function(e) e
            ))
          } else {
            # Try once more from scratch, in case the data was corrupted in
            # transit
            unlink(partial_path)
            pending[[length(pending) + 1]] <<- list(
              index = index,
              retry_count = retry_count,
              start_byte = 0,
              auth_failures = auth_failures,
              did_restart_for_hash = TRUE
            )
          }
          return(invisible(NULL))
        }
        moved <- tryCatch(
          with_local_file_errors(
            download_path,
            file.rename(partial_path, download_path)
          ),
          redivis_error = function(e) e
        )
        if (inherits(moved, "error")) {
          return(fail_locally(moved))
        }
        active_downloads <<- active_downloads - 1L
        active_bytes <<- active_bytes - estimated_size
      } else if (final) {
        # Incomplete download — treat as a retriable error
        tryCatch(close(file_con), error = function(e) {})
        file_connections[[index]] <<- NULL
        active_downloads <<- active_downloads - 1L
        active_bytes <<- active_bytes - estimated_size
        if (retry_count < max_retries) {
          resume_byte <- if (
            supports_range_requests && file.exists(partial_path)
          ) {
            file.size(partial_path)
          } else {
            0
          }
          pending[[length(pending) + 1]] <<- list(
            index = index,
            retry_count = retry_count + 1L,
            start_byte = resume_byte,
            auth_failures = auth_failures,
            did_restart_for_hash = did_restart_for_hash
          )
        } else {
          record_download_error(sprintf(
            "Download incomplete for %s: got %.0f of %.0f bytes after %d retries",
            download_path,
            downloaded_size,
            content_length,
            max_retries
          ))
        }
      }
    }

    # callback: on error
    fail_cb <- function(err) {
      if (did_short_circuit) {
        return(invisible(NULL))
      }

      active_downloads <<- active_downloads - 1L
      active_bytes <<- active_bytes - estimated_size
      if (!is.null(file_con)) {
        tryCatch(close(file_con), error = function(e) {})
        file_connections[[index]] <<- NULL
      }
      if (retry_count < max_retries) {
        resume_byte <- if (
          supports_range_requests && file.exists(partial_path)
        ) {
          file.size(partial_path)
        } else {
          0
        }
        pending[[length(pending) + 1]] <<- list(
          index = index,
          retry_count = retry_count + 1L,
          start_byte = resume_byte,
          auth_failures = auth_failures,
          did_restart_for_hash = did_restart_for_hash
        )
      } else {
        did_short_circuit <<- TRUE
        record_download_error(err)
      }
    }

    # finally, add to the pool
    curl::multi_add(
      handle = h,
      pool = pool,
      data = write_cb,
      fail = fail_cb
    )
    active_downloads <<- active_downloads + 1L
    active_bytes <<- active_bytes + estimated_size
  }

  can_schedule_more <- function() {
    active_downloads < max_concurrency &&
      (active_bytes < TARGET_ACTIVE_BYTES || active_downloads == 0L)
  }

  current_index <- 1

  while (current_index <= length(uris) && can_schedule_more()) {
    schedule_download(current_index)
    current_index <- current_index + 1
  }

  # Drive the pool until everything (including retries) is done.
  #
  # An error raised in a curl callback doesn't propagate out of multi_run():
  # curl prints it, aborts that transfer (calling its fail callback), and keeps
  # running, unless it was raised by the transfer's final callback. So the
  # callbacks record a download that has failed for good in download_error,
  # and it's raised from here, after cancelling the rest of the run.
  #
  # (That's also how a redivis_short_circuit condition skips a file: raising
  # it aborts the transfer, and fail_cb ignores it. The handler below only
  # sees one raised by a final callback.)
  while (active_downloads > 0L) {
    tryCatch(
      {
        curl::multi_run(pool = pool, poll = 1)
      },
      redivis_short_circuit = function(e) {
        idx <- e$index
        h <- handles[[idx]]
        if (!is.null(h)) {
          try(curl::multi_cancel(h), silent = TRUE)
          handles[[idx]] <<- NULL
        }
        invisible(NULL)
      }
    )

    if (!is.null(download_error)) {
      # Don't spend time on the other files; the download has failed
      for (h in handles) {
        if (!is.null(h)) {
          try(curl::multi_cancel(h), silent = TRUE)
        }
      }
      break
    }

    # schedule any pending retries
    if (length(pending) > 0L) {
      for (task in pending) {
        schedule_download(
          task$index,
          retry_count = task$retry_count,
          start_byte = task$start_byte,
          auth_failures = task$auth_failures %||% character(),
          did_restart_for_hash = isTRUE(task$did_restart_for_hash)
        )
      }
      # Make sure we wait for all retries to enqueue. Add 0.1s as additional buffer
      retry_sleep(
        max(vapply(pending, function(task) task$retry_count, 1)) + 0.1
      )
      pending <- list()
    }
    while (current_index <= length(uris) && can_schedule_more()) {
      schedule_download(current_index)
      current_index <- current_index + 1
    }
  }

  if (inherits(download_error, "redivis_error")) {
    # A local failure, e.g. download_path couldn't be written
    stop(download_error)
  } else if (is.list(download_error) && !inherits(download_error, "condition")) {
    # An HTTP error: raise it as make_request() would
    response_json <- tryCatch(
      jsonlite::fromJSON(download_error$body, simplifyVector = FALSE),
      error = function(e) NULL
    )
    raise_api_error(
      response_json = response_json,
      response_text = download_error$body,
      response = list(status_code = download_error$status)
    )
  } else if (!is.null(download_error)) {
    abort_redivis_network_error(
      "A network error occurred. Download failed",
      original_exception = if (inherits(download_error, "condition")) {
        download_error
      } else {
        simpleError(download_error)
      }
    )
  }
}


check_download_filename <- function(
  filename,
  overwrite,
  retry_count,
  size,
  md5_hash
) {
  if (retry_count == 0 && !is.null(filename) && file.exists(filename)) {
    if (
      !is.null(size) &&
        !is.null(md5_hash) &&
        file.size(filename) == as.numeric(size)
    ) {
      # Decode base64 hash if necessary
      if (is.character(md5_hash)) {
        md5_hash <- jsonlite::base64_dec(md5_hash)
      }

      file_hash <- digest::digest(
        filename,
        algo = "md5",
        file = TRUE,
        raw = TRUE
      )

      if (identical(file_hash, md5_hash)) {
        return(TRUE)
      } else if (!isTRUE(overwrite)) {
        abort_redivis_value_error(
          paste0(
            "File already exists at '",
            filename,
            "'. Set parameter overwrite=TRUE to overwrite existing files."
          )
        )
      }
    } else if (!isTRUE(overwrite)) {
      abort_redivis_value_error(
        paste0(
          "File already exists at '",
          filename,
          "'. Set parameter overwrite=TRUE to overwrite existing files."
        )
      )
    }
  }
  FALSE
}


# Raises a failure to open or write download_path (e.g. permission denied, or
# a full disk) as a redivis_error, which is_retryable_error() won't retry:
# retrying can't fix it, and it would be reported as a network error. Warnings
# are caught too, since base::file() only describes why it failed in one.
with_local_file_errors <- function(download_path, expr) {
  on_error <- function(e) {
    abort_redivis_error(
      paste0(
        "Failed to write to '",
        download_path,
        "': ",
        conditionMessage(e)
      )
    )
  }
  tryCatch(expr, warning = on_error, error = on_error)
}
