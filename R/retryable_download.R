perform_retryable_download <- function(
  uri,
  download_path = NULL,
  query = NULL,
  overwrite = FALSE,
  size = NULL,
  md5_hash = NULL,
  start_byte = 0,
  retry_count = 0
) {
  args <- list(
    uri = uri,
    download_path = download_path,
    query = query,
    overwrite = overwrite,
    size = size,
    md5_hash = md5_hash,
    start_byte = start_byte,
    retry_count = retry_count
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

  tryCatch(
    {
      exact_file_exists <- FALSE
      pb_multiplier <- 0
      pb <- progressr::progressor(steps = 100)
      last_progress_time <- proc.time()[["elapsed"]]
      progress_bytes_written <- 0
      supports_range_requests <- FALSE

      headers_callback <- function(response_headers) {
        should_check_filename <- is.null(size) || is.null(md5_hash)
        accept_ranges <- response_headers[["accept-ranges"]]
        if (!is.null(accept_ranges) && tolower(accept_ranges) != "none") {
          supports_range_requests <<- TRUE
        }
        content_range <- response_headers[["content-range"]]
        if (!is.null(content_range)) {
          size <<- as.integer(sub(".*/", "", content_range))
        } else {
          size <<- as.integer(response_headers[["content-length"]])
        }
        # Prefer content-digest (values wrapped in colons, e.g. "md5=:uE0r1xmbDXTJAGiWL6xlHw==:")
        # over x-goog-hash (no colons, e.g. "md5=uE0r1xmbDXTJAGiWL6xlHw==")
        hash_header <- response_headers[["content-digest"]]
        if (!is.null(hash_header)) {
          md5_hash <<- trimws(gsub(
            ":",
            "",
            regmatches(
              hash_header,
              regexpr("(?<=md5=)[^,]+", hash_header, perl = TRUE)
            )
          ))
        } else {
          hash_header <- response_headers[["x-goog-hash"]]
          md5_hash <<- trimws(regmatches(
            hash_header,
            regexpr("(?<=md5=)[^,]+", hash_header, perl = TRUE)
          ))
        }
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
        if (size > 0) {
          pb_multiplier <<- 100 / size
        }
      }

      con <- NULL
      stream_callback = function(chunk) {
        if (exact_file_exists) {
          return(FALSE) # Stop download
        } else if (is.null(con)) {
          con <<- base::file(
            base::file.path(download_path),
            if (start_byte == 0) "wb" else "ab"
          )
        }

        progress_bytes_written <<- progress_bytes_written + length(chunk)
        if (proc.time()[["elapsed"]] - last_progress_time > 0.2) {
          pb(amount = progress_bytes_written * pb_multiplier)
          progress_bytes_written <<- 0
          last_progress_time <<- proc.time()[["elapsed"]]
        }
        writeBin(chunk, con)
        return(NULL)
      }
      on.exit(if (!is.null(con)) close(con), add = TRUE)

      headers <- list()
      if (start_byte > 0) {
        headers[["Range"]] <- paste0("bytes=", start_byte, "-")
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
    },
    error = function(e) {
      if (retry_count < 10) {
        Sys.sleep(retry_count)
        args$retry_count <- retry_count + 1
        if (supports_range_requests) {
          args$start_byte <- if (file.exists(download_path)) {
            file.size(download_path)
          } else {
            0
          }
        } else {
          # Server doesn't support range requests; delete partial file and restart
          if (file.exists(download_path)) {
            unlink(download_path)
          }
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

perform_parallel_download <- function(
  uris,
  download_paths,
  sizes = NULL,
  md5_hashes = NULL,
  overwrite = FALSE,
  max_parallelization,
  total_bytes = NULL
) {
  pb <- NULL
  on_progress <- NULL
  if (!is.null(total_bytes)) {
    # We need to define the on_progress callback here, since we can't update pb directly from within the curl handler
    pb_multiplier <- 100 / total_bytes
    pb <- progressr::progressor(steps = 100)
    on_progress <- function(bytes = NULL) {
      pb(amount = bytes * pb_multiplier)
    }
  }

  did_check_existing <- FALSE
  if (!is.null(sizes) && !is.null(md5_hashes)) {
    did_check_existing <- TRUE
    # Filter uris, removing all where check_download_filename returns a non-NULL
    # value (indicating the file already exists and matches the expected size and hash)
    keep <- vapply(
      seq_along(uris),
      function(i) {
        size <- sizes[[i]]
        md5_hash <- md5_hashes[[i]]
        exact_file_exists <- check_download_filename(
          filename = download_paths[[i]],
          overwrite = overwrite,
          retry_count = 0L,
          size = size,
          md5_hash = md5_hash
        )
        if (exact_file_exists) {
          if (!is.null(on_progress)) {
            on_progress(size)
          }
          FALSE
        } else {
          TRUE
        }
      },
      logical(1L)
    )

    uris <- uris[keep]
    download_paths <- download_paths[keep]
    sizes <- sizes[keep]
    md5_hashes <- md5_hashes[keep]
  }

  if (length(uris) == 0L) {
    return(invisible(NULL))
  }

  handles <- vector("list", length(uris))

  # limit open files
  # NOTE: Parallelization above 24 seems to yield diminishing returns, even for a large number of small files
  #       It's also causing weird silent "hangs" when downloading in notebooks
  max_parallelization <- max(min(100, length(uris), max_parallelization), 1)
  # pool <- curl::new_pool(
  #   total_con = ceiling(max(1, max_parallelization / 2)), # multiplexing allows us to have fewer connections, so divide by 2
  # )

  # TODO: upgrade to curl >= 6.1.0 and we can do:
  streams_per_connection <- 10
  pool <- curl::new_pool(
    total_con = 100, #ceiling(max_parallelization / streams_per_connection),
    host_con = 100, #ceiling(max_parallelization / streams_per_connection),
    max_streams = streams_per_connection
  )

  # track open connections so we can clean up on exit
  file_connections <- vector("list", length(uris))

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
    },
    add = TRUE
  )

  # a queue of downloads still to schedule (for retries)
  pending <- list()

  # keep track of simultaneously‐running downloads
  active_downloads <- 0L
  active_bytes <- 0

  # estimate the size of a file by index
  avg_file_size <- if (!is.null(total_bytes) && length(uris) > 0) {
    total_bytes / length(uris)
  } else {
    0
  }
  get_estimated_size <- function(index) {
    if (!is.null(sizes) && index <= length(sizes)) {
      sizes[[index]]
    } else {
      avg_file_size
    }
  }
  TARGET_ACTIVE_BYTES <- 1e9 # 1 GB

  # helper to schedule (or re‐schedule) a download
  schedule_download <- function(index, retry_count = 0L, start_byte = 0L) {
    max_retries <- 10L
    download_path <- download_paths[[index]]
    url <- generate_api_url(uris[[index]])

    if (retry_count > 0) {
      Sys.sleep(retry_count)
    }

    # build a fresh handle
    h <- curl::new_handle()
    handles[[index]] <<- h
    if (Sys.getenv("REDIVIS_API_ENDPOINT") == "https://localhost:8443/api/v1") {
      curl::handle_setopt(h, ssl_verifypeer = 0L)
    }

    headers <- get_authorization_header(as_list = TRUE)
    if (start_byte > 0) {
      headers$Range <- sprintf("bytes=%d-", start_byte)
    }
    curl::handle_setheaders(h, .list = headers)
    curl::handle_setopt(h, url = url)

    # local state for this download
    file_con <- NULL
    content_length <- NULL
    bytes_written <- 0
    last_progress_time <- proc.time()[["elapsed"]]
    progress_bytes_written <- 0
    did_error <- FALSE
    did_short_circuit <- FALSE
    estimated_size <- get_estimated_size(index)
    supports_range_requests <- FALSE

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
            supports_range_requests && file.exists(download_path)
          ) {
            file.size(download_path)
          } else {
            0
          }
          pending[[length(pending) + 1]] <<- list(
            index = index,
            retry_count = retry_count + 1L,
            start_byte = resume_byte
          )
          return(FALSE)
        }
        if (status >= 400L) {
          did_error <<- TRUE
          active_downloads <<- active_downloads - 1L
          active_bytes <<- active_bytes - estimated_size
          stop(sprintf("HTTP %d for path %s", status, url))
        }
        headers <- parse_curl_headers(res)

        accept_ranges <- headers[["accept-ranges"]]
        if (!is.null(accept_ranges) && tolower(accept_ranges) != "none") {
          supports_range_requests <<- TRUE
        }

        content_range <- headers[["content-range"]]
        if (!is.null(content_range)) {
          content_length <<- as.integer(sub(".*/", "", content_range))
        } else {
          content_length <<- as.integer(headers[["content-length"]])
        }

        if (length(content_length) == 0 || is.na(content_length)) {
          content_length <<- NULL
        }

        if (!did_check_existing) {
          # Prefer content-digest (values wrapped in colons, e.g. "md5=:uE0r1xmbDXTJAGiWL6xlHw==:")
          # over x-goog-hash (no colons, e.g. "md5=uE0r1xmbDXTJAGiWL6xlHw==")
          hash_header <- headers[["content-digest"]]
          if (!is.null(hash_header)) {
            extracted_md5 <- trimws(gsub(
              ":",
              "",
              regmatches(
                hash_header,
                regexpr("(?<=md5=)[^,]+", hash_header, perl = TRUE)
              )
            ))
            md5_hash <<- if (length(extracted_md5) == 0) NULL else extracted_md5
          } else {
            hash_header <- headers[["x-goog-hash"]]
            if (is.null(hash_header)) {
              md5_hash <<- NULL
            } else {
              extracted_md5 <- trimws(regmatches(
                hash_header,
                regexpr("(?<=md5=)[^,]+", hash_header, perl = TRUE)
              ))
              md5_hash <<- if (length(extracted_md5) == 0) {
                NULL
              } else {
                extracted_md5
              }
            }
          }

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
        file_con <<- base::file(
          download_path,
          if (start_byte == 0) "wb" else "ab"
        )
        file_connections[[index]] <<- file_con
      }

      # write the chunk
      if (length(chunk)) {
        bytes_written <<- bytes_written + length(chunk)
        writeBin(chunk, file_con)
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

      # on final chunk, clean up if we're done (note that if we haven't read all bytes in content-length, fail_cb will be called)
      if (
        final && (is.null(content_length) || bytes_written == content_length)
      ) {
        close(file_con)
        file_connections[[index]] <<- NULL
        if (!is.null(on_progress) && progress_bytes_written > 0) {
          on_progress(progress_bytes_written)
        }
        active_downloads <<- active_downloads - 1L
        active_bytes <<- active_bytes - estimated_size
      } else if (final && bytes_written != content_length) {
        # Incomplete download — treat as a retriable error
        tryCatch(close(file_con), error = function(e) {})
        file_connections[[index]] <<- NULL
        active_downloads <<- active_downloads - 1L
        active_bytes <<- active_bytes - estimated_size
        if (retry_count < max_retries) {
          resume_byte <- if (
            supports_range_requests && file.exists(download_path)
          ) {
            file.size(download_path)
          } else {
            0
          }
          pending[[length(pending) + 1]] <<- list(
            index = index,
            retry_count = retry_count + 1L,
            start_byte = resume_byte
          )
        } else {
          stop(sprintf(
            "Download incomplete for %s: got %d of %d bytes after %d retries",
            download_path,
            bytes_written,
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
          supports_range_requests && file.exists(download_path)
        ) {
          file.size(download_path)
        } else {
          0
        }
        pending[[length(pending) + 1]] <<- list(
          index = index,
          retry_count = retry_count + 1L,
          start_byte = resume_byte
        )
      } else {
        did_short_circuit <<- TRUE
        stop(err)
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
    active_downloads < max_parallelization &&
      (active_bytes < TARGET_ACTIVE_BYTES || active_downloads == 0L)
  }

  current_index <- 1

  while (current_index <= length(uris) && can_schedule_more()) {
    schedule_download(current_index)
    current_index <- current_index + 1
  }

  # drive the pool until everything (including retries) is done
  while (active_downloads > 0L) {
    # curl::multi_run(pool = pool, poll = 1)

    tryCatch(
      {
        curl::multi_run(pool = pool, poll = 1)
      },
      redivis_short_circuit = function(e) {
        idx <- e$index
        h <- handles[[idx]]
        if (!is.null(h)) {
          try(curl::multi_remove(pool, h), silent = TRUE)
          handles[[idx]] <<- NULL
        }
        invisible(NULL)
      }
    )

    # schedule any pending retries
    if (length(pending) > 0L) {
      for (task in pending) {
        schedule_download(
          task$index,
          retry_count = task$retry_count,
          start_byte = task$start_byte
        )
      }
      # Make sure we wait for all retries to enqueue. Add 0.1s as additional buffer
      Sys.sleep(
        max(vapply(pending, function(task) task$retry_count, 1)) + 0.1
      )
      pending <- list()
    }
    while (current_index <= length(uris) && can_schedule_more()) {
      schedule_download(current_index)
      current_index <- current_index + 1
    }
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
        file.size(filename) == as.integer(size)
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
