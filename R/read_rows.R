RedivisBatchReader <- R6::R6Class(
  "RedivisBatchReader",
  public = list(
    streams = NULL,
    current_offset = 0,
    coerce_schema = FALSE,
    retry_count = 0,
    current_stream_index = 1,
    schema = NULL,
    current_record_batch_reader = NULL,
    current_connection = NULL,
    conform_batch = NULL,
    saw_eos_sentinel = FALSE,

    initialize = function(
      streams,
      coerce_schema,
      current_stream_index,
      schema
    ) {
      self$streams <- streams
      self$coerce_schema <- coerce_schema
      self$current_stream_index <- current_stream_index
      self$schema <- schema
    },

    get_next_reader__ = function(offset = 0) {
      if (!is.null(self$current_connection)) {
        tryCatch(base::close(self$current_connection), error = function(e) {})
      }
      self$current_offset <- offset
      base_url <- generate_api_url('/readStreams')
      options(timeout = 3600)
      headers <- get_authorization_header()
      # Bind con before the tryCatch so the error handler can safely reference
      # it even if url() below fails before assigning it
      con <- NULL
      tryCatch(
        # suppressWarnings rather than a tryCatch warning handler — a warning
        # handler would abandon the rest of this block, leaving the reader
        # pointing at the previous stream
        suppressWarnings({
          con <- url(
            str_interp(
              '${base_url}/${self$streams[[self$current_stream_index]]$id}?offset=${self$current_offset}&eosSentinel=true'
            ),
            open = "rb",
            headers = headers,
            blocking = FALSE
          )
          stream_reader <- arrow::RecordBatchStreamReader$create(
            getNamespace("arrow")$MakeRConnectionInputStream(con)
          )

          # The eosSentinel query param asks the server to terminate the stream
          # with a zero-row sentinel batch, which lets us distinguish a
          # complete stream from one truncated exactly at a message boundary
          # (otherwise silent)
          self$saw_eos_sentinel <- FALSE

          # Batch readers have always passed batches through as they arrive
          # unless their types need converting (for dataset tables). Rebuilt
          # for every connection, since each stream can have its own columns.
          self$conform_batch <- if (self$coerce_schema) {
            make_batch_conformer(stream_reader$schema, self$schema, TRUE)
          } else {
            identity
          }

          self$current_record_batch_reader <- stream_reader
          self$current_connection <- con
        }),
        error = function(e) {
          # Close the connection from this failed attempt before retrying, so
          # failed attempts don't accumulate open connections
          if (!is.null(con)) {
            tryCatch(base::close(con), error = function(e2) {})
          }
          self$retry_count <- self$retry_count + 1
          if (self$retry_count > 10) {
            message(
              "Download connection failed after too many retries, giving up."
            )
            abort_redivis_network_error(conditionMessage(e))
          }
          retry_sleep(self$retry_count)
          return(self$get_next_reader__(self$current_offset))
        }
      )
    },

    read_next_batch = function() {
      tryCatch(
        {
          batch <- self$current_record_batch_reader$read_next_batch()

          if (is.null(batch) && !self$saw_eos_sentinel) {
            # The stream ended without the sentinel, meaning it was truncated.
            # This triggers the error handler below, which reconnects at
            # current_offset.
            stop(
              "Stream ended before the end-of-stream sentinel was received (truncated download)"
            )
          }

          if (!is.null(batch) && batch$num_rows == 0) {
            # A zero-row batch is the server's end-of-stream sentinel; it
            # carries no data
            self$saw_eos_sentinel <- TRUE
            return(self$read_next_batch())
          }

          if (is.null(batch)) {
            if (self$current_stream_index == length(self$streams)) {
              tryCatch(
                base::close(self$current_connection),
                error = function(e) {}
              )
              return(NULL)
            } else {
              self$current_stream_index <- self$current_stream_index + 1
              self$get_next_reader__()
              return(self$read_next_batch())
            }
          } else {
            batch <- self$conform_batch(batch)
            self$current_offset <- self$current_offset + batch$num_rows
            self$retry_count <- 0
            return(batch)
          }
        },
        error = function(e) {
          self$retry_count <- self$retry_count + 1
          if (self$retry_count > 10) {
            message(
              "Download connection failed after too many retries, giving up."
            )
            abort_redivis_network_error(conditionMessage(e))
          }
          retry_sleep(self$retry_count)
          self$get_next_reader__(self$current_offset)
          return(self$read_next_batch())
        }
      )
    },

    close = function() {
      tryCatch(base::close(self$current_connection), error = function(e) {})
    }
  )
)

# Reads at or below this estimated size go through table.listRows rather than
# a read session. See should_use_list_rows()
LIST_ROWS_MAX_BYTES <- 1e8

# Whether to read an instance's rows via table.listRows. listRows returns every
# row in a single request, avoiding the read session round trip: small reads
# don't meaningfully benefit from the parallelization read sessions provide,
# and listRows can also be read without credentials when the table is public.
#
# What matters is the size of the read, not of the table, so a capped read of
# a large table is sized by the fraction of its rows that were requested (the
# same approximation the server makes).
should_use_list_rows <- function(instance, max_results = NULL) {
  if (!inherits(instance, "Table")) {
    return(FALSE)
  }
  num_bytes <- instance$properties$numBytes
  if (is.null(num_bytes)) {
    return(FALSE)
  }
  num_bytes <- as.numeric(num_bytes)
  num_rows <- as.numeric(instance$properties$numRows %||% 0)
  if (!is.null(max_results) && num_rows > 0) {
    num_bytes <- num_bytes * min(max_results, num_rows) / num_rows
  }
  num_bytes < LIST_ROWS_MAX_BYTES
}

get_expected_list_rows <- function(instance, max_results = NULL) {
  num_rows <- instance$properties$numRows
  if (is.null(num_rows)) {
    return(NULL)
  }
  num_rows <- as.numeric(num_rows)
  if (is.null(max_results)) num_rows else min(num_rows, max_results)
}

# Whether an error from a read could plausibly succeed on a retry: network
# failures and truncated responses. API errors aren't: a 503 is the only
# transient one, and make_request() has already retried it. That covers 503s
# at any point in a read, since a status only arrives ahead of a response's
# body: a failure partway through the body is a network error, and the request
# made to recover from it gets make_request()'s 503 retries of its own.
is_retryable_error <- function(e) {
  !inherits(e, "redivis_error")
}

# Fetches rows via table.listRows, returning list(schema, batches).
#
# listRows can't be resumed and has no end-of-stream sentinel, so rather than
# streaming it, the whole response is fetched and its row count checked before
# any of it is used: an interrupted or truncated response is simply requested
# again, and no rows are ever handed to the caller twice. Reads are bounded by
# LIST_ROWS_MAX_BYTES, so holding the response in memory is fine.
fetch_list_rows <- function(
  uri,
  selected_variable_names = NULL,
  max_results = NULL,
  expected_rows = NULL
) {
  query <- list(format = "arrow")
  if (!is.null(selected_variable_names)) {
    query$selectedVariables <- paste(
      unlist(selected_variable_names),
      collapse = ","
    )
  }
  if (!is.null(max_results)) {
    query$maxResults <- max_results
  }

  retry_count <- 0
  repeat {
    result <- tryCatch(
      {
        res <- make_request(
          method = "GET",
          path = str_interp("${uri}/rows"),
          query = query,
          parse_response = FALSE
        )
        reader <- arrow::RecordBatchStreamReader$create(
          arrow::buffer(httr2::resp_body_raw(res))
        )
        batches <- list()
        rows_read <- 0
        while (!is.null(batch <- reader$read_next_batch())) {
          batches[[length(batches) + 1]] <- batch
          rows_read <- rows_read + batch$num_rows
        }
        if (!is.null(expected_rows) && rows_read < expected_rows) {
          stop(str_interp(
            "Received ${rows_read} of ${expected_rows} rows (truncated download)"
          ))
        }
        list(schema = reader$schema, batches = batches)
      },
      error = function(e) e
    )

    if (!inherits(result, "error")) {
      return(result)
    }
    if (!is_retryable_error(result)) {
      stop(result)
    }
    retry_count <- retry_count + 1
    if (retry_count > 10) {
      abort_redivis_network_error(
        "Download connection failed after too many retries",
        original_exception = result
      )
    }
    retry_sleep(retry_count)
  }
}

# The listRows counterpart to the read session path in make_rows_request()
read_list_rows <- function(
  instance,
  uri,
  max_results,
  selected_variable_names,
  type,
  variables,
  coerce_schema,
  batch_preprocessor
) {
  schema <- get_arrow_schema(variables)
  result <- fetch_list_rows(
    uri,
    selected_variable_names = selected_variable_names,
    max_results = max_results,
    expected_rows = get_expected_list_rows(instance, max_results)
  )
  if (type == 'arrow_stream') {
    # Like RedivisBatchReader, only convert batches whose types need it
    batches <- if (coerce_schema) {
      conform_batch <- make_batch_conformer(result$schema, schema, TRUE)
      lapply(result$batches, conform_batch)
    } else {
      result$batches
    }
    return(BufferedBatchReader$new(batches, schema))
  }

  conform_batch <- make_batch_conformer(result$schema, schema, coerce_schema)
  batches <- lapply(result$batches, conform_batch)
  if (!is.null(batch_preprocessor)) {
    batches <- Filter(Negate(is.null), lapply(batches, batch_preprocessor))
  }

  tbl <- if (length(batches)) {
    do.call(arrow::arrow_table, batches)
  } else {
    arrow::arrow_table(schema = schema)
  }

  if (type == 'arrow_dataset') {
    folder <- file.path(get_temp_dir(), "tables", uuid::UUIDgenerate())
    dir.create(folder, recursive = TRUE)
    arrow::write_feather(tbl, file.path(folder, "rows.feather"))
    return(arrow::open_dataset(folder, format = "feather"))
  }

  arrow_table_to_type(tbl, type)
}

# A batch reader over batches already in memory, with the same interface as
# RedivisBatchReader. See fetch_list_rows()
BufferedBatchReader <- R6::R6Class(
  "BufferedBatchReader",
  public = list(
    batches = NULL,
    schema = NULL,
    index = 0,

    initialize = function(batches, schema) {
      self$batches <- batches
      self$schema <- schema
    },

    read_next_batch = function() {
      if (self$index >= length(self$batches)) {
        return(NULL)
      }
      self$index <- self$index + 1
      self$batches[[self$index]]
    },

    close = function() {
      self$batches <- list()
      invisible(NULL)
    }
  )
)

arrow_table_to_type <- function(tbl, type) {
  if (type == 'arrow_table') {
    tbl
  } else if (type == 'tibble') {
    tibble::as_tibble(tbl)
  } else if (type == 'data_frame') {
    as.data.frame(tbl)
  } else if (type == 'data_table') {
    data.table::as.data.table(tbl)
  }
}

# Returns a function that conforms a batch read from the API (whose stream has
# `stream_schema`) to `schema`. Dataset tables (coerce_schema) may store values
# with a different type than their logical one, so their time columns are cast,
# and columns missing from the stream are added as nulls (e.g. an unreleased
# table made up of uploads with inconsistent variables). Columns are also
# reordered to match `schema`.
make_batch_conformer <- function(stream_schema, schema, coerce_schema) {
  fields_to_add <- list()
  should_reorder_fields <- FALSE
  time_variables_to_coerce <- c()

  i <- 0
  # Only consider the fields in the stream, since the stream may lack some
  for (field in stream_schema$fields) {
    i <- i + 1
    if (coerce_schema && is(schema[[field$name]]$type, "Time64")) {
      time_variables_to_coerce <- append(time_variables_to_coerce, field$name)
    }
    if (!should_reorder_fields && i != match(field$name, names(schema))) {
      should_reorder_fields <- TRUE
    }
  }

  for (field_name in schema$names) {
    if (is.null(stream_schema$GetFieldByName(field_name))) {
      fields_to_add <- append(fields_to_add, schema$GetFieldByName(field_name))
    }
  }

  if (!should_reorder_fields && length(fields_to_add)) {
    should_reorder_fields <- TRUE
  }

  function(batch) {
    if (coerce_schema) {
      # Note: this approach is much more performant than using %>% mutate(across())
      # TODO: in the future, Arrow may support native conversion from time string to their type
      # To test if supported: arrow::arrow_array(rep('10:30:04.123', 2))$cast(arrow::time64(unit="us"))
      for (time_variable in time_variables_to_coerce) {
        if (!is(batch[[time_variable]]$type, 'Time64')) {
          batch[[time_variable]] <- arrow::arrow_array(stringr::str_c(
            '2000-01-01T',
            batch[[time_variable]]$as_vector()
          ))$cast(arrow::timestamp(unit = 'us'))
        }
      }

      # Add all missing fields at once to avoid repeated AddColumn copies
      if (length(fields_to_add) > 0) {
        null_columns <- lapply(fields_to_add, function(field) {
          if (is(field$type, "Date32")) {
            arrow::arrow_array(rep(
              NA_integer_,
              batch$num_rows
            ))$cast(arrow::date32())
          } else if (is(field$type, "Time64")) {
            arrow::arrow_array(rep(
              NA_integer_,
              batch$num_rows
            ))$cast(arrow::int64())$cast(arrow::time64())
          } else if (is(field$type, "Float64")) {
            arrow::arrow_array(rep(NA_real_, batch$num_rows))
          } else if (is(field$type, "Boolean")) {
            arrow::arrow_array(rep(NA, batch$num_rows))
          } else if (is(field$type, "Int64")) {
            arrow::arrow_array(rep(
              NA_integer_,
              batch$num_rows
            ))$cast(arrow::int64())
          } else if (is(field$type, "Timestamp")) {
            arrow::arrow_array(rep(
              NA_integer_,
              batch$num_rows
            ))$cast(arrow::int64())$cast(
              arrow::timestamp(unit = "us", timezone = "")
            )
          } else {
            arrow::arrow_array(rep(NA_character_, batch$num_rows))
          }
        })
        names(null_columns) <- sapply(fields_to_add, function(f) f$name)
        existing_columns <- setNames(
          lapply(batch$schema$names, function(name) batch[[name]]),
          batch$schema$names
        )
        all_columns <- c(existing_columns, null_columns)
        batch <- do.call(arrow::record_batch, all_columns[names(schema)])
      } else if (should_reorder_fields) {
        batch <- batch[, names(schema)] # reorder fields
      }

      batch <- arrow::as_record_batch(batch, schema = schema)
    } else if (should_reorder_fields) {
      batch <- batch[, names(schema)] # reorder fields
      batch <- arrow::as_record_batch(batch, schema = schema)
    }
    batch
  }
}


#' @include util.R
make_rows_request <- function(
  uri,
  max_results = NULL,
  selected_variable_names = NULL,
  type = 'tibble',
  variables = NULL,
  progress = TRUE,
  coerce_schema = FALSE,
  batch_preprocessor = NULL,
  instance = NULL,
  use_export_api = FALSE,
  max_parallelization = parallelly::availableCores()
) {
  if (inherits(instance, "ReadStream")) {
    read_session <- list(
      streams = list(instance),
      numRows = instance$properties$estimatedRows
    )
  } else {
    payload = list(
      "requestedStreamCount" = if (type == 'arrow_stream') {
        1
      } else {
        min(8, max_parallelization)
      },
      format = "arrow"
    )

    if (!is.null(max_results)) {
      payload$maxResults = max_results
    }
    if (!is.null(selected_variable_names)) {
      payload$selectedVariables = selected_variable_names
    }

    arrow::set_cpu_count(max_parallelization)
    arrow::set_io_thread_count(max(max_parallelization, 2))
    # Any table, query, or upload can be exported, via instance$download()
    use_export_api <- use_export_api &&
      type != 'arrow_stream' &&
      is.null(selected_variable_names) &&
      is.null(batch_preprocessor) &&
      is.null(max_results)
    use_list_rows <- !use_export_api &&
      should_use_list_rows(instance, max_results)

    if (use_list_rows) {
      return(read_list_rows(
        instance,
        uri,
        max_results = max_results,
        selected_variable_names = selected_variable_names,
        type = type,
        variables = variables,
        coerce_schema = coerce_schema,
        batch_preprocessor = batch_preprocessor
      ))
    }

    if (!use_export_api) {
      read_session <- make_request(
        method = "post",
        path = str_interp("${uri}/readSessions"),
        parse_response = TRUE,
        payload = payload
      )
    }
  }

  if (type == 'arrow_stream') {
    reader = RedivisBatchReader$new(
      streams = read_session$streams,
      coerce_schema = coerce_schema,
      current_stream_index = 1,
      schema = get_arrow_schema(variables)
    )
    reader$get_next_reader__()

    return(reader)
  }

  folder <- NULL
  if (
    type == 'arrow_dataset' ||
      use_export_api ||
      # We need to always write to disk if we're doing things in parallel,
      # because we can't efficiently copy results between processes when working in parallel
      (length(read_session$streams) > 1 &&
        max_parallelization > 1)
  ) {
    folder <- file.path(get_temp_dir(), "tables", uuid::UUIDgenerate())
    dir.create(folder, recursive = TRUE)

    if (type != 'arrow_dataset') {
      on.exit(unlink(folder, recursive = TRUE), add = TRUE)
    }
  }

  if (use_export_api) {
    instance$download(folder, format = 'parquet', progress = progress)
  } else if (progress) {
    # avoid any progressr warnings
    result <- suppressWarnings(progressr::with_progress(
      parallel_stream_arrow(
        folder,
        read_session$streams,
        max_results = read_session$numRows,
        variables,
        coerce_schema,
        batch_preprocessor,
        max_parallelization
      )
    ))
  } else {
    result <- parallel_stream_arrow(
      folder,
      read_session$streams,
      max_results = read_session$numRows,
      variables,
      coerce_schema,
      batch_preprocessor,
      max_parallelization
    )
  }

  if (!is.null(folder)) {
    ds <- arrow::open_dataset(
      folder,
      format = if (use_export_api) "parquet" else "feather",
      schema = if (is.null(batch_preprocessor) && !use_export_api) {
        get_arrow_schema(variables)
      } else {
        NULL
      }
    )

    if (type == 'arrow_dataset') {
      return(ds)
    } else {
      if (type == 'arrow_table') {
        arrow::as_arrow_table(ds)
      } else if (type == 'tibble') {
        tibble::as_tibble(ds)
      } else if (type == 'data_frame') {
        as.data.frame(ds)
      } else if (type == 'data_table') {
        # This is the most performant way to get a data.table
        # We need to temporarily set use_altrep to FALSE, since when set to TRUE (the default), it defers materialization of most values,
        # but then materialization happens anyways on data.table::setDT(). It's much faster to do this during the initial arrow conversion.
        old <- options(arrow.use_altrep = FALSE)
        on.exit(options(old), add = TRUE)
        df <- as.data.frame(ds)
        data.table::setDT(df)
        df
      }
    }
  } else {
    arrow_table_to_type(result, type)
  }
}

parallel_stream_arrow <- function(
  folder,
  streams,
  max_results,
  variables,
  coerce_schema,
  batch_preprocessor,
  max_parallelization
) {
  if (!length(streams)) {
    if (is.null(folder)) {
      return(arrow::arrow_table(schema = get_arrow_schema(variables)))
    } else {
      # Write an empty feather file so the dataset has the correct schema
      empty_table <- arrow::arrow_table(schema = get_arrow_schema(variables))
      arrow::write_feather(empty_table, file.path(folder, "empty.feather"))
      return()
    }
  }

  pb <- NULL
  pb_multiplier <- 0
  if (!is.null(max_results) && max_results > 0) {
    pb_multiplier <- 100 / max_results
    pb <- progressr::progressor(steps = 100)
  }

  worker_count <- max(
    1,
    min(
      8,
      length(streams),
      parallelly::availableCores(),
      max_parallelization
    )
  )

  if (length(streams) == 1) {
    # Don't use parallelization if we only have one stream, to avoid the overhead of spawning a future and copying data
    result <- process_arrow_stream(
      streams[[1]],
      folder,
      variables,
      coerce_schema,
      batch_preprocessor,
      pb,
      pb_multiplier,
      is_subprocess = FALSE
    )
    if (is.null(folder)) {
      if (length(result) == 0) {
        return(arrow::arrow_table(schema = get_arrow_schema(variables)))
      }
      return(do.call(arrow::arrow_table, result))
    } else {
      return()
    }
  }

  # Parallely is returning True here in positron for Mac, but multicore still doesn't work
  # TODO: resolve / validate at a later point
  if (parallelly::supportsMulticore() && Sys.info()[["sysname"]] != "Darwin") {
    oplan <- future::plan(future::multicore, workers = worker_count)
  } else {
    oplan <- future::plan(future::multisession, workers = worker_count)
    # Helpful for testing in dev
    # oplan <- future::plan(future::sequential)
  }

  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(future::plan(oplan), add = TRUE)

  # Need a local variable for parallelization to work
  .process_arrow_stream <- process_arrow_stream
  results <- furrr::future_map(streams, function(stream) {
    .process_arrow_stream(
      stream,
      folder,
      variables,
      coerce_schema,
      batch_preprocessor,
      pb,
      pb_multiplier,
      is_subprocess = length(streams) > 1
    )
  })

  if (is.null(folder)) {
    if (length(unlist(results, recursive = FALSE)) == 0) {
      return(arrow::arrow_table(schema = get_arrow_schema(variables)))
    }
    return(do.call(arrow::arrow_table, unlist(results, recursive = FALSE)))
  }
}

process_arrow_stream <- function(
  stream,
  folder,
  variables,
  coerce_schema,
  batch_preprocessor,
  pb,
  pb_multiplier,
  is_subprocess,
  in_memory_batches = c(),
  stream_writer = NULL,
  output_file = NULL,
  stream_rows_read = 0,
  retry_count = 0
) {
  base_url = generate_api_url('/readStreams')
  headers <- get_authorization_header()
  schema <- get_arrow_schema(variables)
  # Offset at the start of any file created by this invocation, so its rows can
  # be re-fetched if the file can't be finalized after an error
  file_start_offset <- stream_rows_read
  output_file_path <- NULL
  # Bind con before the tryCatch so the error handler can safely reference it
  # even if url() below fails before assigning it
  con <- NULL

  tryCatch(
    {
      # This ensures the url method doesn't time out after 60s. Only applies to this function, doesn't set globally
      # IMPORTANT: if not set, and the download exceeds the default 60s limit, we end up w/ corrupted feather files that can't be read
      options(timeout = 3600)
      con <- url(
        str_interp(
          '${base_url}/${stream$id}?offset=${stream_rows_read}&eosSentinel=true'
        ),
        open = "rb",
        headers = headers,
        blocking = FALSE
      )
      # close() errors on an already-closed connection, and the error handler
      # below closes it before retrying
      on.exit(tryCatch(close(con), error = function(e) {}), add = TRUE)
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace(
        "arrow"
      )$MakeRConnectionInputStream(con))

      # The eosSentinel query param asks the server to terminate the stream
      # with a zero-row sentinel batch, which lets us distinguish a complete
      # stream from one truncated exactly at a message boundary (otherwise
      # silent)
      saw_eos_sentinel <- FALSE

      if (!is.null(folder) && is.null(output_file)) {
        retry_suffix <- if (stream_rows_read == 0) {
          ""
        } else {
          str_interp("-retry_offset-${stream_rows_read}")
        }
        output_file_path <- file.path(
          folder,
          paste0(stream$id, retry_suffix, ".feather")
        )
        output_file <- arrow::FileOutputStream$create(output_file_path)
        if (is.null(batch_preprocessor)) {
          # Create the writer eagerly so the file is a valid Arrow file (with
          # magic bytes and footer) once closed, even if zero batches arrive —
          # e.g. when resuming from an offset at the end of the stream.
          # With a batch_preprocessor the schema isn't known until the first
          # batch, so the writer stays lazy and an empty file is removed below.
          stream_writer <- arrow::RecordBatchFileWriter$create(
            output_file,
            schema = schema
          )
        }
      }

      conform_batch <- make_batch_conformer(
        stream_reader$schema,
        schema,
        coerce_schema
      )

      last_measured_time <- proc.time()[3]
      current_progress_rows <- 0

      while (TRUE) {
        batch <- stream_reader$read_next_batch()
        if (is.null(batch)) {
          if (!saw_eos_sentinel) {
            # The stream ended without the sentinel, meaning it was truncated.
            # This triggers the retryable error handler below, which resumes
            # from stream_rows_read.
            stop(
              "Stream ended before the end-of-stream sentinel was received (truncated download)"
            )
          }
          break
        } else if (batch$num_rows == 0) {
          # A zero-row batch is the server's end-of-stream sentinel; it
          # carries no data
          saw_eos_sentinel <- TRUE
        } else {
          batch_rows <- batch$num_rows
          batch <- conform_batch(batch)

          if (!is.null(batch_preprocessor)) {
            batch <- batch_preprocessor(batch)
          }

          if (!is.null(batch)) {
            if (!is.null(output_file)) {
              if (is.null(stream_writer)) {
                # Only reachable with a batch_preprocessor; otherwise the
                # writer was created eagerly above
                stream_writer <- arrow::RecordBatchFileWriter$create(
                  output_file,
                  schema = batch$schema
                )
              }
              stream_writer$write_batch(batch)
            } else {
              in_memory_batches <- c(in_memory_batches, batch)
            }
          }

          # Only count these rows towards the resume offset after they've been
          # written, so a retry after a failed write doesn't skip them
          stream_rows_read <- stream_rows_read + batch_rows
          current_progress_rows <- current_progress_rows + batch_rows

          if (proc.time()[3] - last_measured_time > 0.2) {
            # Yield to R's event loop to check for pending interrupts. Only need to do if on the main thread
            # This only sort of works - need to hit the Jupyter "stop" button many times, but better than nothing
            # TODO: This doesn't really work right now, and adds overhead. A longer Sys.sleep improves it, but adds more overhead.
            #       Disabling for now, only relevant for single-stream downloads, which only meaningfully happens with max_results set
            # if (!is_subprocess) {
            #   Sys.sleep(0.01)
            # }

            if (!is.null(pb)) {
              pb(amount = current_progress_rows * pb_multiplier)
            }

            current_progress_rows <- 0
            last_measured_time <- proc.time()[3]
          }
        }
        retry_count <- 0
      }

      if (!is.null(pb)) {
        pb(amount = current_progress_rows * pb_multiplier)
      }

      if (is.null(output_file)) {
        return(in_memory_batches)
      } else {
        if (!is.null(stream_writer)) {
          stream_writer$close()
          output_file$close()
        } else {
          # The batch_preprocessor filtered out every batch: nothing was
          # written, so the empty file isn't a valid Arrow file
          output_file$close()
          if (!is.null(output_file_path)) {
            unlink(output_file_path)
          }
        }
      }
    },
    error = function(e) {
      if (
        grepl("IOError", conditionMessage(e)) ||
          grepl("cannot read from connection", conditionMessage(e)) ||
          grepl("cannot open the connection", conditionMessage(e)) ||
          grepl("end-of-stream sentinel", conditionMessage(e))
      ) {
        if (retry_count > 10) {
          message(
            "Download connection failed after too many retries, giving up."
          )
          abort_redivis_network_error(conditionMessage(e))
        }
        # Close the failed connection now, rather than accumulating open
        # connections across nested retries
        if (!is.null(con)) {
          tryCatch(close(con), error = function(e2) {})
        }
        if (!is.null(stream_writer)) {
          # Closing the writer writes the file footer, making the rows written
          # so far readable; the retry below then resumes from stream_rows_read.
          # If the footer can't be written the file is invalid, so remove it
          # and re-fetch all of its rows from the file's starting offset.
          footer_written <- tryCatch(
            {
              stream_writer$close()
              TRUE
            },
            error = function(e2) FALSE
          )
          tryCatch(output_file$close(), error = function(e2) {})
          if (!footer_written) {
            if (!is.null(output_file_path)) {
              unlink(output_file_path)
            }
            stream_rows_read <- file_start_offset
          }
        } else if (!is.null(output_file)) {
          # No batches were written, so the empty file isn't a valid Arrow file
          tryCatch(output_file$close(), error = function(e2) {})
          if (!is.null(output_file_path)) {
            unlink(output_file_path)
          }
        }
        retry_sleep(retry_count)
        return(process_arrow_stream(
          stream,
          folder,
          variables,
          coerce_schema,
          batch_preprocessor,
          pb,
          pb_multiplier,
          is_subprocess,
          in_memory_batches,
          stream_writer = NULL,
          output_file = NULL,
          stream_rows_read,
          retry_count = retry_count + 1
        ))
      } else {
        abort_redivis_network_error(conditionMessage(e))
      }
    }
  )
}
