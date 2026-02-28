RedivisBatchReader <- R6::R6Class(
  "RedivisBatchReader",
  public = list(
    streams = NULL,
    current_offset = 0,
    coerce_schema = FALSE,
    retry_count = 0,
    current_stream_index = 1,
    time_variables = NULL,
    schema = NULL,
    writer_schema = NULL,
    current_record_batch_reader = NULL,
    current_connection = NULL,

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
      self$time_variables <- list()
    },

    get_next_reader__ = function(offset = 0) {
      self$current_offset <- offset
      base_url <- generate_api_url('/readStreams')
      options(timeout = 3600)
      headers <- get_authorization_header()
      tryCatch(
        {
          con <- url(
            str_interp(
              '${base_url}/${self$streams[[self$current_stream_index]]$id}?offset=${self$current_offset}'
            ),
            open = "rb",
            headers = headers,
            blocking = FALSE
          )
          stream_reader <- arrow::RecordBatchStreamReader$create(
            getNamespace("arrow")$MakeRConnectionInputStream(con)
          )

          writer_schema_fields <- list()

          # Only get the fields in the reader, to handle when reading from an
          # unreleased table made up of uploads with inconsistent variables
          for (field_name in stream_reader$schema$names) {
            writer_schema_fields <- append(
              writer_schema_fields,
              self$schema$GetFieldByName(field_name)
            )
          }

          if (self$coerce_schema) {
            for (field in writer_schema_fields) {
              if (is(field$type, "Time64")) {
                self$time_variables <- append(self$time_variables, field$name)
              }
            }
          }

          self$writer_schema <- arrow::schema(writer_schema_fields)
          self$current_record_batch_reader <- stream_reader
          self$current_connection <- con
        },
        warning = function(w) {},
        error = function(e) {
          self$retry_count <- self$retry_count + 1
          if (self$retry_count > 10) {
            message(
              "Download connection failed after too many retries, giving up."
            )
            abort_redivis_network_error(conditionMessage(e))
          }
          Sys.sleep(self$retry_count)
          return(self$get_next_reader__(self$current_offset))
        }
      )
    },

    read_next_batch = function() {
      tryCatch(
        {
          batch <- self$current_record_batch_reader$read_next_batch()

          if (is.null(batch)) {
            if (self$current_stream_index == length(self$streams)) {
              return(NULL)
            } else {
              self$current_stream_index <- self$current_stream_index + 1
              self$get_next_reader__()
              return(self$read_next_batch())
            }
          } else {
            if (self$coerce_schema) {
              # Note: this approach is much more performant than using %>% mutate(across())
              # TODO: in the future, Arrow may support native conversion from time string to their type
              for (time_variable in self$time_variables) {
                if (!is(batch[[time_variable]]$type, 'Time64')) {
                  batch[[time_variable]] <- arrow::arrow_array(stringr::str_c(
                    '2000-01-01T',
                    batch[[time_variable]]$as_vector()
                  ))$cast(arrow::timestamp(unit = 'us'))
                }
              }

              batch <- arrow::as_record_batch(
                batch,
                schema = self$writer_schema
              )
            }
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
          Sys.sleep(self$retry_count)
          self$get_next_reader__(self$current_offset)
          return(self$read_next_batch())
        }
      )
    },

    close = function() {
      base::close(self$current_connection)
    }
  )
)


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
  print('v1')
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
    use_export_api <- use_export_api &&
      inherits(instance, "table") &&
      type != 'arrow_stream' &&
      is.null(selected_variable_names) &&
      is.null(batch_preprocessor) &&
      is.null(max_results)

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
    folder <- str_interp('${get_temp_dir()}/tables/${uuid::UUIDgenerate()}')
    dir.create(folder, recursive = TRUE)

    if (type != 'arrow_dataset') {
      on.exit(unlink(folder, recursive = TRUE), add = TRUE)
    }
  }

  if (use_export_api) {
    instance$download(folder, format = 'parquet', progress = progress)
  } else if (progress) {
    result <- progressr::with_progress(
      parallel_stream_arrow(
        folder,
        read_session$streams,
        max_results = read_session$numRows,
        variables,
        coerce_schema,
        batch_preprocessor,
        max_parallelization
      )
    )
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
        data.table::as.data.table(ds)
      }
    }
  } else {
    if (type == 'arrow_table') {
      return(result)
    } else if (type == 'tibble') {
      return(tibble::as_tibble(result))
    } else if (type == 'data_frame') {
      return(as.data.frame(result))
    } else if (type == 'data_table') {
      return(data.table::as.data.table(result))
    }
  }
}

get_on_progress_callback <- function(total) {
  if (is.null(total)) {
    return(function(amount, final = FALSE) {})
  }
  if (FALSE) {} else {
    multiplier <- 100 / total
    pb <- progressr::progressor(steps = 100)
    cached_amount <- 0
    last_timestamp <- proc.time()[3]
    return(
      function(amount, final = FALSE) {
        if (proc.time()[3] - last_timestamp > 0.2 || final) {
          pb(amount = (cached_amount + amount) * multiplier)
          cached_amount <<- 0
          last_timestamp <<- proc.time()[3]
        } else {
          cached_amount <<- cached_amount + amount
        }
      }
    )
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
    return()
  }
  pb <- NULL
  if (!is.null(max_results)) {
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

  # Parallely is returning True here in positron for Mac, but multicore still doesn't work
  # TODO: resolve / validate at a later point
  if (
    parallelly::supportsMulticore() &&
      Sys.info()[["sysname"]] != "Darwin" &&
      FALSE
  ) {
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
  futures <- lapply(streams, function(stream) {
    future::future({
      .process_arrow_stream(
        stream,
        folder,
        variables,
        coerce_schema,
        batch_preprocessor,
        pb,
        pb_multiplier
      )
    })
  })

  # Resolve futures with interrupt checking — Sys.sleep is an interrupt point
  # in R, allowing Jupyter's stop button (which sends SIGINT to the main
  # process) to actually cancel execution.
  results <- lapply(futures, function(f) {
    while (!future::resolved(f)) {
      Sys.sleep(0.1)
    }
    future::value(f)
  })

  if (is.null(folder)) {
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
  in_memory_batches = c(),
  stream_writer = NULL,
  output_file = NULL,
  stream_rows_read = 0,
  retry_count = 0
) {
  base_url = generate_api_url('/readStreams')
  headers <- get_authorization_header()
  schema <- get_arrow_schema(variables)

  # Workaround for self-signed certs in dev
  # h <- curl::new_handle()
  # auth = get_authorization_header()
  # curl::handle_setheaders(h, .list=auth)
  # if (Sys.getenv("REDIVIS_API_ENDPOINT") == "https://localhost:8443/api/v1"){
  #   curl::handle_setopt(h, "ssl_verifypeer"=0L)
  # }
  # url <- str_interp('${base_url}/${stream$id}')
  # con <- curl::curl(url, handle=h)
  # open(con, "rb")

  tryCatch(
    {
      con <- url(
        str_interp('${base_url}/${stream$id}?offset=${stream_rows_read}'),
        open = "rb",
        headers = headers,
        blocking = FALSE
      )
      on.exit(close(con), add = TRUE)
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace(
        "arrow"
      )$MakeRConnectionInputStream(con))

      retry_count <- 0

      if (!is.null(folder) && is.null(output_file)) {
        output_file <- arrow::FileOutputStream$create(str_interp(
          '${folder}/${stream$id}.feather'
        ))
      }

      fields_to_add <- list()
      should_reorder_fields <- FALSE
      time_variables_to_coerce = c()

      i <- 0
      # Make sure to first only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
      for (field in stream_reader$schema$fields) {
        i <- i + 1
        if (coerce_schema && is(schema[[field$name]]$type, "Time64")) {
          time_variables_to_coerce <- append(
            time_variables_to_coerce,
            field$name
          )
        }
        if (!should_reorder_fields && i != match(field$name, names(schema))) {
          should_reorder_fields <- TRUE
        }
      }

      for (field_name in schema$names) {
        if (is.null(stream_reader$schema$GetFieldByName(field_name))) {
          fields_to_add <- append(
            fields_to_add,
            schema$GetFieldByName(field_name)
          )
        }
      }

      if (!should_reorder_fields && length(fields_to_add)) {
        should_reorder_fields <- TRUE
      }

      last_measured_time <- proc.time()[3]
      current_progress_rows <- 0

      while (TRUE) {
        # Yield to R's event loop to check for pending interrupts (e.g.,
        # Jupyter's stop button). This is a no-op in terms of delay but
        # allows R_CheckUserInterrupt to fire.
        Sys.sleep(0)

        batch <- stream_reader$read_next_batch()
        if (is.null(batch)) {
          break
        } else {
          current_progress_rows <- current_progress_rows + batch$num_rows
          stream_rows_read <- stream_rows_read + batch$num_rows

          # We need to coerce_schema for all dataset tables, since their underlying storage type may not be the same as the logical type
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
              batch <- do.call(
                arrow::record_batch,
                all_columns[names(schema)]
              )
            } else if (should_reorder_fields) {
              batch <- batch[, names(schema)] # reorder fields
            }

            batch <- arrow::as_record_batch(batch, schema = schema)
          } else if (should_reorder_fields) {
            batch <- batch[, names(schema)] # reorder fields
            batch <- arrow::as_record_batch(batch, schema = schema)
          }

          if (!is.null(batch_preprocessor)) {
            batch <- batch_preprocessor(batch)
          }

          if (!is.null(batch)) {
            if (!is.null(output_file)) {
              if (is.null(stream_writer)) {
                stream_writer <- arrow::RecordBatchFileWriter$create(
                  output_file,
                  schema = if (is.null(batch_preprocessor)) {
                    schema
                  } else {
                    batch$schema
                  }
                )
              }
              stream_writer$write_batch(batch)
            } else {
              in_memory_batches <- c(in_memory_batches, batch)
            }
          }

          if (proc.time()[3] - last_measured_time > 0.2) {
            if (!is.null(pb)) {
              pb(amount = current_progress_rows * pb_multiplier)
            }

            current_progress_rows <- 0
            last_measured_time <- proc.time()[3]
          }
        }
      }

      if (!is.null(pb)) {
        pb(amount = current_progress_rows * pb_multiplier)
      }

      if (is.null(output_file)) {
        return(in_memory_batches)
      } else {
        if (!is.null(stream_writer)) {
          stream_writer$close()
        }
        output_file$close()
      }
    },
    warning = function(w) {},
    error = function(e) {
      if (grepl("cannot read from connection", conditionMessage(e))) {
        if (retry_count > 10) {
          message(
            "Download connection failed after too many retries, giving up."
          )
          abort_redivis_network_error(conditionMessage(e))
        }
        Sys.sleep(retry_count)
        return(process_arrow_stream(
          stream,
          folder,
          variables,
          coerce_schema,
          batch_preprocessor,
          pb,
          pb_multiplier,
          in_memory_batches,
          stream_writer,
          output_file,
          stream_rows_read,
          retry_count = retry_count + 1
        ))
      } else {
        abort_redivis_network_error(conditionMessage(e))
      }
    }
  )
}
