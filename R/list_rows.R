RedivisBatchReader <- setRefClass(
  "RedivisBatchReader",
  fields = list(
    streams = "list",
    current_offset = "numeric",
    coerce_schema = "logical",
    retry_count = "numeric",
    # Can't figure out how to pass non-built-in classes here, so just pass as list
    custom_classes = "list",
    current_stream_index = "numeric",
    time_variables = "list"
  ),

  methods = list(
    get_next_reader__ = function(offset = 0) {
      .self$current_offset = offset
      base_url = generate_api_url('/readStreams')
      options(timeout = 3600)
      headers <- get_authorization_header()
      tryCatch(
        {
          con <- url(
            str_interp(
              '${base_url}/${streams[[.self$current_stream_index]]$id}?offset=${.self$current_offset}'
            ),
            open = "rb",
            headers = headers,
            blocking = FALSE
          )
          stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace(
            "arrow"
          )$MakeRConnectionInputStream(con))

          writer_schema_fields <- list()

          # Make sure to only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
          for (field_name in stream_reader$schema$names) {
            writer_schema_fields <- append(
              writer_schema_fields,
              .self$custom_classes$schema$GetFieldByName(field_name)
            )
          }

          if (coerce_schema) {
            for (field in writer_schema_fields) {
              if (is(field$type, "Time64")) {
                .self$time_variables <- append(.self$time_variables, field$name)
              }
            }
          }

          .self$custom_classes = list(
            current_record_batch_reader = stream_reader,
            writer_schema = arrow::schema(writer_schema_fields),
            schema = .self$custom_classes$schema,
            current_connection = con
          )
        },
        warning = function(w) {},
        error = function(e) {
          .self$retry_count = .self$retry_count + 1
          if (.self$retry_count > 10) {
            message(
              "Download connection failed after too many retries, giving up."
            )
            abort_redivis_network_error(conditionMessage(e))
          }
          Sys.sleep(.self$retry_count)

          return(.self$get_next_reader__(.self$current_offset))
        }
      )
    },
    read_next_batch = function() {
      tryCatch(
        {
          batch <- .self$custom_classes$current_record_batch_reader$read_next_batch()

          if (is.null(batch)) {
            if (.self$current_stream_index == length(.self$streams)) {
              return(NULL)
            } else {
              .self$current_stream_index = .self$current_stream_index + 1
              .self$get_next_reader__()
              return(.self$read_next_batch())
            }
          } else {
            if (.self$coerce_schema) {
              # Note: this approach is much more performant than using %>% mutate(across())
              # TODO: in the future, Arrow may support native conversion from time string to their type
              for (time_variable in .self$time_variables) {
                if (!is(batch[[time_variable]]$type, 'Time64')) {
                  batch[[time_variable]] <- arrow::arrow_array(stringr::str_c(
                    '2000-01-01T',
                    batch[[time_variable]]$as_vector()
                  ))$cast(arrow::timestamp(unit = 'us'))
                }
              }

              batch <- (arrow::as_record_batch(
                batch,
                schema = .self$custom_classes$writer_schema
              ))
            }
            .self$current_offset <- .self$current_offset + batch$num_rows
            .self$retry_count <- 0
            return(batch)
          }
        },
        error = function(e) {
          .self$retry_count = .self$retry_count + 1
          if (.self$retry_count > 10) {
            message(
              "Download connection failed after too many retries, giving up."
            )
            abort_redivis_network_error(conditionMessage(e))
          }
          Sys.sleep(.self$retry_count)
          .self$get_next_reader__(.self$current_offset)
          return(.self$read_next_batch())
        }
      )
    },
    close = function() {
      base::close(.self$custom_classes$current_connection)
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
  table = NULL,
  use_export_api = FALSE,
  max_parallelization = parallelly::availableCores()
) {
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
    !is.null(table) &&
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

  if (type == 'arrow_stream') {
    reader = RedivisBatchReader$new(
      streams = read_session$streams,
      coerce_schema = coerce_schema,
      current_stream_index = 1,
      custom_classes = list(schema = get_arrow_schema(variables)),
      time_variables = list()
    )
    reader$get_next_reader__()

    return(reader)
  }

  folder <- NULL
  # Always write to disk for now to improve memory efficiency
  if (type == 'arrow_dataset' || TRUE) {
    folder <- str_interp('${get_temp_dir()}/tables/${uuid::UUIDgenerate()}')
    dir.create(folder, recursive = TRUE)

    if (type != 'arrow_dataset') {
      on.exit(unlink(folder, recursive = TRUE), add = TRUE)
    }
  }

  if (use_export_api) {
    table$download(folder, format = 'parquet', progress = progress)
  } else if (progress) {
    result <- progressr::with_progress(parallel_stream_arrow(
      folder,
      read_session$streams,
      max_results = read_session$numRows,
      variables,
      coerce_schema,
      batch_preprocessor,
      max_parallelization
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

  pb_multiplier <- 100 / max_results
  pb <- progressr::progressor(steps = 100)

  headers <- get_authorization_header()
  worker_count <- min(
    8,
    length(streams),
    parallelly::availableCores(),
    max_parallelization
  )

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
  base_url = generate_api_url('/readStreams')

  process_arrow_stream <- function(
    stream,
    in_memory_batches = c(),
    stream_writer = NULL,
    output_file = NULL,
    stream_rows_read = 0,
    retry_count = 0
  ) {
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
        # This ensures the url method doesn't time out after 60s. Only applies to this function, doesn't set globally
        options(timeout = 3600)
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

        last_measured_time <- Sys.time()
        current_progress_rows <- 0

        while (TRUE) {
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

              # TODO: this is a significant bottleneck. Can we make it faster, maybe call all at once?
              for (field in fields_to_add) {
                if (is(field$type, "Date32")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(
                      NA_integer_,
                      batch$num_rows
                    ))$cast(arrow::date32())
                  )
                } else if (is(field$type, "Time64")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(
                      NA_integer_,
                      batch$num_rows
                    ))$cast(arrow::int64())$cast(arrow::time64())
                  )
                } else if (is(field$type, "Float64")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(NA_real_, batch$num_rows))
                  )
                } else if (is(field$type, "Boolean")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(NA, batch$num_rows))
                  )
                } else if (is(field$type, "Int64")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(
                      NA_integer_,
                      batch$num_rows
                    ))$cast(arrow::int64())
                  )
                } else if (is(field$type, "Timestamp")) {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(
                      NA_integer_,
                      batch$num_rows
                    ))$cast(arrow::int64())$cast(arrow::timestamp(
                      unit = "us",
                      timezone = ""
                    ))
                  )
                } else {
                  batch <- batch$AddColumn(
                    0,
                    field,
                    arrow::arrow_array(rep(NA_character_, batch$num_rows))
                  )
                }
              }

              if (should_reorder_fields) {
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

            if (Sys.time() - last_measured_time > 0.2) {
              pb(amount = current_progress_rows * pb_multiplier)
              current_progress_rows <- 0
              last_measured_time = Sys.time()
            }
          }
        }

        pb(amount = current_progress_rows * pb_multiplier)

        if (is.null(output_file)) {
          # Need to serialize the table to pass between threads
          table <- do.call(arrow::arrow_table, in_memory_batches)
          serialized <- arrow::write_to_raw(table, format = "stream") # stream is much faster to read
          return(serialized)
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

  results <- furrr::future_map(streams, function(stream) {
    process_arrow_stream(stream)
  })

  if (is.null(folder)) {
    return(do.call(
      arrow::concat_tables,
      sapply(results, function(x) {
        arrow::read_ipc_stream(x, as_data_frame = FALSE)
      })
    ))
  }
}
