
#' @include auth.R
make_request <- function(method='GET', query=NULL, payload = NULL, parse_response=TRUE, path = "", download_path = NULL, download_overwrite = FALSE, as_stream=FALSE, headers_callback=NULL, get_download_path_callback=NULL, stream_callback = NULL, stop_on_error=TRUE){
  # IMPORTANT: if updating the function signature, make sure to also pass to the two scenarios where we retry on 401 status

  if (!is.null(stream_callback) || !is.null(get_download_path_callback)){
    h <- curl::new_handle()
    auth = get_authorization_header()
    curl::handle_setheaders(h, "Authorization"=auth[[1]])
    curl::handle_setopt(h, failonerror = 0)
    url <- generate_api_url(path)
    if (length(query) > 0){
      query_string <- paste(
        names(query),
        sapply(query, function(x){ URLencode(as.character(x), reserved = TRUE) }),
        sep = "=",
        collapse = "&"
      )
      url <- str_interp("${url}?${query_string}")
    }
    con <- curl::curl(url, handle=h)
    on.exit(close(con))

    tryCatch({
      open(con, "rb", blocking = FALSE)
      res_data <- curl::handle_data(h)
    }, error = function(e){
      res_data <- curl::handle_data(h)
      if (res_data$status_code >= 400){
        if (res_data$status_code == 401 && is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA)) && is.na(Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID", unset=NA))){
          refresh_credentials()
          return(make_request(method, query, payload, parse_response, path, download_path, download_overwrite, as_stream, headers_callback, get_download_path_callback, stream_callback, stop_on_error))
        }
        if (stop_on_error){
          stop(str_interp("Received HTTP status ${res_data$status_code} for path ${url}"))
        } else {
          return(NULL)
        }
      }
    })


    res_data <- curl::handle_data(h)

    headers <- parse_curl_headers(res_data)
    if (!is.null(headers_callback)){
      headers_callback(headers)
    }

    if (is.null(get_download_path_callback)){
      while(isIncomplete(con)){
        buf <- readBin(con, raw(), 16384)
        cb_res <- stream_callback(buf)
        if (!is.null(cb_res) && stream_callback(cb_res) == FALSE){
          break;
        }
      }
    } else {
      download_path <- get_download_path_callback(headers)
      file_con <- base::file(download_path, "w+b")
      on.exit(close(file_con), add=TRUE)
      while(isIncomplete(con)){
        writeBin(readBin(con, raw(), 16384), file_con)
      }
      return(download_path)
    }

  } else {
    handler <- httr::write_memory()
    if (!is.null(download_path)) handler <- httr::write_disk(download_path, overwrite = download_overwrite)
    else if (!is.null(stream_callback)) handler <- httr::write_stream(stream_callback)

    res <- httr::VERB(
      method,
      handler,
      url = generate_api_url(path),
      httr::add_headers(get_authorization_header()),
      query = query,
      body = payload,
      encode="json",
      httr::timeout(3600)
    )

    if (httr::status_code(res) == 401 && is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA)) && is.na(Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID", unset=NA))){
      refresh_credentials()
      return(make_request(method, query, payload, parse_response, path, download_path, download_overwrite, as_stream, get_download_path_callback, stream_callback, stop_on_error))
    }

    if (!parse_response && httr::status_code(res) < 400){
      return(res)
    }

    is_json <- FALSE
    response_content <- NULL

    if (method == "HEAD"){
      if (httr::status_code(res) >= 400){
        response_content <- jsonlite::fromJSON(URLdecode(httr::headers(res)$'x-redivis-error-payload'))
      }
    } else {
      response_content <- httr::content(res, as="text", encoding='UTF-8')

      if (!is.null(httr::headers(res)$'content-type') && stringr::str_starts(httr::headers(res)$'content-type', 'application/json') && (httr::status_code(res) >= 400 || parse_response)){
        is_json <- TRUE
        response_content <- jsonlite::fromJSON(response_content, simplifyVector = FALSE)
      }
    }

    if (httr::status_code(res) >= 400 && stop_on_error){
      if (is_json){
        stop(response_content$error$message)
      } else {
        stop(response_content)
      }
    } else {
      response_content
    }
  }
}

parse_curl_headers <- function(res_data){
  vec <- curl::parse_headers(res_data$headers)

  header_names <- purrr::map(vec, function(header) {
    tolower(strsplit(header, ':')[[1]][[1]])
  })
  header_contents <- purrr::map(vec, function(header) {
    split = strsplit(header, ':\\s+')[[1]]
    paste0(tail(split, -1), collapse=':')
  })
  headers <- purrr::set_names(header_contents, header_names)
}

perform_parallel_download <- function(paths, overwrite, get_download_path_from_headers, on_finish, stop_on_error=TRUE){
  pool <- curl::new_pool()
  handles = list()
  for (path in paths){
    h <- curl::new_handle()
    url <- generate_api_url(path)
    auth = get_authorization_header()
    curl::handle_setheaders(h, "Authorization"=auth[[1]])
    curl::handle_setopt(h, "url"=url)

    fail_fn <- function(e){
      stop(e)
    }
    curl::multi_add(h, fail = fail_fn, data = parallel_download_data_cb_factory(h, url, get_download_path_from_headers, overwrite, on_finish, stop_on_error), pool = pool)
  }
  curl::multi_run(pool=pool)
}

parallel_download_data_cb_factory <- function(h, url, get_download_path_from_headers, overwrite, on_finish, stop_on_error){
  file_con <- NULL
  handle <- h
  path <- url
  return(function(chunk, final){
    if (is.null(file_con)){
      res_data <- curl::handle_data(handle)
      status_code <- res_data$status_code
      if (status_code >= 400){
        if (stop_on_error){
          stop(str_interp("Received HTTP status ${status_code} for path ${path}"))
        } else {
          return(NULL)
        }
      }

      headers <- parse_curl_headers(res_data)
      download_path <- get_download_path_from_headers(headers)
      if (!overwrite && base::file.exists(download_path)){
        stop(str_interp("File already exists at '${download_path}'. Set parameter overwrite=TRUE to overwrite existing files."))
      }
      file_con <<- base::file(download_path, "w+b")
    }
    if (length(chunk)){
      writeBin(chunk, file_con)
    }
    if (final){
      on_finish()
      close(file_con)
    }
  })
}

make_paginated_request <- function(path, query=list(), page_size=100, max_results=NULL){
  page <- 0
  results <- list()
  next_page_token <- NULL

  while (TRUE){
    if (!is.null(max_results) && length(results) >= max_results){
      break
    }

    response = make_request(
      method="GET",
      path=path,
      parse_response=TRUE,
      query=append(
        query,
        list(
          "pageToken"= next_page_token,
          "maxResults"= if (is.null(max_results) || (page + 1) * page_size < max_results) page_size else max_results - page * page_size
        )
      )
    )

    page <- page + 1
    results <- append(results, response$results)
    next_page_token <- response$nextPageToken
    if (is.null(next_page_token)){
      break
    }

  }

  results
}

RedivisBatchReader <- setRefClass(
  "RedivisBatchReader",
  fields = list(
    streams = "list",
    coerce_schema="logical",
    # Can't figure out how to pass non-built-in classes here, so just pass as list
    custom_classes="list",
    current_stream_index="numeric",
    time_variables="list"
  ),

  methods = list(
    get_next_reader__ = function(){
      base_url = generate_api_url('/readStreams')

      options(timeout=3600)
      headers <- get_authorization_header()
      con <- url(str_interp('${base_url}/${streams[[.self$current_stream_index]]$id}'), open = "rb", headers = headers, blocking=FALSE)
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace("arrow")$MakeRConnectionInputStream(con))
      writer_schema_fields <- list()

      # Make sure to only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
      for (field_name in stream_reader$schema$names){
        writer_schema_fields <- append(writer_schema_fields, .self$custom_classes$schema$GetFieldByName(field_name))
      }

      if (coerce_schema){
        for (field in writer_schema_fields){
          if (is(field$type, "Time64")){
            .self$time_variables <- append(.self$time_variables, field$name)
          }
        }
      }

      .self$custom_classes = list(
        current_record_batch_reader=stream_reader,
        writer_schema=arrow::schema(writer_schema_fields),
        schema=.self$custom_classes$schema,
        current_connection=con
      )
    },
    read_next_batch = function() {
      batch <- .self$custom_classes$current_record_batch_reader$read_next_batch()

      if (is.null(batch)){
        if (.self$current_stream_index == length(.self$streams)){
          return(NULL)
        } else {
          .self$current_stream_index = .self$current_stream_index + 1
          .self$get_next_reader__()
          return(.self$read_next_batch())
        }
      } else {
        if (.self$coerce_schema){
          # Note: this approach is much more performant than using %>% mutate(across())
          # TODO: in the future, Arrow may support native conversion from time string to their type
          for (time_variable in .self$time_variables){
            batch[[time_variable]] <- arrow::arrow_array(stringr::str_c('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
          }

          batch <- (arrow::as_record_batch(batch, schema=.self$custom_classes$writer_schema))
        }

        return (batch)
      }
    },
    close = function(){
      base::close(.self$custom_classes$current_connection)
    }
  )
)


make_rows_request <- function(uri, max_results=NULL, selected_variable_names = NULL, type = 'tibble', variables = NULL, progress = TRUE, coerce_schema=FALSE, batch_preprocessor=NULL, table=NULL, use_export_api=FALSE){
  read_session <- make_request(
    method="post",
    path=str_interp("${uri}/readSessions"),
    parse_response=TRUE,
    payload=list(
        "maxResults"=max_results,
        "selectedVariables"=selected_variable_names,
        "format"="arrow",
        "requestedStreamCount"=if (type == 'arrow_stream') 1 else min(8, parallelly::availableCores())
    )
  )

  use_export_api <- use_export_api && !is.null(table) && type != 'arrow_stream' && is.null(selected_variable_names) && is.null(batch_preprocessor) && is.null(max_results)

  if (type == 'arrow_stream'){
    reader = RedivisBatchReader$new(
      streams=read_session$streams,
      coerce_schema=coerce_schema,
      current_stream_index=1,
      custom_classes=list(schema=get_arrow_schema(variables)),
      time_variables=list()
    )
    reader$get_next_reader__()

    return(reader)
  }

  folder <- NULL
  # Always write to disk for now to improve memory efficiency
  if (type == 'arrow_dataset' || TRUE ){
    folder <- str_interp('${tempdir()}/redivis/tables/${uuid::UUIDgenerate()}')
    dir.create(folder, recursive = TRUE)

    if (type != 'arrow_dataset'){
      on.exit(unlink(folder, recursive=TRUE))
    }
  }

  if (use_export_api){
    table$download(folder, format='parquet', progress=progress)
  } else if (progress){
    result <- progressr::with_progress(parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, variables, coerce_schema, batch_preprocessor))
  } else {
    result <- parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, variables, coerce_schema, batch_preprocessor)
  }

  ds <- arrow::open_dataset(folder, format = if (use_export_api) "parquet" else "feather", schema = if (is.null(batch_preprocessor) && !use_export_api) get_arrow_schema(variables) else NULL)

  # TODO: remove head() once BE is sorted
  if (type == 'arrow_dataset'){
    return(ds)
  } else {
    if (type == 'arrow_table'){
      arrow::as_arrow_table(ds)
    }else if (type == 'tibble'){
      tibble::as_tibble(ds)
    }else if (type == 'data_frame'){
      as.data.frame(ds)
    } else if (type == 'data_table'){
      data.table::as.data.table(ds)
    }
  }

}

generate_api_url <- function(path){
  str_interp('${if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1" else Sys.getenv("REDIVIS_API_ENDPOINT")}${utils::URLencode(path)}')
}

get_authorization_header <- function(){
  auth_token <- get_auth_token()
  c("Authorization"=str_interp("Bearer ${auth_token}"))
}


parallel_stream_arrow <- function(folder, streams, max_results, variables, coerce_schema, batch_preprocessor){
  if (!length(streams)){
    return();
  }

  pb <- progressr::progressor(steps = max_results)

  headers <- get_authorization_header()
  worker_count <- min(8, length(streams), parallelly::availableCores())

  if (parallelly::supportsMulticore()){
    oplan <- future::plan(future::multicore, workers = worker_count)
  } else {
    oplan <- future::plan(future::multisession, workers = worker_count)
    # Helpful for testing in dev
    # oplan <- future::plan(future::sequential)
  }

  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(future::plan(oplan), add = TRUE)
  base_url = generate_api_url('/readStreams')

  process_arrow_stream <- function(stream, in_memory_batches=c(), stream_writer=NULL, output_file=NULL, stream_rows_read=0, retry_count=0){
    schema <- get_arrow_schema(variables)
    schema_field_uncased_name_map <- sapply(schema$names, tolower)
    schema_field_uncased_name_map <- purrr::set_names(names(schema_field_uncased_name_map), schema_field_uncased_name_map)

    # Workaround for self-signed certs in dev
    # h <- curl::new_handle()
    # auth = get_authorization_header()
    # curl::handle_setheaders(h, "Authorization"=auth[[1]])
    # if (Sys.getenv("REDIVIS_API_ENDPOINT") == "https://localhost:8443/api/v1"){
    #   curl::handle_setopt(h, "ssl_verifypeer"=0L)
    # }
    # url <- str_interp('${base_url}/${stream$id}')
    # con <- curl::curl(url, handle=h)
    # open(con, "rb")

    tryCatch({
      # This ensures the url method doesn't time out after 60s. Only applies to this function, doesn't set globally
      options(timeout=3600)
      con <- url(str_interp('${base_url}/${stream$id}?offset=${stream_rows_read}'), open = "rb", headers = headers, blocking=FALSE)
      on.exit(close(con))
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace("arrow")$MakeRConnectionInputStream(con))

      if (!is.null(folder) && is.null(output_file)){
        output_file <- arrow::FileOutputStream$create(str_interp('${folder}/${stream$id}'))
      }

      fields_to_rename <- list()
      fields_to_add <- list()
      should_reorder_fields <- FALSE
      time_variables_to_coerce = c()

      i <- 0
      # Make sure to first only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
      for (field in stream_reader$schema$fields){
        i <- i+1
        final_schema_field_name <- schema_field_uncased_name_map[[tolower(field$name)]]

        if (final_schema_field_name != field$name){
          stream_reader_schema <- stream_reader$schema
          rename_config <- purrr::set_names(list(list(new_name=final_schema_field_name, index=i)), c(final_schema_field_name))
          fields_to_rename <- append(fields_to_rename, rename_config)
        }
        if (coerce_schema && is(field$type, "Time64")){
          time_variables_to_coerce <- append(time_variables_to_coerce, field$name)
        }
        if (!should_reorder_fields && i != match(final_schema_field_name, names(schema))){
          should_reorder_fields <- TRUE
        }
      }

      for (field_name in schema$names){
        if (is.null(stream_reader$schema$GetFieldByName(field_name)) && is.null(fields_to_rename[[field_name]])){
          fields_to_add <- append(fields_to_add, schema$GetFieldByName(field_name))
        }
      }

      if (should_reorder_fields && length(fields_to_add)){
        should_reorder_fields <- TRUE
      }

      last_measured_time <- Sys.time()
      current_progress_rows <- 0
      overread_stream_rows <- 0

      while (TRUE){
        batch <- stream_reader$read_next_batch()
        if (is.null(batch) || overread_stream_rows > 0){
          break
        } else {
          current_progress_rows <- current_progress_rows + batch$num_rows
          stream_rows_read <- stream_rows_read + batch$num_rows

          if (!is.null(stream$maxResults) && stream_rows_read > stream$maxResults){
            overread_stream_rows <- stream_rows_read - stream$maxResults
            current_progress_rows <- current_progress_rows - overread_stream_rows
          }

          # We need to coerce_schema for all dataset tables, since their underlying storage type is always a string
          if (coerce_schema){
            # Note: this approach is much more performant than using %>% mutate(across())
            # TODO: in the future, Arrow may support native coversion from time string to their type
            # To test if supported: arrow::arrow_array(rep('10:30:04.123', 2))$cast(arrow::time64(unit="us"))
            for (time_variable in time_variables_to_coerce){
              batch[[time_variable]] <- arrow::arrow_array(stringr::str_c('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
            }
            for (rename_args in fields_to_rename){
              names(batch)[[rename_args$index]] <- rename_args$new_name
            }
            # TODO: this is a significant bottleneck. Can we make it faster, maybe call all at once?
            for (field in fields_to_add){
              if (is(field$type, "Date32")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_integer_, batch$num_rows))$cast(arrow::date32()))
              } else if (is(field$type, "Time64")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_integer_, batch$num_rows))$cast(arrow::int64())$cast(arrow::time64()))
              } else if (is(field$type, "Float64")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_real_, batch$num_rows)))
              } else if (is(field$type, "Boolean")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA, batch$num_rows)))
              } else if (is(field$type, "Int64")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_integer_, batch$num_rows))$cast(arrow::int64()))
              } else if (is(field$type, "Timestamp")){
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_integer_, batch$num_rows))$cast(arrow::int64())$cast(arrow::timestamp(unit="us", timezone="")))
              } else {
                batch <- batch$AddColumn(0, field, arrow::arrow_array(rep(NA_character_, batch$num_rows)))
              }
            }

            if (should_reorder_fields){
              batch <- batch[, names(schema)] # reorder fields
            }

            batch <- arrow::as_record_batch(batch, schema=schema)
          } else if (should_reorder_fields){
            batch <- batch[, names(schema)] # reorder fields
            batch <- arrow::as_record_batch(batch, schema=schema)
          }

          if (!is.null(batch_preprocessor)){
            batch <- batch_preprocessor(batch)
          }

          if (!is.null(batch)){
            if (overread_stream_rows > 0){
              batch <- head(batch, batch$num_rows - overread_stream_rows)
            }
            if (!is.null(output_file)){
              if (is.null(stream_writer)){
                stream_writer <- arrow::RecordBatchFileWriter$create(output_file, schema=if (is.null(batch_preprocessor)) schema else batch$schema)
              }
              stream_writer$write_batch(batch)
            } else {
              in_memory_batches <- c(in_memory_batches, batch)
            }
          }

          if (Sys.time() - last_measured_time > 0.2){
            pb(amount = current_progress_rows)
            current_progress_rows <- 0
            last_measured_time = Sys.time()
          }
        }
      }

      pb(amount = current_progress_rows)

      if (is.null(output_file)){
        # Need to serialize the table to pass between threads
        table <- do.call(arrow::arrow_table, in_memory_batches)
        serialized <- arrow::write_to_raw(table, format="stream") # stream is much faster to read
        return(serialized)
      } else {
        if (!is.null(stream_writer)){
          stream_writer$close()
        }
        output_file$close()
      }
    }, error = function(cond){
      if (grepl("cannot read from connection", conditionMessage(cond))){
        if (retry_count > 10){
          message("Download connection failed after too many retries, giving up.")
          stop(conditionMessage(cond))
        }
        Sys.sleep(1)
        return(process_arrow_stream(stream, in_memory_batches, stream_writer, output_file, stream_rows_read, retry_count=retry_count+1))
      } else {
        stop(conditionMessage(cond))
      }
    })

  }

  results <- furrr::future_map(streams, function(stream){
    process_arrow_stream(stream)
  })

  if (is.null(folder)){
      return(do.call(arrow::concat_tables, sapply(results, function(x) arrow::read_ipc_stream(x, as_data_frame = FALSE))))
  }

}
