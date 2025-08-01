
#' @include auth.R util.R
make_request <- function(
    method='GET',
    query=NULL,
    payload = NULL,
    files = NULL,
    parse_response=TRUE,
    path = "",
    download_path = NULL,
    download_overwrite = FALSE,
    as_stream=FALSE,
    headers=list(),
    headers_callback=NULL,
    get_download_path_callback=NULL,
    stream_callback = NULL,
    stop_on_error=TRUE,
    start_byte=0,
    end_byte=NULL,
    resumable_byte=0,
    retry_count=0
){
  args <- list(
    method=method,
    query=query,
    payload = payload,
    files = files,
    parse_response=parse_response,
    path = path,
    download_path = download_path,
    download_overwrite = download_overwrite,
    as_stream=as_stream,
    headers=headers,
    headers_callback=headers_callback,
    get_download_path_callback=get_download_path_callback,
    stream_callback = stream_callback,
    stop_on_error=stop_on_error,
    start_byte=start_byte,
    end_byte=end_byte,
    resumable_byte=resumable_byte,
    retry_count=retry_count
  )

  if (start_byte || resumable_byte){
    if (!is.null(end_byte)){
      headers$Range = str_interp("bytes=${start_byte+resumable_byte}-${end_byte}")
    } else {
      headers$Range = str_interp("bytes=${start_byte+resumable_byte}-")
    }
  }

  if (!is.null(stream_callback) || !is.null(get_download_path_callback)){
    h <- curl::new_handle()
    if (Sys.getenv("REDIVIS_API_ENDPOINT") == "https://localhost:8443/api/v1"){
      curl::handle_setopt(h, "ssl_verifypeer"=0L)
    }
    curl::handle_setheaders(h, .list=append(get_authorization_header(as_list=TRUE), headers))
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
    on.exit(close(con), add=TRUE)

    tryCatch({
      open(con, "rb", blocking = FALSE)
      res_data <- curl::handle_data(h)
    },
    warning=function(w){},
    error=function(e){
      res_data <- curl::handle_data(h)
      if ((res_data$status_code == 503 || res_data$status_code == 0) && retry_count < 10){
        Sys.sleep(retry_count)
        args$retry_count <- args$retry_count + 1
        return(do.call(make_request, args))
      }
      if (res_data$status_code >= 400){
        if (res_data$status_code == 401 && is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA)) && is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset=NA))){
          refresh_credentials()
          return(do.call(make_request, args))
        }
        if (stop_on_error){
          stop(str_interp("Received HTTP status ${res_data$status_code} for path ${url}"))
        } else {
          return(NULL)
        }
      }
    })


    res_data <- curl::handle_data(h)

    response_headers <- parse_curl_headers(res_data)
    if (!is.null(headers_callback)){
      headers_callback(response_headers)
    }

    args$retry_count <- 0
    if (is.null(get_download_path_callback)){
      bytes_read <- 0
      tryCatch({
        while(isIncomplete(con)){
          buf <- readBin(con, raw(), 16384)
          cb_res <- stream_callback(buf)
          bytes_read <- bytes_read + length(buf)
          if (!is.null(cb_res) && stream_callback(cb_res) == FALSE){
            break;
          }
        }
      },
      warning=function(w){},
      error=function(e){
        if ("accept-ranges" %in% names(response_headers) && retry_count < 10){
          Sys.sleep(retry_count)
          args$retry_count <- args$retry_count + 1
          args$resumable_byte=bytes_read
          return(do.call(make_request, args))
        }
      })
    } else {

      download_path <- get_download_path_callback(response_headers)
      file_con <- base::file(download_path, if (resumable_byte) "ab" else "w+b")
      tryCatch({
        on.exit(close(file_con), add=TRUE)
        while(isIncomplete(con)){
          writeBin(readBin(con, raw(), 16384), file_con)
        }
        return(download_path)
      },
      warning=function(w){},
      error=function(e){
        if ("accept-ranges" %in% names(response_headers) && retry_count < 10){
          Sys.sleep(retry_count)
          args$resumable_byte=file.size(download_path)
          return(do.call(make_request, args))
        }
        stop(e)
      })
    }

  } else {
    handler <- httr::write_memory()
    if (!is.null(download_path)) handler <- httr::write_disk(download_path, overwrite = download_overwrite)
    else if (!is.null(stream_callback)) handler <- httr::write_stream(stream_callback)

    request_headers <- append(get_authorization_header(), unlist(headers))
    encode <- NULL
    body <- NULL

    if (!is.null(files)){
      encode <- "multipart"
      body <- files
    }
    else if (!is.null(payload)){
      # Don't use encode for JSON, so we can have more control
      request_headers <- append(request_headers, c("Content-Type"="application/json"))
      body <- jsonlite::toJSON(payload, na="null", null="null", auto_unbox=TRUE)
    }

    res <- httr::VERB(
      method,
      handler,
      url = generate_api_url(path),
      httr::add_headers(request_headers),
      query = query,
      body = body,
      encode=encode,
      httr::timeout(3600)
    )

    if (httr::status_code(res) == 503 && retry_count < 10){
      Sys.sleep(retry_count)
      args$retry_count <- args$retry_count + 1
      return(do.call(make_request, args))
    }

    if (!is.null(httr::headers(res)$'x-redivis-warning') && is.null(globals$printed_warnings[[httr::headers(res)$'x-redivis-warning']])){
      globals$printed_warnings[httr::headers(res)$'x-redivis-warning'] <- TRUE
      warning(httr::headers(res)$'x-redivis-warning')
    }

    if ((!parse_response || method == "HEAD") && httr::status_code(res) < 400){
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

    if (
      (
        httr::status_code(res) == 401
        || (httr::status_code(res) == 403 && response_content$error == 'insufficient_scope')
      )
      && is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA))
      && is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset=NA))
    ){
      message(
        str_interp("\n${response_content$error}: ${response_content$error_description}\n")
      )
      flush.console()
      refresh_credentials(
        scope=if(is.null(response_content$scope)) NULL else strsplit(response_content$scope, " "),
        amr_values=response_content$amr_values
      )
      return(do.call(make_request, args))
    }

    if (httr::status_code(res) >= 400 && stop_on_error){
      if (is_json){
        stop(str_interp("${response_content$error}_error: ${response_content$error_description}"))
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

perform_parallel_download <- function(
    paths,
    overwrite,
    get_download_path_from_headers,
    on_finish,
    max_parallelization,
    stop_on_error = TRUE
) {
  # limit open files
  max_parallelization <- max(min(128, length(paths), max_parallelization), 1)
  pool <- curl::new_pool(
    total_con   = ceiling(max(1, max_parallelization / 2)), # multiplexing allows us to have fewer connections, so divide by 2
  )

  # TODO: upgrade to curl >= 6.1.0 and we can do:
  # streams_per_connection <- 5
  # pool <- curl::new_pool(
  #   total_con   = ceiling(max_parallelization / streams_per_connection),
  #   max_streams = streams_per_connection
  # )

  # track open connections so we can clean up on exit
  file_connections <- vector("list", length(paths))
  file_paths <- vector("list", length(paths))

  on.exit({
    for (con in file_connections) {
      if (!is.null(con)) {
        tryCatch(close(con), error = function(e) {})
      }
    }
  }, add = TRUE)

  # a queue of downloads still to schedule (for retries)
  pending <- list()

  # keep track of simultaneously‐running downloads
  active_downloads <- 0L

  # helper to schedule (or re‐schedule) a download
  schedule_download <- function(index, retry_count = 0L, start_byte = 0L) {
    max_retries <- 10L
    path        <- paths[[index]]
    url         <- generate_api_url(path)

    if (retry_count > 0){
      Sys.sleep(retry_count)
    }

    # build a fresh handle
    h <- curl::new_handle()
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
    file_con     <- NULL
    download_path <- NULL
    content_length <- NULL
    bytes_written <- 0
    did_error <- FALSE

    # callback: write chunks
    write_cb <- function(chunk, final) {
      if (did_error){
        return()
      }
      # open file on first chunk
      if (is.null(file_con)) {
        # Note: at this point the response headers have been processed,
        # so we can query the status code via curl::handle_data()
        res <- curl::handle_data(h)
        status <- res$status_code

        if (status == 0) {
          # curl‐level error; let fail_cb handle it
          return()
        }
        if (status == 503L && retry_count < max_retries) {
          did_error <<- TRUE
          active_downloads <<- active_downloads - 1L
          # queue a retry
          pending[[length(pending) + 1]] <<- list(
            index       = index,
            retry_count = retry_count + 1L,
            start_byte  = start_byte + bytes_written
          )
          return()
        }
        if (status >= 400L) {
          active_downloads <<- active_downloads - 1L
          if (stop_on_error) {
            stop(sprintf("HTTP %d for path %s", status, path))
          } else {
            return()
          }
        }

        headers <- parse_curl_headers(res)
        content_length <<- as.integer(headers[["content-length"]])
        download_path <<- get_download_path_from_headers(headers)
        if (!overwrite && file.exists(download_path)) {
          stop(sprintf(
            "File already exists at '%s'. Set overwrite = TRUE to overwrite.",
            download_path
          ))
        }
        file_paths[[index]] <<- download_path
        retry_count <<- 0
        file_con <<- base::file(download_path, if (start_byte == 0) "wb" else "ab")
        file_connections[[index]] <<- file_con
      }

      # write the chunk
      if (length(chunk)) {
        bytes_written <<- bytes_written + length(chunk)
        writeBin(chunk, file_con)
      }

      # on final chunk, clean up if we're done (note that if we haven't read all bytes in content-length, fail_cb will be called)
      if (final && bytes_written == content_length) {
        close(file_con)
        file_connections[[index]] <<- NULL
        on_finish()
        active_downloads <<- active_downloads - 1L
      }
    }

    # callback: on error
    fail_cb <- function(err) {
      active_downloads <<- active_downloads - 1L
      if (!is.null(file_con)) {
        tryCatch(close(file_con), error = function(e) {})
        file_connections[[index]] <<- NULL
      }
      if (retry_count < max_retries) {
        pending[[length(pending) + 1]] <<- list(
          index       = index,
          retry_count = retry_count + 1L,
          start_byte  = bytes_written + start_byte
        )
      } else {
        stop(err)
      }
    }

    # finally, add to the pool
    curl::multi_add(
      handle = h,
      pool   = pool,
      data   = write_cb,
      fail   = fail_cb
    )
    active_downloads <<- active_downloads + 1L
  }

  current_index <- 1

  # schedule all initial downloads
  while (current_index <= length(paths)) {
    schedule_download(current_index)
    current_index <- current_index + 1
    if (active_downloads >= max_parallelization) break
  }

  # drive the pool until everything (including retries) is done
  while (active_downloads > 0L) {
    curl::multi_run(pool = pool, poll = 1)

    # schedule any pending retries
    if (length(pending) > 0L) {
      for (task in pending) {
        schedule_download(
          task$index,
          retry_count = task$retry_count,
          start_byte  = task$start_byte
        )
        # Make sure we wait for all retries to enqueue. Add 0.1s as additional buffer
        Sys.sleep(max(vapply(pending, function(task) task$retry_count, 1)) + 0.1)
      }
      pending <- list()
    }
    while (current_index <= length(paths)) {
      schedule_download(current_index)
      current_index <- current_index + 1
      if (active_downloads >= max_parallelization) break
    }
  }

  return(file_paths)
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
    current_offset="numeric",
    coerce_schema="logical",
    retry_count="numeric",
    # Can't figure out how to pass non-built-in classes here, so just pass as list
    custom_classes="list",
    current_stream_index="numeric",
    time_variables="list"
  ),

  methods = list(
    get_next_reader__ = function(offset=0){
      .self$current_offset=offset
      base_url = generate_api_url('/readStreams')
      options(timeout=3600)
      headers <- get_authorization_header()
      tryCatch({
        con <- url(str_interp('${base_url}/${streams[[.self$current_stream_index]]$id}?offset=${.self$current_offset}'), open = "rb", headers = headers, blocking=FALSE)
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
      warning=function(w){},
      error=function(e){
        .self$retry_count = .self$retry_count + 1
        if (.self$retry_count > 10){
          message("Download connection failed after too many retries, giving up.")
          stop(conditionMessage(e))
        }
        Sys.sleep(.self$retry_count)

        return(.self$get_next_reader__(.self$current_offset))
      })
    },
    read_next_batch = function() {
      tryCatch({
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
          .self$current_offset <- .self$current_offset + batch$num_rows
          .self$retry_count <- 0
          return (batch)
        }
      },
      error=function(e){
          .self$retry_count = .self$retry_count + 1
          if (.self$retry_count > 10){
            message("Download connection failed after too many retries, giving up.")
            stop(conditionMessage(e))
          }
          Sys.sleep(.self$retry_count)
          .self$get_next_reader__(.self$current_offset)
          return(.self$read_next_batch())
        })

    },
    close = function(){
      base::close(.self$custom_classes$current_connection)
    }
  )
)


#' @include util.R
make_rows_request <- function(uri, max_results=NULL, selected_variable_names = NULL, type = 'tibble', variables = NULL, progress = TRUE, coerce_schema=FALSE, batch_preprocessor=NULL, table=NULL, use_export_api=FALSE, max_parallelization=parallelly::availableCores()){
  payload = list("requestedStreamCount"=if (type == 'arrow_stream') 1 else min(8, max_parallelization), format="arrow")

  if (!is.null(max_results)){
    payload$maxResults = max_results
  }
  if (!is.null(selected_variable_names)){
    payload$selectedVariables = selected_variable_names
  }

  arrow::set_cpu_count(max_parallelization)
  arrow::set_io_thread_count(max(max_parallelization,2))

  use_export_api <- use_export_api && !is.null(table) && type != 'arrow_stream' && is.null(selected_variable_names) && is.null(batch_preprocessor) && is.null(max_results)

  if (!use_export_api){
    read_session <- make_request(
      method="post",
      path=str_interp("${uri}/readSessions"),
      parse_response=TRUE,
      payload=payload
    )
  }

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
    folder <- str_interp('${get_temp_dir()}/tables/${uuid::UUIDgenerate()}')
    dir.create(folder, recursive = TRUE)

    if (type != 'arrow_dataset'){
      on.exit(unlink(folder, recursive=TRUE), add=TRUE)
    }
  }

  if (use_export_api){
    table$download(folder, format='parquet', progress=progress)
  } else if (progress){
    result <- progressr::with_progress(parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, variables, coerce_schema, batch_preprocessor, max_parallelization))
  } else {
    result <- parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, variables, coerce_schema, batch_preprocessor, max_parallelization)
  }

  ds <- arrow::open_dataset(folder, format = if (use_export_api) "parquet" else "feather", schema = if (is.null(batch_preprocessor) && !use_export_api) get_arrow_schema(variables) else NULL)

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

version_info <- R.Version()
redivis_version <- packageVersion("redivis")
get_authorization_header <- function(as_list=FALSE){
  auth_token <- get_auth_token()
  if (as_list){
    list("Authorization"=str_interp("Bearer ${auth_token}"), "User-Agent"=str_interp("redivis-r/${redivis_version} (${version_info$platform}; R/${version_info$major}.${version_info$minor})"))
  } else {
    c("Authorization"=str_interp("Bearer ${auth_token}"), "User-Agent"=str_interp("redivis-r/${redivis_version} (${version_info$platform}; R/${version_info$major}.${version_info$minor})"))
  }
}


parallel_stream_arrow <- function(folder, streams, max_results, variables, coerce_schema, batch_preprocessor, max_parallelization){
  if (!length(streams)){
    return();
  }

  pb_multiplier <- 100/max_results
  pb <- progressr::progressor(steps = 100)

  headers <- get_authorization_header()
  worker_count <- min(8, length(streams), parallelly::availableCores(), max_parallelization)

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

    tryCatch({
      # This ensures the url method doesn't time out after 60s. Only applies to this function, doesn't set globally
      options(timeout=3600)
      con <- url(str_interp('${base_url}/${stream$id}?offset=${stream_rows_read}'), open = "rb", headers = headers, blocking=FALSE)
      on.exit(close(con), add=TRUE)
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace("arrow")$MakeRConnectionInputStream(con))

      retry_count <- 0

      if (!is.null(folder) && is.null(output_file)){
        output_file <- arrow::FileOutputStream$create(str_interp('${folder}/${stream$id}.feather'))
      }

      fields_to_add <- list()
      should_reorder_fields <- FALSE
      time_variables_to_coerce = c()

      i <- 0
      # Make sure to first only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
      for (field in stream_reader$schema$fields){
        i <- i+1
        if (coerce_schema && is(schema[[field$name]]$type, "Time64")){
          time_variables_to_coerce <- append(time_variables_to_coerce, field$name)
        }
        if (!should_reorder_fields && i != match(field$name, names(schema))){
          should_reorder_fields <- TRUE
        }
      }

      for (field_name in schema$names){
        if (is.null(stream_reader$schema$GetFieldByName(field_name))){
          fields_to_add <- append(fields_to_add, schema$GetFieldByName(field_name))
        }
      }

      if (!should_reorder_fields && length(fields_to_add)){
        should_reorder_fields <- TRUE
      }

      last_measured_time <- Sys.time()
      current_progress_rows <- 0

      while (TRUE){
        batch <- stream_reader$read_next_batch()
        if (is.null(batch)){
          break
        } else {
          current_progress_rows <- current_progress_rows + batch$num_rows
          stream_rows_read <- stream_rows_read + batch$num_rows

          # We need to coerce_schema for all dataset tables, since their underlying storage type is always a string
          if (coerce_schema){
            # Note: this approach is much more performant than using %>% mutate(across())
            # TODO: in the future, Arrow may support native coversion from time string to their type
            # To test if supported: arrow::arrow_array(rep('10:30:04.123', 2))$cast(arrow::time64(unit="us"))
            for (time_variable in time_variables_to_coerce){
              batch[[time_variable]] <- arrow::arrow_array(stringr::str_c('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
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
            pb(amount = current_progress_rows * pb_multiplier)
            current_progress_rows <- 0
            last_measured_time = Sys.time()
          }
        }
      }

      pb(amount = current_progress_rows * pb_multiplier)

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
    },
    warning=function(w){},
    error=function(e){
      print(e)
      if (grepl("cannot read from connection", conditionMessage(e))){
        if (retry_count > 10){
          message("Download connection failed after too many retries, giving up.")
          stop(conditionMessage(e))
        }
        Sys.sleep(retry_count)
        return(process_arrow_stream(stream, in_memory_batches, stream_writer, output_file, stream_rows_read, retry_count=retry_count+1))
      } else {
        stop(conditionMessage(e))
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
