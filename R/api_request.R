

#' @importFrom httr VERB headers add_headers content status_code write_memory write_disk write_stream timeout
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_interp str_starts
#' @import curl
make_request <- function(method='GET', query=NULL, payload = NULL, parse_response=TRUE, path = "", download_path = NULL, download_overwrite = FALSE, as_stream=FALSE, get_download_path_callback=NULL, stream_callback = NULL, stop_on_error=TRUE){
  if (!is.null(stream_callback) || !is.null(get_download_path_callback)){
    h <- curl::new_handle()
    auth = get_authorization_header()
    curl::handle_setheaders(h, "Authorization"=auth[[1]])
    url <- generate_api_url(path)
    con <- curl::curl(url, handle=h)
    open(con, "rb", blocking = FALSE)
    on.exit(close(con))
    res_data <- curl::handle_data(h)
    if (res_data$status_code >= 400){
      if (stop_on_error){
        stop(str_interp("Received HTTP status ${status_code} for path ${path}"))
      } else {
        return(NULL)
      }
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
      headers <- parse_curl_headers(res_data)

      download_path <- get_download_path_callback(headers)
      file_con <- base::file(download_path, "w+b")
      on.exit(close(file_con), add=TRUE)
      while(isIncomplete(con)){
        writeBin(readBin(con, raw(), 16384), file_con)
      }
      return(download_path)
    }

  } else {
    handler <- write_memory()

    if (!is.null(download_path)) handler <- write_disk(download_path, overwrite = download_overwrite)
    else if (!is.null(stream_callback)) handler <- write_stream(stream_callback)
    # req <- httr2::request(str_interp("${base_url}${utils::URLencode(path)}")) %>%
    #   httr2::req_auth_bearer_token(get_auth_token()) %>%
    #   httr2::req_method(method)
    #
    # if (!is.null(payload)){
    #   req %>% httr2::req_body_json(payload)
    # }
    #
    # if (!is.null(download_path)){
    #   if (download_overwrite == FALSE && file.exists(download_path)){
    #     stop(str_interp("File already exists at path '${download_path}'. To overwrite existing files, set parameter overwrite=TRUE."))
    #   }
    #   conn = base::file(download_path, 'w+b')
    #   httr2::req_stream(req, function(buff) {
    #     writeBin(buff, conn, append=TRUE)
    #     print('chunk')
    #     TRUE
    #   })
    #   close(conn)
    # } else if (as_stream){
    #   return(httr2::req_stream(req))
    # } else {
    #   resp <- httr2::req_perform(req)
    #   if (parse_response){
    #     httr2::resp_body_json(resp)
    #   } else {
    #     httr2::resp_body_raw(resp)
    #   }
    # }
    res <- VERB(
      method,
      handler,
      url = generate_api_url(path),
      add_headers(get_authorization_header()),
      query = query,
      body = payload,
      encode="json",
      httr::timeout(3600)
    )

    if (!parse_response && status_code(res) < 400){
      return(res)
    }

    is_json <- FALSE
    response_content <- NULL

    if (method == "HEAD"){
      if (status_code(res) >= 400){
        response_content <- fromJSON(URLdecode(headers(res)$'x-redivis-error-payload'))
      }
    } else {
      response_content <- content(res, as="text", encoding='UTF-8')

      if (!is.null(headers(res)$'content-type') && str_starts(headers(res)$'content-type', 'application/json') && (status_code(res) >= 400 || parse_response)){
        is_json <- TRUE
        response_content <- fromJSON(response_content, simplifyVector = FALSE)
      }
    }


    if (status_code(res) >= 400 && stop_on_error){
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
    strsplit(header, ':')[[1]][[1]]
  })
  headers <- purrr::map(vec, function(header) {
    split = strsplit(header, ':\\s+')[[1]]
    paste0(tail(split, -1), collapse=':')
  }) %>% purrr::set_names(header_names)
}

#' @import curl
perform_parallel_download <- function(paths, overwrite, get_download_path_from_headers, on_finish, stop_on_error=TRUE){
  pool <- curl::new_pool(total_con = 100, host_con = 20, multiplex = TRUE)
  handles = list()
  for (path in paths){
    h <- curl::new_handle()
    url <- generate_api_url(path)
    auth = get_authorization_header()
    curl::handle_setheaders(h, "Authorization"=auth[[1]])
    curl::handle_setopt(h, "url"=url)

    fail_fn <- function(e){
      print(e)
      stop(e)
    }
    curl::multi_add(h, fail = fail_fn, data = make_data_fn(h, url, get_download_path_from_headers, overwrite, on_finish), pool = pool)
  }
  curl::multi_run(pool=pool)
}

make_data_fn <- function(h, url, get_download_path_from_headers, overwrite, on_finish){
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
      print(res_data)
      print(handle)

      headers <- parse_curl_headers(res_data)
      print(headers)
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
  page = 0
  results = list()
  next_page_token = NULL

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

    page = page + 1
    results = append(results, response$results)
    next_page_token = response$nextPageToken
    if (is.null(next_page_token)){
      break
    }

  }

  results
}

#' @importFrom progressr progressor progress
#' @import arrow
RedivisBatchReader <- setRefClass(
  "RedivisBatchReader",
  fields = list(
    streams = "list",
    progress="logical",
    coerce_schema="logical",
    current_progress_rows="numeric",
    # Can't figure out how to pass non-built-in classes here, so just pass as list
    custom_classes="list",
    current_stream_index="numeric",
    date_variables="list",
    time_variables="list",
    last_progressed_time="POSIXct"
  ),

  methods = list(
    get_next_reader__ = function(){
      options(timeout=3600)
      headers <- get_authorization_header()
      base_url = generate_api_url('/readStreams')
      con <- url(str_interp('${base_url}/${streams[[.self$current_stream_index]]$id}'), open = "rb", headers = headers, blocking=FALSE)
      stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace("arrow")$MakeRConnectionInputStream(con))
      writer_schema_fields <- list()

      # Make sure to only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
      for (field_name in stream_reader$schema$names){
        writer_schema_fields <- append(writer_schema_fields, .self$custom_classes$schema$GetFieldByName(field_name))
      }

      if (coerce_schema){
        cast = getNamespace("arrow")$cast
        .self$date_variables = list()
        .self$time_variables = list()

        for (field in writer_schema_fields){
          if (is(field$type, "Date32")){
            .self$date_variables <- append(.self$date_variables, field$name)
          } else if (is(field$type, "Time64")){
            .self$time_variables <- append(.self$time_variables, field$name)
          }
        }
      }
      .self$custom_classes = list(
        current_record_batch_reader=stream_reader,
        writer_schema=arrow::schema(writer_schema_fields),
        schema=.self$custom_classes$schema,
        progressor=.self$custom_classes$progressor,
        current_connection=con
      )
    },
    read_next_batch = function() {
      batch <- .self$custom_classes$current_record_batch_reader$read_next_batch()
      if (is.null(batch)){
        if (.self$current_stream_index == length(.self$streams)){
          # if (.self$current_progress_rows > 0 && .self.progress){
          #   .self$custom_classes$progressor(amount = .self$current_progress_rows)
          # }
          return(NULL)
        } else {
          .self$current_stream_index = .self$current_stream_index + 1
          .self$get_next_reader__()
          return(.self$read_next_batch())
        }
      } else {
        if (.self$coerce_schema){
          # Note: this approach is much more performant than using %>% mutate(across())
          # TODO: in the future, Arrow may support native conversion from date / time string to their type
          for (date_variable in .self$date_variables){
            batch[[date_variable]] <- batch[[date_variable]]$cast(arrow::timestamp())
          }
          for (time_variable in time_variables){
            batch[[time_variable]] <- arrow::arrow_array(stringr::str_c('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
          }

          batch <- (as_record_batch(batch, schema=.self$custom_classes$writer_schema))
        }
        # if (.self$progress){
        #   .self$current_progress_rows = .self$current_progress_rows + batch$num_rows
        #   if (Sys.time() - .self$last_progressed_time > 0.2){
        #     .self$custom_classes$progressor(amount = .self$current_progress_rows)
        #     .self$current_progress_rows <- 0
        #     .self$last_progressed_time = Sys.time()
        #   }
        # }
        return (batch)
      }
    },
    close = function(){
      base::close(.self$custom_classes$current_connection)
      # progressr::handlers(global=FALSE)
    }
  )
)

#' @importFrom tibble as_tibble
#' @importFrom data.table as.data.table
#' @importFrom arrow open_dataset Scanner
#' @importFrom uuid UUIDgenerate
#' @importFrom progressr progress with_progress
make_rows_request <- function(uri, max_results=NULL, selected_variables = NULL, type = 'tibble', schema = NULL, progress = TRUE, coerce_schema=FALSE, batch_preprocessor=NULL){
  read_session <- make_request(
    method="post",
    path=str_interp("${uri}/readSessions"),
    parse_response=TRUE,
    payload=list(
        "maxResults"=max_results,
        "selectedVariables"=selected_variables,
        "format"="arrow",
        "requestedStreamCount"=if (type == 'arrow_stream') 1 else parallelly::availableCores()
    )
  )

  if (type == 'arrow_stream'){
    p <- NULL
    # if (progress){
    #   progressr::handlers(global = TRUE)
    #   p <- progressr::progressor(steps = read_session$numRows)
    # }
    reader = RedivisBatchReader$new(
      streams=read_session$streams,
      progress=FALSE,
      coerce_schema=coerce_schema,
      current_stream_index=1,
      date_variables=list(),
      time_variables=list(),
      custom_classes=list(schema=schema, progressor=p),
      last_progressed_time=Sys.time(),
      current_progress_rows=0
    )
    reader$get_next_reader__()

    return(reader)
  }

  folder <- str_interp('/${tempdir()}/redivis/tables/${uuid::UUIDgenerate()}')
  dir.create(folder, recursive = TRUE)
  if (type != 'arrow_dataset'){
    on.exit(unlink(folder))
  }

  if (progress){
    progressr::with_progress(parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, schema, coerce_schema, batch_preprocessor))
  } else {
    parallel_stream_arrow(folder, read_session$streams, max_results=read_session$numRows, schema, coerce_schema, batch_preprocessor)
  }

  arrow_dataset <- arrow::open_dataset(folder, format = "feather", schema = if (is.null(batch_preprocessor)) schema else NULL)

  # TODO: remove head() once BE is sorted
  if (type == 'arrow_dataset'){
    arrow_dataset
  } else {
    arrow_table = if (is.null(max_results)) arrow::Scanner$create(arrow_dataset)$ToTable() else head(arrow_dataset, max_results)
    if (type == 'arrow_table'){
      arrow_table
    }else if (type == 'tibble'){
      as_tibble(arrow_table)
    }else if (type == 'data_frame'){
      as.data.frame(arrow_table)
    } else if (type == 'data_table'){
      as.data.table(arrow_table)
    }
  }

}

generate_api_url <- function(path){
  str_interp('${if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1" else Sys.getenv("REDIVIS_API_ENDPOINT")}${utils::URLencode(path)}')
}

get_authorization_header <- function(){
  if (Sys.getenv("REDIVIS_API_TOKEN") == ""){
    stop("The environment variable REDIVIS_API_TOKEN must be set.")
  }

  c("Authorization"=str_interp("Bearer ${Sys.getenv('REDIVIS_API_TOKEN')}"))
}

#' @importFrom furrr future_map
#' @importFrom future plan multicore multisession sequential
#' @importFrom parallelly availableCores supportsMulticore
#' @importFrom progressr progressor
#' @importFrom stringr str_c
#' @import arrow
#' @import dplyr
parallel_stream_arrow <- function(folder, streams, max_results, schema, coerce_schema, batch_preprocessor){
  pb <- progressr::progressor(steps = max_results)
  # pb <- txtProgressBar(0, max_results, style = 3)
  headers <- get_authorization_header()
  worker_count <- length(streams)


  if (parallelly::supportsMulticore()){
    oplan <- future::plan(future::multicore, workers = worker_count)
  } else {
    # oplan <- future::plan(future::sequential)
    oplan <- future::plan(future::multisession, workers = worker_count)
  }

  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(plan(oplan), add = TRUE)
  base_url = generate_api_url('/readStreams')

  furrr::future_map(streams, function(stream){
    # for (stream in streams){
    output_file_path <- str_interp('${folder}/${stream$id}')

    # This ensures the url method doesn't time out after 60s. Only applies to this function, doesn't set globally
    options(timeout=3600)
    con <- url(str_interp('${base_url}/${stream$id}'), open = "rb", headers = headers, blocking=FALSE)
    on.exit(close(con))
    stream_reader <- arrow::RecordBatchStreamReader$create(getNamespace("arrow")$MakeRConnectionInputStream(con))

    output_file <- arrow::FileOutputStream$create(output_file_path)

    writer_schema_fields <- list()

    # Make sure to only get the fields in the reader, to handle when reading from an unreleased table made up of uploads with inconsistent variables
    for (field_name in stream_reader$schema$names){
      writer_schema_fields <- append(writer_schema_fields, schema$GetFieldByName(field_name))
    }

    if (coerce_schema){
      cast = getNamespace("arrow")$cast
      date_variables = c()
      time_variables = c()

      for (field in writer_schema_fields){
        if (is(field$type, "Date32")){
          date_variables <- append(date_variables, field$name)
        } else if (is(field$type, "Time64")){
          time_variables <- append(time_variables, field$name)
        }
      }
    }

    stream_writer <- NULL
    writer_schema <- arrow::schema(writer_schema_fields)

    current_progress_rows <- 0
    last_measured_time <- Sys.time()

    while (TRUE){
      batch <- stream_reader$read_next_batch()
      if (is.null(batch)){
        break
      } else {
        current_progress_rows = current_progress_rows + batch$num_rows

        # We need to coerce_schema for all dataset tables, since their underlying storage type is always a string
        if (coerce_schema){
          # Note: this approach is much more performant than using %>% mutate(across())
          # TODO: in the future, Arrow may support native coversion from date / time string to their type
          for (date_variable in date_variables){
            batch[[date_variable]] <- batch[[date_variable]]$cast(arrow::timestamp())
          }
          for (time_variable in time_variables){
            batch[[time_variable]] <- arrow::arrow_array(stringr::str_c('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
          }

          batch <- (as_record_batch(batch, schema=writer_schema))
        }

        if (!is.null(batch_preprocessor)){
          batch <- batch_preprocessor(batch)
        }

        if (!is.null(batch)){
          if (is.null(stream_writer)){
            stream_writer <- arrow::RecordBatchFileWriter$create(output_file, schema=if (is.null(batch_preprocessor)) writer_schema else batch$schema)
          }
          stream_writer$write_batch(batch)
        }

        if (Sys.time() - last_measured_time > 0.2){
          # setTxtProgressBar(pb, current_progress_rows)
          pb(amount = current_progress_rows)
          current_progress_rows <- 0
          last_measured_time = Sys.time()
        }
      }
    }

    pb(amount = current_progress_rows)
    # setTxtProgressBar(pb, current_progress_rows)

    stream_writer$close()
    output_file$close()
    # }
  })
}
