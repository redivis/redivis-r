

#' @importFrom httr VERB headers add_headers content status_code write_memory write_disk write_stream timeout
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_interp str_starts
make_request <- function(method='GET', query=NULL, payload = NULL, parse_response=TRUE, path = "", download_path = NULL, download_overwrite = FALSE, as_stream=FALSE, stream_callback = NULL){

  handler <- write_memory()

  if (!is.null(download_path)) handler <- write_disk(download_path, overwrite = download_overwrite)
  else if (!is.null(stream_callback)) handler <- write_stream(stream_callback) # TODO

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
    httr::timeout(10000)
  )

  if (!parse_response && status_code(res) < 400){
    return(res)
  }

  response_content = content(res, as="text", encoding='UTF-8')

  is_json <- FALSE

  if (!is.null(headers(res)$'content-type') && str_starts(headers(res)$'content-type', 'application/json') && (status_code(res) >= 400 || parse_response)){
    is_json <- TRUE
    response_content <- fromJSON(response_content, simplifyVector = FALSE)
  }

  if (status_code(res) >= 400){
    if (is_json){
      stop(response_content$error$message)
    } else {
      stop(response_content)
    }
  } else {
    response_content
  }
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

# TODO
# RedivisBatchReader <- setRefClass(
#   "RedivisBatchReader",
#   fields = list(streams = "list", mapped_variables="list", progress="boolean", coerce_schema="boolean" ),
#
#   methods = list(
#     get_next_reader_ = function(){
#
#
#     },
#     read_next_batch = function() {
#
#     }
#   )
# )

#' @importFrom tibble as_tibble
#' @importFrom data.table as.data.table
#' @importFrom arrow open_dataset
#' @importFrom uuid UUIDgenerate
#' @importFrom progressr progress with_progress
#' @importFrom parallelly availableCores supportsMulticore
make_rows_request <- function(uri, max_results, selected_variables = NULL, type = 'tibble', schema = NULL, progress = TRUE, coerce_schema=FALSE, batch_preprocessor=NULL){
  read_session <- make_request(
    method="post",
    path=str_interp("${uri}/readSessions"),
    parse_response=TRUE,
    payload=list(
        "maxResults"=max_results,
        "selectedVariables"=selected_variables,
        "format"="arrow",
        "requestedStreamCount"=parallelly::availableCores()
    )
  )

  # TODO
  # if (type == 'arrow_stream'){
  #   return(RedivisBatchReader$new(streams=read_session$streams, mapped_variable))
  # }

  folder <- str_interp('/${tempdir()}/redivis/tables/${uuid::UUIDgenerate()}')
  dir.create(folder, recursive = TRUE)

  if (progress){
    progressr::with_progress(parallel_stream_arrow(folder, read_session$streams, max_results, schema, coerce_schema, batch_preprocessor))
  } else {
    parallel_stream_arrow(folder, read_session$streams, max_results, schema, coerce_schema, batch_preprocessor)
  }

  arrow_dataset <- arrow::open_dataset(folder, format = "feather", schema = if (is.null(batch_preprocessor)) schema else NULL)

  # TODO: remove head() once BE is sorted
  if (type == 'arrow_dataset'){
    arrow_dataset
  } else {
    on.exit(unlink(folder))
    if (type == 'arrow_table'){
      head(arrow_dataset, max_results)
    }else if (type == 'tibble'){
     as_tibble(head(arrow_dataset, max_results))
    }else if (type == 'data_frame'){
      as.data.frame(head(arrow_dataset, max_results))
    } else if (type == 'data_table'){
      as.data.table(head(arrow_dataset, max_results))
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
#' @importFrom future plan multicore multisession
#' @importFrom progressr progressor
#' @import arrow
#' @import dplyr
parallel_stream_arrow <- function(folder, streams, max_results, schema, coerce_schema, batch_preprocessor){
  p <- progressr::progressor(steps = max_results)
  headers <- get_authorization_header()
  strategy <- if (parallelly::supportsMulticore()) future::multicore else future::multisession
  oplan <- future::plan(strategy, workers = length(streams))
  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(plan(oplan), add = TRUE)
  base_url = generate_api_url('/readStreams')

  furrr::future_map(streams, function(stream) {
    output_file_path <- str_interp('${folder}/${stream$id}')

    con <- url(str_interp('${base_url}/${stream$id}'), open = "rb", headers = headers)
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
          for (date_variable in date_variables){
            batch[[date_variable]] <- batch[[date_variable]]$cast(arrow::timestamp())
          }
          for (time_variable in time_variables){
            batch[[time_variable]] <- arrow::Array$create(paste0('2000-01-01T', batch[[time_variable]]$as_vector()))$cast(arrow::timestamp(unit='us'))
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
          p(amount = current_progress_rows)
          current_progress_rows <- 0
          last_measured_time = Sys.time()
        }
      }
    }
    p(amount = current_progress_rows)

    stream_writer$close()
    output_file$close()
  })
}
