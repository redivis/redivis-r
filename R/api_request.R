

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

#' @importFrom tibble as_tibble
#' @importFrom data.table as.data.table
#' @importFrom arrow open_dataset
#' @importFrom uuid UUIDgenerate
#' @importFrom progressr progress with_progress
#' @importFrom parallelly availableCores supportsMulticore
make_rows_request <- function(uri, max_results, selected_variables = NULL, type = 'tibble', schema = NULL, progress = TRUE, coerce_schema=FALSE){
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

  folder <- str_interp('/${tempdir()}/redivis/tables/${uuid::UUIDgenerate()}')
  dir.create(folder, recursive = TRUE)

  if (progress){
    progressr::with_progress(parallel_stream_arrow(folder, read_session$streams, max_results, schema, coerce_schema))
  } else {
    parallel_stream_arrow(folder, read_session$streams, max_results, schema, coerce_schema)
  }

  arrow_dataset <- arrow::open_dataset(folder, format = "feather", schema = schema)

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
parallel_stream_arrow <- function(folder, streams, max_results, schema, coerce_schema){
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

    for (field_name in stream_reader$schema$names){
      writer_schema_fields <- append(writer_schema_fields, schema$GetFieldByName(field_name))
    }

    if (coerce_schema){
      schema_retype_map = list()

      for (field in writer_schema_fields){
        if (!is.null(schema_retype_map[class(field$type)[1]][[1]])){
          schema_retype_map[class(field$type)[1]][[1]]$variable_names = append(schema_retype_map[class(field$type)[1]][[1]]$variable_names, c(field$name))
        }
        else {
          if (is(field$type, "Date32")){
            schema_retype_map <- append(schema_retype_map, list(Date32=list(
              type="date",
              variable_names = c(field$name)
            )))
          } else if (is(field$type, "Timestamp")){
            schema_retype_map <- append(schema_retype_map, list(Timestamp=list(
              type="dateTime",
              variable_names = c(field$name)
            )))
          } else if (is(field$type, "Time64")){
            schema_retype_map <- append(schema_retype_map, list(Time64=list(
              type="time",
              variable_names = c(field$name)
            )))
          } else if (is(field$type, "Int64")){
            schema_retype_map <- append(schema_retype_map, list(Int64=list(
              type="integer",
              variable_names = c(field$name)
            )))
          } else if (is(field$type, "Float64")){
            schema_retype_map <- append(schema_retype_map, list(Float64=list(
              type="float",
              variable_names = c(field$name)
            )))
          }
          else if (is(field$type, "Boolean")){
            schema_retype_map <- append(schema_retype_map, list(Boolean=list(
              type="boolean",
              variable_names = c(field$name)
            )))
          }
        }
      }
    }

    stream_writer <- arrow::RecordBatchFileWriter$create(output_file, arrow::schema(writer_schema_fields))

    current_progress_rows <- 0
    last_measured_time <- Sys.time()

    while (TRUE){
      batch = stream_reader$read_next_batch()
      if (is.null(batch)){
        break
      } else {
        current_progress_rows = current_progress_rows + batch$num_rows
        if (Sys.time() - last_measured_time > 0.2){
          p(amount = current_progress_rows)
          current_progress_rows <- 0
          last_measured_time = Sys.time()
        }
        # We need to coerce_schema for all dataset tables, since their underlying storage type is always a string
        if (coerce_schema){
          cast = getNamespace("arrow")$cast
          for (retype_spec in schema_retype_map){
            names <- retype_spec$variable_names

            if (retype_spec$type == 'date'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(cast(cast(.x, timestamp()),date32()))
                  )
                )
            } else if (retype_spec$type == 'dateTime'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(cast(.x, timestamp(unit="us")))
                  )
                )
            } else if (retype_spec$type == 'time'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(
                      getNamespace("arrow")$cast(
                        getNamespace("arrow")$cast(
                          if_else(is.na(.x), NA, paste0('2000-01-01T', .x)),
                          timestamp(unit="us")
                        ),
                        time64(unit="us")
                      )
                    )
                  )
                )
            } else if (retype_spec$type == 'integer'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(cast(.x, int64()))
                  )
                )
            } else if (retype_spec$type == 'float'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(cast(.x, float64()))
                  )
                )
            }
            else if (retype_spec$type == 'boolean'){
              batch <- batch %>%
                mutate(
                  across(
                    all_of(names),
                    ~(cast(.x, bool()))
                  )
                )
            }
          }

          stream_writer$write_batch(as_record_batch(batch))
        } else {
          stream_writer$write_batch(batch)
        }
      }
    }
    p(amount = current_progress_rows)

    stream_writer$close()
    output_file$close()
  })
}
