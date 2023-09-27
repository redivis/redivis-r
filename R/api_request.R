

#' @importFrom httr VERB headers add_headers content status_code write_memory write_disk write_stream
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_interp str_starts
make_request <- function(method='GET', query=NULL, payload = NULL, parse_response=TRUE, path = "", download_path = NULL, download_overwrite = FALSE, stream_callback = NULL){
  base_url <- if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1" else Sys.getenv("REDIVIS_API_ENDPOINT")

  handler <- write_memory()

  if (!is.null(download_path)) handler <- write_disk(download_path, overwrite = download_overwrite)
  else if (!is.null(stream_callback)) handler <- write_stream(stream_callback)

  res <- VERB(
    method,
    handler,
    url = str_interp("${base_url}${utils::URLencode(path)}"),
    add_headers("Authorization"=str_interp("Bearer ${get_auth_token()}")),
    query = query,
    body = payload,
    encode="json"
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

#' @importFrom dplyr collect
#' @importFrom data.table as.data.table
#' @importFrom arrow read_ipc_stream open_dataset write_feather
#' @importFrom uuid UUIDgenerate
make_rows_request <- function(uri, max_results, selected_variables = NULL, type = 'tibble', schema = NULL){
  read_session <- make_request(
    method="post",
    path=str_interp("${uri}/readSessions"),
    parse_response=TRUE,
    payload=list(
        "maxResults"=max_results,
        "selectedVariables"=selected_variables,
        "format"="arrow"
    )
  )

  folder <- str_interp('/tmp/redivis/tables/${uuid::UUIDgenerate()}')

  dir.create(folder, recursive = TRUE)
  dir.create(str_interp('${folder}/stream'))
  dir.create(str_interp('${folder}/feather'))

  for (stream in read_session["streams"]){
    arrow_response <- make_request(
      method="get",
      path=str_interp('/readStreams/${stream[[1]]$id}'),
      parse_response=FALSE,
      download_path=str_interp('${folder}/stream/${stream[[1]]$id}')
    )

    # Temporarily store the stream as a file, and then remove it, avoiding needing to bring it into memory
    arrow::write_feather(arrow::read_ipc_stream(str_interp('${folder}/stream/${stream[[1]]$id}'), as_data_frame = FALSE), str_interp('${folder}/feather/${stream[[1]]$id}'))
    file.remove(str_interp('${folder}/stream/${stream[[1]]$id}'))
  }
  arrow_dataset <- arrow::open_dataset(str_interp('${folder}/feather'), format = "feather", schema = schema)

  # TODO: remove head() once BE is sorted

  if (type == 'tibble'){
    head(arrow_dataset, max_results) %>% dplyr::collect()
  } else if (type == 'arrow_table'){
    head(arrow_dataset, max_results)
  } else if (type == 'arrow_dataset'){
    arrow_dataset
  } else if (type == 'data_frame'){
    as.data.frame(head(arrow_dataset, max_results))
  } else if (type == 'data_table'){
    as.data.table(head(arrow_dataset, max_results))
  }


}
