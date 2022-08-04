

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

#' @importFrom arrow read_ipc_stream
#' @importFrom dplyr bind_rows
make_rows_request <- function(uri, max_results, selected_variables = NULL){
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

  data_frames = list()
  for (stream in read_session["streams"]){
    arrow_response <- make_request(
      method="get",
      path=str_interp('/readStreams/${stream[[1]]$id}'),
      parse_response=FALSE
    )
    data_frames <- append(data_frames, read_ipc_stream(content(arrow_response, type="raw"), as_data_frame=TRUE))
  }
  # TODO: remove once BE is sorted
  bind_rows(data_frames)[0:max_results,]

}
