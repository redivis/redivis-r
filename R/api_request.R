

#' @importFrom httr VERB headers add_headers content status_code
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_interp str_starts
make_request <- function(method='GET', query=NULL, payload = NULL, parse_response=TRUE, path = ""){
  base_url <- if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1" else Sys.getenv("REDIVIS_API_ENDPOINT")

  res <- VERB(
    method,
    url = str_interp("${base_url}${path}"),
    add_headers("Authorization"=str_interp("Bearer ${get_auth_token()}")),
    query = query,
    body = payload,
    encode="json"
  )

  response_content = content(res, as="text", encoding='UTF-8')
  print(is.null(headers(res)$'content-type'))
  print(status_code(res))
  print(headers(res)$'content-type')
  print(headers(res))
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


#' @importFrom jsonlite fromJSON
make_rows_request <- function(uri, max_results, query=list()){
  page = 0
  page_size = 100000

  rows = ""

  while (page * page_size < max_results){
    results <- make_request(
      method="GET",
      path=str_interp("${uri}/rows"),
      parse_response=FALSE,
      query=append(
        query,
        list(
          "startIndex"=page * page_size,
          "maxResults"=if ((page  + 1) * page_size < max_results) page_size else max_results - (page * page_size)
        )
      )
    )
    if (page != 0){
      rows = paste0(rows, '\n')
    }

    rows = paste0(rows, results)

    page = page + 1
  }


  if (rows == ""){
    list()
  } else {
    Map(function(row) fromJSON(row), unlist(strsplit(rows, '\n')))
  }
}
