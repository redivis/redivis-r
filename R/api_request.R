base_url <- if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1query" else Sys.getenv("REDIVIS_API_ENDPOINT")

#' @importFrom httr VERB headers add_headers content status_code
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_interp str_starts
make_request <- function(method='GET', query=NULL, body = NULL, parse_response=TRUE, path = ""){
  print(base_url)
  res <- VERB(
    method,
    url = str_interp("${base_url}${path}"),
    add_headers("Authorization"=str_interp("Bearer ${get_auth_token()}")),
    query = query,
    body = body,
    encode="json"
  )

  response_content = content(res, as="text", encoding='UTF-8')

  if (str_starts(headers(res)$'content-type', 'application/json') && (status_code(res) >= 400 || parse_response)){
    response_content <- fromJSON(response_content, simplifyVector = FALSE)
  }

  if (status_code(res) >= 400){
    if (str_starts(headers(res)$'content-type', 'application/json')){
      stop(response_content$error$message)
    } else {
      stop(response_content)
    }
  } else {
    response_content
  }
}

#' @importFrom jsonlite fromJSON
make_rows_request <- function(uri, max_results, query){
  page = 0
  page_size = 100000

  rows = ""

  while (page * page_size < max_results){
    results <- make_request(
      method="GET",
      path=str_interp("${uri}/rows"),
      parse_response=FALSE,
      query=list(
          "startIndex"=page * page_size,
          "maxResults"=if ((page  + 1) * page_size < max_results) page_size else max_results - (page * page_size)
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
