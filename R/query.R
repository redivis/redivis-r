#' @title query
#'
#' @description Execute a redivis query
#'
#' @param query The query string to execute
#'
#' @return A data frame object containing the query results
#' @examples
#' output_table <- query(query = 'SELECT 1 + 1 AS two')
#' @export
query <- function(query="") {
  res <- make_request(method='POST', path="/queries", body=list(query=query))
  res <- query_wait_for_finish(res)

  rows <- make_rows_request(uri=str_interp("/queries/${res$id}"), max_results = res$outputNumRows)

  df <- data.frame(matrix(unlist(rows), nrow=length(rows), byrow=TRUE), stringsAsFactors=FALSE)
  colnames(df) <- Map(function(variable) variable$name, res$outputSchema)

  options(digits.secs = 6)
  for (variable in res$outputSchema){
    if (variable$type == 'integer'){
      df[[variable$name]] <- as.integer(df[[variable$name]])
    } else if (variable$type == 'float'){
      df[[variable$name]] <- as.double(df[[variable$name]])
    } else if (variable$type == 'boolean'){
      df[[variable$name]] <- as.logical(df[[variable$name]])
    } else if (variable$type == 'date'){
      df[[variable$name]] <- as.Date(df[[variable$name]])
    } else if (variable$type == 'dateTime'){
      df[[variable$name]] <- as.POSIXlt(df[[variable$name]], format="%Y-%m-%dT%H:%M:%OS")
    } else if (variable$type == 'time'){
      # Leave as string representation, there's no R native time type
      # df[[variable$name]] <- as.difftime(df[[variable$name]])
    }
  }
}

query_wait_for_finish <- function(previous_response, count = 0){
  if (previous_response$status == 'running' || previous_response$status == 'queued'){
    Sys.sleep(2)
    count = count + 1
    res <- make_request(method='GET', path=str_interp("/queries/${previous_response$id}"))
    query_wait_for_finish(res, count)
  } else {
    previous_response
  }
}
