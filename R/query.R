#' @title query
#'
#' @description Execute a redivis query
#'
#' @param query The query string to execute
#'
#' @return A data frame object containing the query results
#' @examples
#' output_table <- query(query = 'SELECT 1 + 1 AS two')
#' @import tibble
#' @importFrom hms as_hms
#' @export
query <- function(query="", default_project=NULL, default_dataset=NULL) {
  payload <- list(query=query)

  if (!is.null(default_project)){
    payload$defaultProject <- default_project$get_identifier()
  }
  if (!is.null(default_dataset)){
    payload$defaultDataset <- default_dataset$get_identifier()
  }

  res <- make_request(method='POST', path="/queries", payload=payload)
  res <- query_wait_for_finish(res)

  rows <- make_rows_request(uri=str_interp("/queries/${res$id}"), max_results = res$outputNumRows)

  rows_to_dataframe(rows, res$outputSchema)
}

query_wait_for_finish <- function(previous_response, count = 0){
  if (previous_response$status == 'running' || previous_response$status == 'queued'){
    Sys.sleep(1)
    count = count + 1
    res <- make_request(method='GET', path=str_interp("/queries/${previous_response$id}"))
    query_wait_for_finish(res, count)
  } else {
    previous_response
  }
}
