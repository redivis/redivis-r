#' @include Dataset.R Project.R
Query <- setRefClass("Query",
  fields = list(query="character", default_dataset="Dataset", default_project="Project", properties="list"),
  methods = list(
    initialize = function(query, default_dataset=NULL, default_project=NULL){
      payload <- list(query=query)

      if (!is.null(default_project)){
        payload$defaultProject <- default_project$get_identifier()
      }
      if (!is.null(default_dataset)){
        payload$defaultDataset <- default_dataset$get_identifier()
      }

      .self$properties <- make_request(method='POST', path="/queries", payload=payload)
    },
    to_tibble = function() {
      res <- query_wait_for_finish(.self$properties)

      rows <- make_rows_request(uri=str_interp("/queries/${res$id}"), max_results = res$outputNumRows)

      rows_to_tibble(rows, res$outputSchema)
    }
  )
)

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
