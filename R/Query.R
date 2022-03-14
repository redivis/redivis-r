#' @include Dataset.R Project.R
Query <- setRefClass("Query",
  fields = list(query="character", default_dataset="character", default_project="character", properties="list"),
  methods = list(
    initialize = function(query, default_dataset=NULL, default_project=NULL){
      payload <- list(query=query)

      if (!is.null(default_project) && default_project != ""){
        payload$defaultProject <- default_project
      }
      if (!is.null(default_dataset) && default_project != "" ){
        payload$defaultDataset <- default_dataset
      }

      .self$properties <- make_request(method='POST', path="/queries", payload=payload)
    },
    to_tibble = function(max_results=NULL) {
      res <- query_wait_for_finish(.self$properties)

      max_results <- if(!is.null(max_results)) min(max_results, res$outputNumRows) else res$outputNumRows

      df <- make_rows_request(uri=str_interp("/queries/${res$id}"), max_results=max_results)

      set_tibble_types(df, res$outputSchema)
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
