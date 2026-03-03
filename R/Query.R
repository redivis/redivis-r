#' @include Dataset.R Workflow.R api_request.R TabularReader.R
Query <- R6::R6Class(
  "Query",
  inherit = TabularReader,
  public = list(
    query = NULL,
    default_dataset = NULL,
    default_workflow = NULL,
    did_initiate = FALSE,
    payload = NULL,

    initialize = function(
      query,
      default_dataset = NULL,
      default_workflow = NULL
    ) {
      super$initialize()
      self$query <- query
      self$default_dataset <- default_dataset
      self$default_workflow <- default_workflow

      initial_payload <- list(query = query)

      if (!is.null(default_workflow) && default_workflow != "") {
        initial_payload$defaultWorkflow <- default_workflow
      }
      if (!is.null(default_dataset) && default_dataset != "") {
        initial_payload$defaultDataset <- default_dataset
      }

      self$payload <- initial_payload
    },

    print = function(...) {
      cat(str_interp("<Query ${self$properties$id %||% [uninitialized]}>\n"))
      invisible(self)
    },

    get = function() {
      initiate_query(self)
      self$properties <- make_request(
        method = 'GET',
        path = str_interp("/queries/${self$properties$id}")
      )
      self
    },

    variable = function(name) {
      query_wait_for_finish(self)
      Variable$new(name = name, query = self)
    },

    list_variables = function(max_results = NULL) {
      query_wait_for_finish(self)
      variables <- make_paginated_request(
        path = str_interp("${self$uri}/variables"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(
          name = variable_properties$name,
          query = self,
          properties = variable_properties
        )
      })
    }
  )
)

initiate_query <- function(query) {
  if (!query$did_initiate) {
    query$did_initiate <- TRUE
    query$properties <- make_request(
      method = 'POST',
      path = "/queries",
      payload = query$payload
    )
    query$uri <- query$properties$uri
  }
}

query_wait_for_finish <- function(query, count = 0) {
  initiate_query(query)
  if (
    query$properties$status == 'running' ||
      query$properties$status == 'queued'
  ) {
    Sys.sleep(1)
    query$properties <- make_request(
      method = 'GET',
      path = str_interp("/queries/${query$properties$id}")
    )
    query_wait_for_finish(query, count + 1)
  }
}
