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
      cat(str_interp("<Query ${self$properties$id %||% '[uninitialized]'}>\n"))
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
      lapply(variables, function(variable_properties) {
        Variable$new(
          name = variable_properties$name,
          query = self,
          properties = variable_properties
        )
      })
    }
  )
)

# Creates the query, once. With `list_rows` (the API's listRows options), the
# API responds with the query's rows rather than the query itself if it
# finishes quickly enough, in which case this returns that response, streaming,
# for the caller to read. Content-Location (the URI of the rows) is what tells
# the two apart: a JSONL body's content type starts with application/json.
initiate_query <- function(query, list_rows = NULL) {
  if (query$did_initiate) {
    return(invisible(NULL))
  }
  if (is.null(list_rows)) {
    query$properties <- make_request(
      method = 'POST',
      path = "/queries",
      payload = query$payload
    )
    query$did_initiate <- TRUE
    query$uri <- query$properties$uri
    return(invisible(NULL))
  }

  res <- make_request(
    method = 'POST',
    path = "/queries",
    payload = c(query$payload, list(listRows = list_rows)),
    as_connection = TRUE
  )
  rows_response <- NULL
  if (is.null(httr2::resp_header(res, "content-location"))) {
    query$properties <- jsonlite::fromJSON(
      httr2::resp_body_string(res),
      simplifyVector = FALSE
    )
  } else {
    # Location is the URI of the query
    id <- basename(httr2::resp_header(res, "location"))
    query$properties <- list(
      kind = 'query',
      id = id,
      uri = str_interp("/queries/${id}"),
      status = 'completed'
    )
    rows_response <- res
  }
  query$did_initiate <- TRUE
  query$uri <- query$properties$uri
  rows_response
}

# Waits for a query to be readable, for a read of its rows. A read that runs
# the query asks for the rows along with it, and returns the response
# containing them if the API sent them; it's verified by its end-of-stream
# sentinel, since it doesn't say how many rows to expect.
query_prepare_read <- function(query, max_results = NULL, variables = NULL) {
  list_rows <- list(format = 'arrow', eosSentinel = TRUE)
  if (!is.null(max_results)) {
    list_rows$maxResults <- max_results
  }
  # Variables are validated once the query has run, so only names are sent,
  # leaving anything else for that to report. I() keeps a single name an array.
  if (is.character(variables) && length(variables) > 0) {
    list_rows$selectedVariables <- I(variables)
  }
  rows_response <- initiate_query(query, list_rows = list_rows)
  if (!is.null(rows_response)) {
    return(rows_response)
  }
  query_wait_for_finish(query)
  # A query that responded with its rows didn't tell us its size, which a later
  # read needs in order to choose how to read them
  if (!("outputNumBytes" %in% names(query$properties))) {
    query$get()
  }
  NULL
}

query_wait_for_finish <- function(query) {
  initiate_query(query)
  # A loop rather than recursion: one frame per poll would exhaust R's
  # expression nesting limit on queries that run for over an hour
  while (query$properties$status %in% c('running', 'queued')) {
    Sys.sleep(1)
    query$properties <- make_request(
      method = 'GET',
      path = str_interp("/queries/${query$properties$id}")
    )
  }
  if (query$properties$status == 'failed') {
    abort_redivis_job_error(
      message = query$properties$errorMessage,
      kind = query$properties$kind,
      status = query$properties$status
    )
  } else if (query$properties$status == 'cancelled') {
    abort_redivis_job_error(
      message = "Query job was cancelled",
      kind = query$properties$kind,
      status = query$properties$status
    )
  }
}
