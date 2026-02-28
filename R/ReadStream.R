#' @include Dataset.R Workflow.R api_request.R
ReadStream <- R6::R6Class(
  "ReadStream",
  inherit = TabularReader,
  public = list(
    id = NULL,
    table = NULL,
    query = NULL,
    upload = NULL,
    uri = NULL,
    initialize = function(
      id,
      table = NULL,
      query = NULL,
      upload = NULL,
      properties = list()
    ) {
      self$uri <- str_interp("/readStreams/${id}")
      self$id <- id
      self$table <- table
      self$query <- query
      self$upload <- upload
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp("<ReadStream ${self$id}>\n"))
      invisible(self)
    }
  )
)
