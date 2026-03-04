#' @include Dataset.R Secret.R Workflow.R api_request.R
Organization <- R6::R6Class(
  "Organization",
  public = list(
    name = NULL,

    initialize = function(name = "") {
      self$name <- name
    },

    print = function(...) {
      cat(str_interp("<Organization ${self$name}>\n"))
      invisible(self)
    },
    dataset = function(name, version = NULL) {
      Dataset$new(name = name, version = version, organization = self)
    },
    secret = function(name) {
      Secret$new(name = name, organization = self)
    },
    workflow = function(name) {
      Workflow$new(name = name, organization = self)
    },

    exists = function() {
      # TODO: once we have org.get() endpoint, refactor
      tryCatch(
        {
          make_request(
            method = "HEAD",
            path = str_interp("/organizations/${self$name}/datasets"),
            query = list(maxResults = 1)
          )
          TRUE
        },
        redivis_not_found_error = function(e) {
          FALSE
        }
      )
    },

    list_datasets = function(max_results = NULL) {
      datasets <- make_paginated_request(
        path = str_interp("/organizations/${self$name}/datasets"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(datasets, function(dataset) {
        Dataset$new(
          name = dataset$name,
          properties = dataset,
          organization = self
        )
      })
    },

    list_workflows = function(max_results = NULL) {
      workflows <- make_paginated_request(
        path = str_interp("/organizations/${self$name}/workflows"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(workflows, function(workflow) {
        Workflow$new(
          name = workflow$name,
          properties = workflow,
          organization = self
        )
      })
    }
  )
)
