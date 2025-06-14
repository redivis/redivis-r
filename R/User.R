#' @include Dataset.R Workflow.R Secret.R api_request.R
User <- setRefClass("User",
  fields = list(name="character"),
  methods = list(
    show = function(){
      print(str_interp("<User ${.self$name}>"))
    },
    dataset = function(name, version=NULL) {
      Dataset$new(name=name, version=version, user=.self)
    },

    workflow = function(name) {
      Workflow$new(name=name, user=.self)
    },

    project = function(name) {
      warning("Deprecation warning: Projects have been renamed to Workflows, please update your code to: user$workflow()")
      Workflow$new(name=name, user=.self)
    },

    secret = function(name) {
        Secret$new(name=name, user=.self)
    },

    list_datasets = function(max_results=NULL) {
      datasets <- make_paginated_request(path=str_interp("/users/${.self$name}/datasets"), page_size=100, max_results=max_results)
      purrr::map(datasets, function(dataset) {
        Dataset$new(name=dataset$name, properties=dataset, user=.self)
      })
    },

    list_workflows = function(max_results=NULL) {
      workflows <- make_paginated_request(path=str_interp("/users/${.self$name}/workflows"), page_size=100, max_results=max_results)
      purrr::map(workflows, function(workflow) {
        Workflow$new(name=workflow$name, properties=workflow, user=.self)
      })
    },

    list_projects = function(max_results=NULL) {
      warning("Deprecation warning: Projects have been renamed to Workflows, please update your code to: user$list_workflows()")
      workflows <- make_paginated_request(path=str_interp("/users/${.self$name}/workflows"), page_size=100, max_results=max_results)
      purrr::map(workflows, function(workflow) {
        Workflow$new(name=workflow$name, properties=workflow, user=.self)
      })
    }
  )
)
