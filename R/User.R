User <- setRefClass("User",
  fields = list(name="character"),
  methods = list(
    show = function(){
      print(str_interp("<User ${.self$name}>"))
    },
    dataset = function(name, version="current") {
      Dataset$new(name=name, version=version, user=.self)
    },

    project = function(name) {
      Project$new(name=name, user=.self)
    },

    list_datasets = function(max_results=NULL) {
      datasets <- make_paginated_request(path=str_interp("/users/${.self$name}/datasets"), page_size=100, max_results=max_results)
      purrr::map(datasets, function(dataset) {
        Dataset$new(name=dataset$name, version="current", properties=dataset, user=.self)
      })
    },

    list_projects = function(max_results=NULL) {
      projects <- make_paginated_request(path=str_interp("/users/${.self$name}/projects"), page_size=100, max_results=max_results)
      purrr::map(projects, function(project) {
        Project$new(name=project$name, properties=project, user=.self)
      })
    }
  )
)
