#' @include Dataset.R api_request.R
Organization <- setRefClass("Organization",
  fields = list(name="character"),
  methods = list(
    show = function(){
      print(str_interp("<Organization ${.self$name}>"))
    },
    dataset = function(name, version=NULL) {
      Dataset$new(name=name, version=version, organization=.self)
    },
    list_datasets = function(max_results=NULL) {
      datasets <- make_paginated_request(path=str_interp("/organizations/${.self$name}/datasets"), page_size=100, max_results=max_results)
      purrr::map(datasets, function(dataset) {
        Dataset$new(name=dataset$name, properties=dataset, organization=.self)
      })
    }
  )
)

