#' @include Dataset.R Secret.R api_request.R
Organization <- setRefClass("Organization",
  fields = list(name="character"),
  methods = list(
    show = function(){
      print(str_interp("<Organization ${.self$name}>"))
    },
    dataset = function(name, version=NULL) {
      Dataset$new(name=name, version=version, organization=.self)
    },
    secret = function(name) {
      Secret$new(name=name, organization=.self)
    },

    exists = function(){
      # TODO: once we have org.get() endpoint, refactor
      res <- make_request(method="GET", path=str_interp("/organizations/${.self$name}/datasets"), stop_on_error=FALSE, query=list(maxResults=1))
      if (length(res$error)){
        if (res$status == 404){
          return(FALSE)
        } else {
          stop(str_interp("${res$error}: ${res$error_description}"))
        }
      } else {
        return(TRUE)
      }
    },

    list_datasets = function(max_results=NULL) {
      datasets <- make_paginated_request(path=str_interp("/organizations/${.self$name}/datasets"), page_size=100, max_results=max_results)
      purrr::map(datasets, function(dataset) {
        Dataset$new(name=dataset$name, properties=dataset, organization=.self)
      })
    }
  )
)

