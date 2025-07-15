#' @include Table.R api_request.R
Variable <- setRefClass("Variable",
  fields = list(name="character", table="ANY", upload="ANY", query="ANY", properties="list", uri="character"),
  methods = list(
    initialize = function(..., name="", table=NULL, upload=NULL, query=NULL, properties=list()){
      parent_uri <- NULL
      if (!is.null(table)){
        parent_uri <- table$uri
      } else if (!is.null(upload)){
        parent_uri <- upload$uri
      } else {
        parent_uri <- query$uri
      }
      uri_val <- if (length(properties$uri)) properties$uri else str_interp("${parent_uri}/variables/${name}")
      callSuper(...,
                name=name,
                upload=upload,
                table=table,
                query=query,
                uri=uri_val,
                properties=properties
      )
    },
    show = function(){
      if (!is.null(.self$table)){
        print(str_interp("<Variable `${.self$table$qualified_reference}`.${.self$name}>"))
      } else if (!is.null(.self$upload)){
        print(str_interp("<Variable `${.self$table$qualified_reference}.${.self$upload$name}`.${.self$name}>"))
      } else {
        print(str_interp("<Variable `${.self$query$uri}`.${.self$name}>"))
      }
    },
    get = function(wait_for_statistics=FALSE) {
      if (wait_for_statistics){
        warning('Calling variable$get with the wait_for_statistics parameter is deprecated. Please use variable$get_statistics() instead.')
      }
      .self$properties = make_request(path=.self$uri)
      .self$uri = .self$properties$uri
      while (wait_for_statistics && .self$properties$statistics$status == "running"){
        Sys.sleep(2)
        .self$properties = make_request(path=.self$uri)
        .self$uri = .self$properties$uri
      }
      .self
    },

    get_statistics = function(){
      statistics <- make_request(path=str_interp("${.self$uri}/statistics"))
      while (statistics$status == "running" || statistics$status == "queued"){
        Sys.sleep(2)
        statistics <- make_request(path=str_interp("${.self$uri}/statistics"))
      }
      statistics
    },

    exists = function(){
      res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
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

    update = function(label=NULL, description=NULL, value_labels=NULL){
      payload <- list()
      if (!is.null(label)){
        payload <- append(payload, list("label"=label))
      }
      if (!is.null(description)){
        payload <- append(payload, list("description"=description))
      }
      if (!is.null(value_labels)){
        payload <- append(payload, list("value_labels"=value_labels))
      }
      .self$properties = make_request(
        method="PATCH",
        path=.self$uri,
        payload=payload,
      )
      .self$uri = .self$properties$uri
      .self
    }
  )
)
