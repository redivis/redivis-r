#' @include Table.R Upload.R
Variable <- setRefClass("Variable",
  fields = list(name="character", table="ANY", upload="ANY", properties="list", uri="character"),
  methods = list(
    initialize = function(..., name="", table=NULL, upload=NULL, properties=list()){
      parent_uri <- if (is.null(upload)) table$uri else upload$uri
      uri_val <- if (length(properties$uri)) properties$uri else str_interp("${parent_uri}}/variables/${name}")
      callSuper(...,
                name=name,
                upload=upload,
                table=table,
                uri=uri_val,
                properties=properties
      )
    },
    show = function(){
      print(str_interp("<Variable `${.self$table$qualified_reference}`.${.self$name}>"))
    },
    get = function(wait_for_statistics=FALSE) {
      .self$properties = make_request(path=.self$uri)
      .self$uri = .self$properties$uri
      while (wait_for_statistics && .self$properties$statistics$status == "running"){
        Sys.sleep(2)
        .self$properties = make_request(path=.self$uri)
        .self$uri = .self$properties$uri
      }
      .self
    },

    exists = function(){
      res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
      if (length(res$error)){
        if (res$error$status == 404){
          return(FALSE)
        } else {
          stop(res$error$message)
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
