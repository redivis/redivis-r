#' @include Workflow.R api_request.R
Parameter <- setRefClass("Parameter",
  fields = list(name="character", workflow="ANY", properties="list", qualified_reference="character", scoped_reference="character", uri="character"),
  methods = list(
    initialize = function(..., name="", workflow=NULL, properties=list()){
      parent_reference <- ""
      parsed_name <- name
      parsed_workflow <- workflow

      if (is.null(parsed_workflow)){
        split <- strsplit(name, "\\.")[[1]]
        if (length(split) == 3){
          parsed_name <- split[[3]]
          parsed_workflow <- Workflow$new(name=paste(split[1:2], collapse='.'))
        } else if (Sys.getenv("REDIVIS_DEFAULT_WORKFLOW") != ""){
          parsed_workflow <- Workflow$new(name=Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"))
        } else if (
          name != "" # Need this check otherwise package won't build (?)
        ){
          stop("Invalid parameter specifier, must be the fully qualified reference if no dataset or workflow is specified")
        }
      }
      uri_val <- properties$uri
      if (is.null(uri_val)){
        uri_val <- str_interp("${parsed_workflow$uri}/parameters/${URLencode(name)}")
      }
      parent_reference <- str_interp("${parsed_workflow$qualified_reference}.")
      scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else parsed_name
      qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${parent_reference}${parsed_name}")
      callSuper(...,
                name=parsed_name,
                workflow=parsed_workflow,
                qualified_reference=qualified_reference_val,
                scoped_reference=scoped_reference_val,
                uri=uri_val,
                properties=properties
      )
    },
    show = function(){
      print(str_interp("<Parameter `${.self$qualified_reference}`>"))
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

    get = function() {
      .self$properties = make_request(path=.self$uri)
      .self$uri = .self$properties$uri
      .self
    },

    get_values = function(){
      .self$get()
      .self$properties$values
    },

    update = function(name=NULL, type=NULL, values=NULL){
      payload = list()

      if (!is.null(name)){
        payload$name = name
      }
      if (!is.null(type)){
        payload$type = type
      }
      if (!is.null(values)){
        payload$values = values
      }


      .self$properties = make_request(
        method="PATCH",
        path=.self$uri,
        payload=payload,
      )
      .self$uri <- .self$properties$uri
      .self
    },

    create = function(type=NULL, values=NULL){
      payload = list(name=.self$name, values=values)
      if (!is.null(type)){
        payload$type = type
      }

      .self$properties = make_request(
        method="POST",
        path=str_interp("${.self$workflow$uri}/parameters"),
        payload=payload,
      )
      .self$uri <- .self$properties$uri
      .self
    },

    delete = function(){
      make_request(
        method="DELETE",
        path=.self$uri,
      )
      return(NULL)
    }
  )
)
