#' @include Table.R Workflow.R api_request.R
Transform <- setRefClass("Transform",
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
          stop("Invalid transform specifier, must be the fully qualified reference if no dataset or workflow is specified")
        }
      }
      parent_reference <- str_interp("${parsed_workflow$qualified_reference}.")
      scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else parsed_name
      qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${parent_reference}${parsed_name}")
      callSuper(...,
                name=parsed_name,
                workflow=parsed_workflow,
                qualified_reference=qualified_reference_val,
                scoped_reference=scoped_reference_val,
                uri=str_interp("/transforms/${URLencode(qualified_reference_val)}"),
                properties=properties
      )
    },
    show = function(){
      print(str_interp("<Transform `${.self$qualified_reference}`>"))
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

    source_tables = function(){
      .self$get()

      lapply(.self$properties[["sourceTables"]], function(source_table) {
        Table$new(name=source_table[["qualifiedReference"]], properties = source_table)
      })
    },

    output_table = function(){
      .self$get()
      output_table_properties <- .self$properties[["outputTable"]]
      Table$new(name=output_table_properties[["qualifiedReference"]], properties = output_table_properties)
    },

    run = function(wait_for_finish=TRUE) {
      .self$properties = make_request(method="POST", path=str_interp("${.self$uri}/run"))
      .self$uri = .self$properties$uri

      if (wait_for_finish) {
        repeat {
          Sys.sleep(2)
          .self$get()

          current_job <- .self$properties[["currentJob"]]
          if (is.null(current_job)){
            # When the job finishes, we need to look at the lastRunJob for status
            current_job <- .self$properties[["lastRunJob"]]
          }

          if (!is.null(current_job) && current_job[["status"]] %in% c("completed", "failed")) {
            if (current_job[["status"]] == "failed") {
              stop(current_job[["errorMessage"]])
            }
            break
          }
        }
      }
      .self
    },

    cancel = function() {
      .self$properties = make_request(method="POST", path=str_interp("${.self$uri}/cancel"))
      .self$uri = .self$properties$uri
      .self
    }
  )
)
