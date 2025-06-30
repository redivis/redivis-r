#' @include Table.R User.R api_request.R util.R
Notebook <- setRefClass("Notebook",
   fields = list(
     name="character",
     workflow="ANY",
     properties="list",
     qualified_reference="character",
     scoped_reference="character",
     uri="character"
   ),

   #
   # IMPORTANT: any calls to R stop() need to use base::stop(), otherwise it'll call the stop method on the notebook, because R ?!
   #
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
           base::stop("Invalid notebook specifier, must be the fully qualified reference if no dataset or workflow is specified")
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
                 uri=str_interp("/notebooks/${URLencode(qualified_reference_val)}"),
                 properties=properties
       )
     },

     show = function(){
       print(str_interp("<Notebook `${.self$qualified_reference}`>"))
     },

     exists = function(){
       res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
       if (length(res$error)){
         if (res$status == 404){
           return(FALSE)
         } else {
           base::stop(str_interp("${res$error}: ${res$error_description}"))
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
       if (is.null(.self$properties[["outputTable"]])){
         return(NULL)
       }
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
               base::stop(current_job[["errorMessage"]])
             }
             break
           }
         }
       }
       .self
     },

     stop = function() {
       .self$properties = make_request(method="POST", path=str_interp("${.self$uri}/stop"))
       .self$uri = .self$properties$uri
       .self
     },

     create_output_table = function(data, name = NULL, geography_variables = NULL, append = FALSE){
       payload = list()
       if (!is.null(name)){
         payload=base::append(
           payload,
           list("name"= name)
         )
       }
       if (!is.null(geography_variables)){
         payload=base::append(
           payload,
           list("geographyVariables" = geography_variables)
         )
       }
       if (append){
         payload=base::append(
           payload,
           list("append" = "TRUE")
         )
       }

       should_remove_tempfile <- TRUE
       temp_file_path <- NULL

       if (is.character(data)){
         if (endsWith(data, ".parquet")) {
           should_remove_tempfile <- FALSE
           temp_file_path <- data
         } else {
           base::stop("Only paths to parquet files (ending in .parquet) are supported when a string argument is provided")
         }
       } else {
          if (is(data,"sf")){
            sf_column_name <- attr(data, "sf_column")
            if (is.null(geography_variables)){
              payload=base::append(
              payload,
              list("geographyVariables" = list(sf_column_name))
              )
            }
          }
         temp_file_path <- convert_data_to_parquet(data)
       }

       file_size <- base::file.info(temp_file_path)$size

       if (is.null(.self$properties$currentJob)){
         .self$get()
       }

       current_notebook_job_id = .self$properties[["currentJob"]][["id"]]

       res <- make_request(
         method="POST",
         path=str_interp("/notebookJobs/${current_notebook_job_id}/tempUploads"),
         payload=list(tempUploads=list(list(size=file_size, resumable=file_size>5e7)))
       )

       temp_upload = res$results[[1]]

       con <- base::file(temp_file_path, "rb")
       on.exit(close(con), add=TRUE)

       if (temp_upload$resumable) {
         perform_resumable_upload(
           data=con,
           temp_upload_url=temp_upload$url,
           proxy_url=str_interp("${Sys.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/${current_notebook_job_id}/tempUploadProxy")
         )
       } else {
         perform_standard_upload(
           data=con,
           temp_upload_url=temp_upload$url,
           proxy_url=str_interp("${Sys.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/${current_notebook_job_id}/tempUploadProxy")
           )
       }

       payload <- base::append(payload, list(tempUploadId=temp_upload$id))

       res <- make_request(method='PUT', path=str_interp("/notebookJobs/${current_notebook_job_id}/outputTable"), payload = payload)

       if (should_remove_tempfile){
         base::file.remove(temp_file_path)
       }

       user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[1]
       workflow_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[2]
       table <- User$new(name=user_name)$workflow(name=workflow_name)$table(name=res$name)

       table$properties <- res

       table
     }
   )
)
