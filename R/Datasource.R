
#' @include Dataset.R Workflow.R api_request.R
Datasource <- setRefClass("Datasource",
   fields = list(
     source="ANY",
     workflow="ANY",
     properties="list",
     uri="character"
   ),

   methods = list(
     initialize = function(source="", workflow=NULL, properties=list()){
       parsed_source <- source
       parsed_workflow <- workflow

       if (is.null(parsed_workflow)) {
         default_workflow <- Sys.getenv("REDIVIS_DEFAULT_WORKFLOW")
         if (default_workflow != "") {
           parsed_workflow <- Workflow$new(name=default_workflow)
         } else {
           stop("REDIVIS_DEFAULT_WORKFLOW must be set if no workflow is specified in the Datasource constructor")
         }
       }

       if (inherits(source, "Dataset") || inherits(source, "Workflow")) {
         parsed_source <- source$qualified_reference
       }

       callSuper(source=parsed_source,
                 workflow=parsed_workflow,
                 uri=str_interp("${workflow$uri}/dataSources/${parsed_source}"),
                 properties=properties
       )
     },

     show = function(){
       print(str_interp("<Datasource ${.self$uri}>"))
     },

     get = function(){
       .self$properties <- make_request(path=.self$uri)
       .self$uri = .self$properties$uri
       .self
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

     update = function(source_dataset=NULL, source_workflow=NULL, sample=NULL, version=NULL, mapped_tables=NULL){
       if (inherits(source_workflow, "Workflow")) {
         source_workflow <- source_workflow$qualified_reference
       }
       if (inherits(source_dataset, "Dataset")) {
         print('hello')
         source_dataset <- source_dataset$qualified_reference
       }

       if (is.list(mapped_tables) && is.null(names(mapped_tables))) {
         mapped_tables_dict <- list()
         for (pair in mapped_tables) {
           source_table <- pair[[1]]
           dest_table <- pair[[2]]
           if (inherits(source_table, "Table")) {
             source_table <- source_table$scoped_reference
           }
           if (inherits(dest_table, "Table")) {
             dest_table <- dest_table$scoped_reference
           }
           mapped_tables_dict[[source_table]] <- dest_table
         }
         mapped_tables <- mapped_tables_dict
       }

       if (!is.null(sample) || !is.null(version)) {
         if (is.null(source_dataset) && is.null(source_workflow)) {
           .self$get()
           sd <- .self$properties[["sourceDataset"]]
           source_dataset <- if (!is.null(sd)) sd[["qualifiedReference"]] else NULL
         }

         if (is.null(source_dataset) || !is.null(source_workflow)) {
           if (!is.null(sample) && isTRUE(sample)) {
             stop("Sampling is not applicable to datasources that reference a workflow")
           }
           if (!is.null(version)) {
             stop("Versions are not applicable to datasources that reference a workflow")
           }
         }
       }

       if (inherits(version, "Version")) {
         version <- version$tag
       }

       ds <- Dataset$new(name=source_dataset, version = version)
       source_dataset <- ds$qualified_reference

       if (identical(sample, FALSE)) {
         source_dataset <- sub(":sample", "", source_dataset)
       } else if (isTRUE(sample) && !grepl(":sample", source_dataset)) {
         source_dataset <- paste0(source_dataset, ":sample")
       }

       .self$properties <- make_request(
         method = "PATCH",
         path = .self$uri,
         payload = list(
           sourceDataset = source_dataset,
           sourceWorkflow = source_workflow,
           mappedTables = mapped_tables
         )
       )
       .self$uri <- .self$properties[["uri"]]

       return(.self)
     }
   )
)

