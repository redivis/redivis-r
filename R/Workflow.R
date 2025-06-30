#' @include User.R Datasource.R Notebook.R Transform.R Table.R api_request.R
Workflow <- setRefClass("Workflow",
   fields = list(name="character",
                 user="ANY",
                 uri="character",
                 qualified_reference="character",
                 scoped_reference="character",
                 properties="list"
             ),
   methods = list(

     initialize = function(..., name="", user=NULL, properties=list()){
       parsed_user <- user
       parsed_name <- name
       if (is.null(user)){
         split <- strsplit(name, "\\.")[[1]]
         if (length(split) == 2){
           parsed_name <- split[[2]]
           username <- split[[1]]
           parsed_user <- User$new(name=username)
         } else if (Sys.getenv("REDIVIS_DEFAULT_USER", FALSE)){
           parsed_user <- User$new(name=Sys.getenv("REDIVIS_DEFAULT_USER"))
         } else {
           stop("Invalid workflow specifier, must be the fully qualified reference if no owner is specified")
         }
       }
       qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${parsed_user$name}.${parsed_name}")
       scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else parsed_name
       callSuper(...,
                 name=parsed_name,
                 user=parsed_user,
                 qualified_reference=qualified_reference_val,
                 scoped_reference=scoped_reference_val,
                 uri=str_interp("/workflows/${URLencode(qualified_reference_val)}"),
                 properties=properties
       )
     },

     show = function(){
       print(str_interp("<Workflow ${.self$qualified_reference}>"))
     },

     get = function(){
       res <- make_request(path=.self$uri)
       update_workflow_properties(.self, res)
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

     table = function(name) {
       Table$new(name=name, workflow=.self)
     },

     notebook = function(name){
       Notebook$new(name=name, workflow=.self)
     },

     transform = function(name){
       Transform$new(name=name, workflow=.self)
     },

     datasource = function(dataset=NULL, workflow=NULL){
       if (is.null(dataset) && is.null(workflow)){
          stop("Either a dataset or a workflow must be specified")
       }
       if (!is.null(dataset) && !is.character(dataset)){
         dataset <- dataset$uri
       }
       if (!is.null(workflow) && !is.character(workflow)){
         workflow <- workflow$uri
       }
       Datasource$new(source=if(!is.null(dataset)) dataset else workflow, workflow=.self)
     },

     query = function(query){
       redivis$query(query, default_workflow = .self$qualified_reference)
     },

     list_tables = function(max_results=NULL) {
       tables <- make_paginated_request(
         path=str_interp("${.self$uri}/tables"),
         page_size=100,
         max_results=max_results,
       )
       purrr::map(tables, function(table_properties) {
         Table$new(name=table_properties$name, workflow=.self, properties=table_properties)
       })
     },

     list_transforms = function(max_results=NULL) {
       transforms <- make_paginated_request(
         path=str_interp("${.self$uri}/transforms"),
         page_size=100,
         max_results=max_results,
       )
       purrr::map(transforms, function(transform_properties) {
         Transform$new(name=transform_properties$name, workflow=.self, properties=transform_properties)
       })
     },

     list_notebooks = function(max_results=NULL) {
       notebooks <- make_paginated_request(
         path=str_interp("${.self$uri}/notebooks"),
         page_size=100,
         max_results=max_results,
       )
       purrr::map(notebooks, function(notebook_properties) {
         Notebook$new(name=notebook_properties$name, workflow=.self, properties=notebook_properties)
       })
     },

     list_datasources = function(max_results=NULL) {
       datasources <- make_paginated_request(
         path=str_interp("${.self$uri}/dataSources"),
         page_size=100,
         max_results=max_results,
       )
       purrr::map(datasources, function(datasource_properties) {
         Datasource$new(source=datasource_properties$id, workflow=.self, properties=datasource_properties)
       })
     }
   )
)

update_workflow_properties <- function(instance, properties){
  instance$properties = properties
  instance$qualified_reference = properties$qualifiedReference
  instance$scoped_reference = properties$scopedReference
  instance$name = properties$name
  instance$uri = properties$uri
}

