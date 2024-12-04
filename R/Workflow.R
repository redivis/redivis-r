#' @include User.R api_request.R
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
       qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${user$name}.${name}")
       scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else name
       callSuper(...,
                 name=name,
                 user=user,
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
           stop(res$message)
         }
       } else {
         return(TRUE)
       }
     },

     table = function(name) {
       Table(name=name, workflow=.self)
     },

     query = function(query){
       redivis::query(query, default_workflow = .self$qualified_reference)
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

