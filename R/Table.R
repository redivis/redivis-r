#' @include Dataset.R Project.R
Table <- setRefClass("Table",
   fields = list(name="character", dataset="Dataset", project="Project"),
   methods = list(
     to_tibble = function(max_results=NULL) {
       container <- if (length(dataset$name) == 0) project else dataset
       owner <- if(length(container$user$name) == 0) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")
       variables <- make_paginated_request(path=str_interp("${uri}/variables"))

       table_metadata <- make_request(method="GET", path=uri)

       max_results <- if(!is.null(max_results)) min(max_results, table_metadata$numRows) else table_metadata$numRows

       rows <- make_rows_request(
         uri=uri,
         max_results=max_results,
         query=list(
           "selectedVariables" = paste(Map(function(variable) variable$name, variables), sep='', collapse=',')
         )
       )

       rows_to_tibble(rows, variables)
     }
   )
)
