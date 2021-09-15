#' @include Dataset.R Project.R
Table <- setRefClass("Table",
   fields = list(name="character", dataset="Dataset", project="Project"),
   methods = list(
     to_tibble = function(max_results=NULL, variables=NULL) {
       container <- if (length(dataset$name) == 0) project else dataset
       owner <- if(length(container$user$name) == 0) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")


       all_variables <- make_paginated_request(path=str_interp("${uri}/variables"))

       if (is.null(variables)){
          variables_list <- all_variables
          original_variable_names <- Map(function(variable) variable$name, all_variables)
       }else{
          variables <- str_split(variables, ',')
          original_variable_names <- variables
          lower_variable_names <- Map(function(variable) tolower(variable), variables)
          variables_list = list(
             Filter(
                function(variable) tolower(variable$name) %in% lower_variable_names,
                all_variables,
             )
          )
          # TODO
          # variables_list.sort(
          #    key=lambda variable: lower_variable_names.index(
          #       variable["name"].lower()
          #    )
          # )
       }


       table_metadata <- make_request(method="GET", path=uri)

       max_results <- if(!is.null(max_results)) min(max_results, table_metadata$numRows) else table_metadata$numRows

       rows <- make_rows_request(
         uri=uri,
         max_results=max_results,
         query=list(
           "selectedVariables" = paste(Map(function(variable) variable$name, all_variables), sep='', collapse=',')
         )
       )

       if (is.null(variables)){
          rows_to_tibble(rows, variables_list)
       }else {
          rows_to_tibble(
             rows,
             Map(
                function(variable, variable_name) {
                   variable$name <- variable_name
                   variable
                },
                variables_list,
                variables,
             ),
          )
       }

     }
   )
)
