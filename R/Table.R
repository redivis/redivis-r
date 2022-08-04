#' @include Dataset.R Project.R
#' @importFrom stringr str_interp
#' @importFrom pbapply pblapply
Table <- setRefClass("Table",
   fields = list(name="character", dataset="Dataset", project="Project"),

   methods = list(
     to_tibble = function(max_results=NULL, variables=NULL, geography_variable='') {
       container <- if (length(dataset$name) == 0) project else dataset
       owner <- if(length(container$user$name) == 0) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")


       all_variables <- make_paginated_request(path=str_interp("${uri}/variables"), page_size=1000)

       if (is.null(variables)){
          variables_list <- all_variables
       }else{
         variables = as.list(variables)
          lower_variable_names <- Map(function(variable) tolower(variable), variables)
          variables_list <- Filter(
              function(variable) tolower(variable$name) %in% lower_variable_names,
              all_variables
           )
          variables_list <- sapply(lower_variable_names,
            function (name) all_variables[match(name, sapply(all_variables, function(variable) tolower(variable$name)))][1]
         )
       }

       table_metadata <- make_request(method="GET", path=uri)

       max_results <- if(!is.null(max_results)) min(max_results, table_metadata$numRows) else table_metadata$numRows

       df <- make_rows_request(
         uri=uri,
         max_results=max_results,
         selected_variables = if (is.null(variables)) NULL else Map(function(variable_name) variable_name, variables)
       )

       set_tibble_types(df, variables_list, geography_variable)
     },

     download_files = function(path = getwd(), overwrite = FALSE, max_results = NULL, file_id_variable = NULL){
       container <- if (length(dataset$name) == 0) project else dataset
       owner <- if(length(container$user$name) == 0) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")

       if (!endsWith(path, '/')) path = str_interp("${path}/")

        if (is.null(file_id_variable)){
          file_id_variables <- make_paginated_request(path=str_interp("${uri}/variables"), max_results=2, query = list(isFileId = TRUE))

                    if (length(file_id_variables) == 0){
          stop("No variable containing file ids was found on this table")
          } else if (length(file_id_variables) > 1){
          stop("This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter.")
          }
          file_id_variable = file_id_variables[[1]]$'name'
        }

         table_metadata <- make_request(method="GET", path=uri)
         max_results <- if(!is.null(max_results)) min(max_results, table_metadata$numRows) else table_metadata$numRows

        df <- make_rows_request(
          uri=uri,
          max_results=max_results,
          selected_variables = list(file_id_variable)
        )
        pblapply(df[[file_id_variable]], function(id) {
          File$new(id=id)$download(path = path, overwrite = overwrite)
        })
      }
   )
)
