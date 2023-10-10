#' @include Dataset.R Project.R util.R
#' @importFrom stringr str_interp
#' @importFrom pbapply pblapply
#' @importFrom purrr map
Table <- setRefClass("Table",
   fields = list(name="character", dataset="Dataset", project="Project", properties="list"),

   methods = list(
     to_arrow_dataset = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL){
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'arrow_dataset',
         schema = params$schema,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )

     },

     to_arrow_table = function(max_results=NULL, variables=NULL, batch_preprocessor=NULL) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'arrow_table',
         schema = params$schema,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )
     },

     to_tibble = function(max_results=NULL, variables=NULL, geography_variable='', batch_preprocessor=NULL) {
       params <- get_table_request_params(.self, max_results, variables, geography_variable)

       if (!is.null(params$geography_variable)){
         warning('Returning sf tibbles via the to_tibble method is deprecated, and will be removed soon. Please use table$to_sf_tibble() instead.', immediate. = TRUE)
       }

       df <- make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'tibble',
         schema = params$schema,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )

       if (!is.null(params$geography_variable)){
         st_as_sf(df, wkt=params$geography_variable, crs=4326)
       } else {
         df
       }
     },

     to_sf_tibble = function(max_results=NULL, variables=NULL, geography_variable='', batch_preprocessor=NULL) {
       params <- get_table_request_params(.self, max_results, variables, geography_variable)

       if (is.null(params$geography_variable)){
         stop('Unable to find geography variable in table')
       }

       df <- make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'tibble',
         schema = params$schema,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )

       st_as_sf(df, wkt=params$geography_variable, crs=4326)
     },

     to_data_frame = function(max_results=NULL, variables=NULL, batch_preprocessor=NULL) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'data_frame',
         schema = params$schema,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )
     },

     to_data_table = function(max_results=NULL, variables=NULL, batch_preprocessor=NULL) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         max_results=params$max_results,
         selected_variables = params$selected_variables,
         type = 'data_table',
         schema = params$schema,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor
       )
     },

     list_files = function(max_results = NULL, file_id_variable = NULL){
       container <- if (length(dataset$name) == 0) project else dataset
       owner <- if(length(container$user$name) == 0) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")

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
         selected_variables = list(file_id_variable),
         type='data_table',
         schema=arrow::schema(arrow::field(file_id_variable, string())),
         progress=FALSE
       )
       purrr::map(df[[file_id_variable]], function(id) {
         File$new(id=id)
       })
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
          selected_variables = list(file_id_variable),
          type='data_table',
          schema=arrow::schema(arrow::field(file_id_variable, string())),
          progress=FALSE
        )
        pblapply(df[[file_id_variable]], function(id) {
          File$new(id=id)$download(path = path, overwrite = overwrite)
        })
      }
   )
)

get_table_request_params = function(self, max_results, variables, geography_variable=NULL){
  container <- if (length(self$dataset$name) == 0) self$project else self$dataset
  owner <- if(length(container$user$name) == 0) container$organization else container$user
  uri <- str_interp("/tables/${owner$name}.${container$name}.${self$name}")

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
    variables_list <- sapply(
      lower_variable_names,
      function (name) all_variables[match(name, sapply(all_variables, function(variable) tolower(variable$name)))][1]
    )
  }

  table_metadata <- make_request(method="GET", path=uri)
  self$properties = table_metadata

  max_results <- if(!is.null(max_results)) min(max_results, table_metadata$numRows) else table_metadata$numRows

  selected_variables <- if (is.null(variables)) NULL else Map(function(variable_name) variable_name, variables)
  schema <- get_arrow_schema(variables_list)

  if (!is.null(geography_variable) && geography_variable == ''){
    geography_variable = NULL
    for (variable in variables_list){
      if (variable$type == 'geography'){
        geography_variable <- variable$name
        break
      }
    }
  }

  list(
    "max_results" = max_results,
    "uri" = uri,
    "selected_variables" = selected_variables,
    "variables_list" = variables_list,
    "schema"=schema,
    "geography_variable"=geography_variable,
    "coerce_schema"=is.null(table_metadata$container) || table_metadata$container$kind == 'dataset'
  )
}
