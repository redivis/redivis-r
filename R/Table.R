
#' @include Dataset.R Project.R Variable.R Export.R util.R api_request.R
Table <- setRefClass("Table",
   fields = list(name="character", dataset="ANY", project="ANY", properties="list", qualified_reference="character", scoped_reference="character", uri="character"),

   methods = list(
     initialize = function(..., name="", dataset=NULL, project=NULL, properties=list()){
       parent_reference <- if (is.null(dataset)) project$qualified_reference else dataset$qualified_reference
       scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else name
       qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${parent_reference}.${name}")
       callSuper(...,
                 name=name,
                 dataset=dataset,
                 project=project,
                 qualified_reference=qualified_reference_val,
                 scoped_reference=scoped_reference_val,
                 uri=str_interp("/tables/${URLencode(qualified_reference_val)}"),
                 properties=properties
       )
     },

     show = function(){
       print(str_interp("<Table ${.self$qualified_reference}>"))
     },

     get = function(){
       res <- make_request(path=.self$uri)
       update_table_properties(.self, res)
       .self
     },

     exists = function(){
       res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
       if (length(res$error)){
         if (res$error$status == 404){
           return(FALSE)
         } else {
           stop(res$error$message)
         }
       } else {
         return(TRUE)
       }
     },

     update = function(name=NULL, description=NULL, upload_merge_strategy=NULL){
       payload <- list()
       if (!is.null(name)){
         payload <- append(payload, list("name"=name))
       }
       if (!is.null(description)){
         payload <- append(payload, list("description"=description))
       }
       if (!is.null(upload_merge_strategy)){
         payload <- append(payload, list("upload_merge_strategy"=upload_merge_strategy))
       }
       res <- make_request(
         method="PATCH",
         path=.self$uri,
         payload=payload,
       )
       update_table_properties(.self, res)
       .self
     },

     delete = function(){
       make_request(method="DELETE", path=.self$uri)
     },

     list_variables = function(max_results=NULL){
       variables <- make_paginated_request(
         path=str_interp("${.self$uri}/variables"),
         page_size=100,
         max_results=max_results
       )
       purrr::map(variables, function(variable_properties) {
         Variable$new(name=variable_properties$name, table=.self, properties=variable_properties)
       })
     },

     # add_file = function(){
     #
     # },

     # list_uploads = function(max_results){
     #   uploads <- make_paginated_request(
     #     path=str_interp("${.self$uri}/uploads"),
     #     page_size=100,
     #     max_results=max_results
     #   )
     #   purrr::map(uploads, function(upload_properties) {
     #     Upload$new(name=upload_properties$name, table=.self, properties=upload_properties)
     #   })
     # },

     variable = function(name){
       Variable$new(name=name, table=.self)
     },

     # upload = function(name){
     #   Upload$new(name=name, table=.self)
     # },

     create = function(description=NULL, upload_merge_strategy="append", is_file_index=FALSE){
       res <- make_request(
         method="POST",
         path=str_interp("${.self$dataset$uri}/tables"),
         payload=list(
           "name"=.self$name,
           "description"=description,
           "uploadMergeStrategy"=upload_merge_strategy,
           "isFileIndex"=is_file_index
         )
       )
       update_table_properties(.self, res)
       .self
     },

     to_arrow_dataset = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()){
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'arrow_dataset',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )

     },

     to_arrow_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'arrow_table',
         progress=progress,
         variables = params$variables,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )
     },

     to_arrow_batch_reader = function(max_results=NULL, variables=NULL, progress=TRUE, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'arrow_stream',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )
     },

     to_tibble = function(max_results=NULL, variables=NULL, geography_variable='', progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables, geography_variable)

       if (!is.null(params$geography_variable)){
         warning('Returning sf tibbles via the to_tibble method is deprecated, and will be removed soon. Please use table$to_sf_tibble() instead.', immediate. = TRUE)
       }

       df <- make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'tibble',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )

       if (!is.null(params$geography_variable)){
         sf::st_as_sf(df, wkt=params$geography_variable, crs=4326)
       } else {
         df
       }
     },

     to_sf_tibble = function(max_results=NULL, variables=NULL, geography_variable='', progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables, geography_variable)

       if (is.null(params$geography_variable)){
         stop('Unable to find geography variable in table')
       }

       df <- make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'tibble',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )

       sf::st_as_sf(df, wkt=params$geography_variable, crs=4326)
     },

     to_data_frame = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'data_frame',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )
     },

     to_data_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
       params <- get_table_request_params(.self, max_results, variables)

       make_rows_request(
         uri=params$uri,
         table=.self,
         max_results=params$max_results,
         selected_variable_names = params$selected_variable_names,
         type = 'data_table',
         variables = params$variables,
         progress = progress,
         coerce_schema = params$coerce_schema,
         batch_preprocessor = batch_preprocessor,
         use_export_api=params$use_export_api,
         max_parallelization=max_parallelization
       )
     },

     list_files = function(max_results = NULL, file_id_variable = NULL){
       if (is.null(file_id_variable)){
         file_id_variables <- make_paginated_request(path=str_interp("${.self$uri}/variables"), max_results=2, query = list(isFileId = TRUE))

         if (length(file_id_variables) == 0){
           stop("No variable containing file ids was found on this table")
         } else if (length(file_id_variables) > 1){
           stop("This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter.")
         }
         file_id_variable = file_id_variables[[1]]$'name'
       }

       df <- make_rows_request(
         uri=.self$uri,
         max_results=max_results,
         selected_variable_names = list(file_id_variable),
         type='data_table',
         variables=list(list(name=file_id_variable, type="string")),
         progress=FALSE
       )
       purrr::map(df[[file_id_variable]], function(id) {
         File$new(id=id)
       })
     },

     download = function(path = NULL, format = 'csv', overwrite = FALSE, progress = TRUE, max_parallelization=parallelly::availableCores()) {
       res <- make_request(
         method = "POST",
         path = paste0(.self$uri, "/exports"),
         payload = list(format = format)
       )
       export_job <- Export$new(table = .self, properties = res)

       res <- export_job$download_files(path = path, overwrite = overwrite, progress=progress, max_parallelization=max_parallelization)

       return(res)
     },

     download_files = function(path = getwd(), overwrite = FALSE, max_results = NULL, file_id_variable = NULL, progress=TRUE, max_parallelization=100){
        if (endsWith(path, '/')) {
          path <- stringr::str_sub(path,1,nchar(path)-1) # remove trailing "/", as this screws up file.path()
        }

        if (is.null(file_id_variable)){
          file_id_variables <- make_paginated_request(path=str_interp("${.self$uri}/variables"), max_results=2, query = list(isFileId = TRUE))

          if (length(file_id_variables) == 0){
            stop("No variable containing file ids was found on this table")
          } else if (length(file_id_variables) > 1){
            stop("This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter.")
          }
          file_id_variable = file_id_variables[[1]]$'name'
        }

        df <- make_rows_request(
          uri=.self$uri,
          max_results=max_results,
          selected_variable_names = list(file_id_variable),
          type='data_table',
          variables=list(list(name=file_id_variable, type="string")),
          progress=FALSE
        )

        if (!dir.exists(path)) dir.create(path, recursive = TRUE)

        if (progress){
          progressr::with_progress(perform_table_parallel_file_download(df[[file_id_variable]], path, overwrite, max_parallelization))
        } else {
          perform_table_parallel_file_download(df[[file_id_variable]], path, overwrite, max_parallelization)
        }
      }
   )
)

perform_table_parallel_file_download <- function(vec, path, overwrite, max_parallelization){
  pb <- progressr::progressor(steps = length(vec))
  download_paths <- list()
  get_download_path_from_headers <- function(headers){
    name <- get_filename_from_content_disposition(headers$'content-disposition')
    file_path <- base::file.path(path, name)
    download_paths <<- append(download_paths, file_path)
    return(file_path)
  }
  perform_parallel_download(
    purrr::map(vec, function(id){str_interp("/rawFiles/${id}?allowRedirect=true")}),
    overwrite=overwrite,
    get_download_path_from_headers=get_download_path_from_headers,
    on_finish=function(){pb(1)},
    max_parallelization
  );
  return(download_paths)
}


update_table_properties <- function(instance, properties){
  instance$properties = properties
  instance$qualified_reference = properties$qualifiedReference
  instance$scoped_reference = properties$scopedReference
  instance$name = properties$name
  instance$uri = properties$uri
}


get_table_request_params = function(self, max_results, variables, geography_variable=NULL){
  all_variables <- make_paginated_request(path=str_interp("${self$uri}/variables"), page_size=1000)

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

  if (is.null(self$properties$container)){
    self$get()
  }

  selected_variable_names <- if (is.null(variables)) NULL else Map(function(variable_name) variable_name, variables)

  if (!is.null(geography_variable) && geography_variable == ''){
    geography_variable = NULL
    for (variable in variables_list){
      if (variable$type == 'geography'){
        geography_variable <- variable$name
        break
      }
    }
  }

  max_streaming_bytes <- if (is.na(Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID", unset = NA))) {
    1e9
  } else {
    1e11
  }

  list(
    "max_results" = max_results,
    "uri" = self$uri,
    "selected_variable_names" = selected_variable_names,
    "variables"=variables_list,
    "geography_variable"=geography_variable,
    "coerce_schema"=self$properties$container$kind == 'dataset',
    "use_export_api"=self$properties$numBytes > max_streaming_bytes
  )
}
