#' @include Dataset.R Workflow.R api_request.R
Query <- setRefClass("Query",
  fields = list(query="character", default_dataset="character", default_workflow="character", properties="list", uri="character"),
  methods = list(
    show = function(){
      print(str_interp("<Query ${.self$properties$id}>"))
    },
    initialize = function(query, default_dataset=NULL, default_workflow=NULL){
      payload <- list(query=query)

      if (!is.null(default_workflow) && default_workflow != ""){
        payload$defaultWorkflow <- default_workflow
      }
      if (!is.null(default_dataset) && default_dataset != "" ){
        payload$defaultDataset <- default_dataset
      }

      .self$properties <- make_request(method='POST', path="/queries", payload=payload)
      .self$uri <- .self$properties$uri
    },

    variable = function(name){
      query_wait_for_finish(.self$properties)
      Variable$new(name=name, query=.self)
    },

    list_variables = function(max_results=NULL){
      query_wait_for_finish(.self$properties)
      variables <- make_paginated_request(
        path=str_interp("${.self$uri}/variables"),
        page_size=100,
        max_results=max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(name=variable_properties$name, query=.self, properties=variable_properties)
      })
    },

    list_files = function(max_results = NULL, file_id_variable = NULL){
      query_wait_for_finish(.self$properties)

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
        File$new(id=id, query=.self)
      })
    },

    download_files = function(path = getwd(), overwrite = FALSE, max_results = NULL, file_id_variable = NULL, progress=TRUE, max_parallelization=NULL){
        parallel_download_raw_files(
          instance=.self,
          path=path,
          overwrite=overwrite,
          max_results=max_results,
          file_id_variable=file_id_variable,
          progress=progress,
          max_parallelization=max_parallelization
        )
    },

    to_arrow_dataset = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()){
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'arrow_dataset',
        variables = params$variables,
        progress = progress,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )
    },

    to_arrow_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'arrow_table',
        variables = params$variables,
        progress=progress,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )
    },

    to_arrow_batch_reader = function(max_results=NULL, variables=NULL, progress=TRUE) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'arrow_stream',
        progress=progress,
        variables = params$variables,
        progress = progress,
        coerce_schema = params$coerce_schema
      )
    },

    to_tibble = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
      params <- get_query_request_params(.self, max_results, variables)

      df <- make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'tibble',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )

      df
    },

    to_sf_tibble = function(max_results=NULL, variables=NULL, geography_variable='', progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
      if (!requireNamespace("sf", quietly = TRUE)) {
        stop("The sf package must be installed to use the to_sf_tibble() method.")
      }

      params <- get_query_request_params(.self, max_results, variables, geography_variable)

      if (is.null(params$geography_variable)){
        stop('Unable to find geography variable in table')
      }

      df <- make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'tibble',
        variables = params$variables,
        progress=progress,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )

      sf::st_as_sf(df, wkt=params$geography_variable, crs=4326)
    },

    to_data_frame = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'data_frame',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )
    },

    to_data_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL, max_parallelization=parallelly::availableCores()) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'data_table',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor,
        max_parallelization = max_parallelization
      )
    }
  )
)

get_query_request_params = function(instance, max_results, variables, geography_variable = NULL){
  res <- query_wait_for_finish(instance$properties)
  instance$properties = res

  uri <- str_interp("/queries/${res$id}")
  all_variables <- res$outputSchema

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
    variables_list <- Filter(Negate(is.null), variables_list)
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

  list(
    "max_results" = max_results,
    "uri" = uri,
    "selected_variable_names" = selected_variable_names,
    "variables" = variables_list,
    "geography_variable"=geography_variable,
    "coerce_schema"=FALSE
  )
}

query_wait_for_finish <- function(previous_response, count = 0){
  if (previous_response$status == 'running' || previous_response$status == 'queued'){
    Sys.sleep(1)
    count = count + 1
    res <- make_request(method='GET', path=str_interp("/queries/${previous_response$id}"))
    query_wait_for_finish(res, count)
  } else {
    previous_response
  }
}
