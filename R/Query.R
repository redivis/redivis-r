#' @include Dataset.R Project.R api_request.R
Query <- setRefClass("Query",
  fields = list(query="character", default_dataset="character", default_project="character", properties="list"),
  methods = list(
    show = function(){
      print(str_interp("<Query ${.self$properties$id}>"))
    },
    initialize = function(query, default_dataset=NULL, default_project=NULL){
      payload <- list(query=query)

      if (!is.null(default_project) && default_project != ""){
        payload$defaultProject <- default_project
      }
      if (!is.null(default_dataset) && default_dataset != "" ){
        payload$defaultDataset <- default_dataset
      }

      .self$properties <- make_request(method='POST', path="/queries", payload=payload)
    },

    to_arrow_dataset = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL){
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'arrow_dataset',
        variables = params$variables,
        progress = progress,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor
      )
    },

    to_arrow_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'arrow_table',
        variables = params$variables,
        progress=progress,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor
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

    to_tibble = function(max_results=NULL, variables=NULL, geography_variable='', progress=TRUE, batch_preprocessor=NULL) {
      params <- get_query_request_params(.self, max_results, variables, geography_variable)

      if (!is.null(params$geography_variable)){
        warning('Returning sf tibbles via the to_tibble method is deprecated, and will be removed soon. Please use table$to_sf_tibble() instead.', immediate. = TRUE)
      }

      df <- make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'tibble',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor
      )

      if (!is.null(params$geography_variable)){
        st_as_sf(df, wkt=params$geography_variable, crs=4326)
      } else {
        df
      }
    },

    to_sf_tibble = function(max_results=NULL, variables=NULL, geography_variable='', progress=TRUE, batch_preprocessor=NULL) {
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
        batch_preprocessor = batch_preprocessor
      )

      st_as_sf(df, wkt=params$geography_variable, crs=4326)
    },

    to_data_frame = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'data_frame',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor
      )
    },

    to_data_table = function(max_results=NULL, variables=NULL, progress=TRUE, batch_preprocessor=NULL) {
      params <- get_query_request_params(.self, max_results, variables)

      make_rows_request(
        uri=params$uri,
        max_results=params$max_results,
        selected_variable_names = params$selected_variable_names,
        type = 'data_table',
        progress=progress,
        variables = params$variables,
        coerce_schema = params$coerce_schema,
        batch_preprocessor = batch_preprocessor
      )
    }
  )
)

get_query_request_params = function(self, max_results, variables, geography_variable = NULL){
  res <- query_wait_for_finish(self$properties)
  self$properties = res

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
