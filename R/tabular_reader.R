tabular_reader_methods <- list(
  to_arrow_dataset = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'arrow_dataset',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )
  },

  to_arrow_table = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'arrow_table',
      progress = progress,
      variables = params$variables,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )
  },

  to_arrow_batch_reader = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'arrow_stream',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      use_export_api = params$use_export_api
    )
  },

  to_tibble = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    df <- make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'tibble',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )

    df
  },

  to_sf_tibble = function(
    max_results = NULL,
    variables = NULL,
    geography_variable = '',
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    if (!requireNamespace("sf", quietly = TRUE)) {
      stop("The sf package must be installed to use the to_sf_tibble() method.")
    }

    params <- get_table_request_params(
      .self,
      max_results,
      variables,
      geography_variable
    )

    if (is.null(params$geography_variable)) {
      stop('Unable to find geography variable in table')
    }

    df <- make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'tibble',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )

    sf::st_as_sf(df, wkt = params$geography_variable, crs = 4326)
  },

  to_data_frame = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'data_frame',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )
  },

  to_data_table = function(
    max_results = NULL,
    variables = NULL,
    progress = TRUE,
    batch_preprocessor = NULL,
    max_parallelization = parallelly::availableCores()
  ) {
    params <- get_table_request_params(.self, max_results, variables)

    make_rows_request(
      uri = params$uri,
      table = .self,
      max_results = params$max_results,
      selected_variable_names = params$selected_variable_names,
      type = 'data_table',
      variables = params$variables,
      progress = progress,
      coerce_schema = params$coerce_schema,
      batch_preprocessor = batch_preprocessor,
      use_export_api = params$use_export_api,
      max_parallelization = max_parallelization
    )
  },

  list_files = function(max_results = NULL, file_id_variable = NULL) {
    if (is.null(file_id_variable)) {
      file_id_variables <- make_paginated_request(
        path = str_interp("${.self$uri}/variables"),
        max_results = 2,
        query = list(isFileId = TRUE)
      )

      if (length(file_id_variables) == 0) {
        stop("No variable containing file ids was found on this table")
      } else if (length(file_id_variables) > 1) {
        stop(
          "This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter."
        )
      }
      file_id_variable = file_id_variables[[1]]$'name'
    }

    df <- make_rows_request(
      uri = .self$uri,
      max_results = max_results,
      selected_variable_names = list(file_id_variable),
      type = 'data_table',
      variables = list(list(name = file_id_variable, type = "string")),
      progress = FALSE
    )
    purrr::map(df[[file_id_variable]], function(id) {
      File$new(id = id, table = .self)
    })
  },
  download_files = function(
    path = getwd(),
    overwrite = FALSE,
    max_results = NULL,
    file_id_variable = NULL,
    progress = TRUE,
    max_parallelization = NULL
  ) {
    parallel_download_raw_files(
      instance = .self,
      path = path,
      overwrite = overwrite,
      max_results = max_results,
      file_id_variable = file_id_variable,
      progress = progress,
      max_parallelization = max_parallelization
    )
  }
)

get_table_request_params = function(
  instance,
  max_results,
  variables,
  geography_variable = NULL
) {
  # IMPORTANT: note that this is also called be the upload$to_* methods
  all_variables <- make_paginated_request(
    path = str_interp("${instance$uri}/variables"),
    page_size = 1000
  )

  if (is.null(variables)) {
    variables_list <- all_variables
  } else {
    variables = as.list(variables)
    lower_variable_names <- Map(function(variable) tolower(variable), variables)
    variables_list <- Filter(
      function(variable) tolower(variable$name) %in% lower_variable_names,
      all_variables
    )
    variables_list <- sapply(
      lower_variable_names,
      function(name) {
        all_variables[match(
          name,
          sapply(all_variables, function(variable) tolower(variable$name))
        )][1]
      }
    )
    variables_list <- Filter(Negate(is.null), variables_list)
  }

  if (is.null(instance$properties$container)) {
    instance$get()
  }

  selected_variable_names <- if (is.null(variables)) {
    NULL
  } else {
    Map(function(variable_name) variable_name, variables)
  }

  if (!is.null(geography_variable) && geography_variable == '') {
    geography_variable = NULL
    for (variable in variables_list) {
      if (variable$type == 'geography') {
        geography_variable <- variable$name
        break
      }
    }
  }

  max_streaming_bytes <- if (
    is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset = NA))
  ) {
    1e9
  } else {
    1e11
  }

  list(
    "max_results" = max_results,
    "uri" = instance$uri,
    "selected_variable_names" = selected_variable_names,
    "variables" = variables_list,
    "geography_variable" = geography_variable,
    "coerce_schema" = instance$properties$container$kind == 'dataset',
    "use_export_api" = instance$properties$numBytes > max_streaming_bytes
  )
}
