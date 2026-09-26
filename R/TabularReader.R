# TODO: can this be some sort of weakRef, like in python?
cached_directories <- new.env(parent = emptyenv())

#' @include read_rows.R Directory.R api_request.R
TabularReader <- R6::R6Class(
  "TabularReader",
  public = list(
    uri = NULL,
    properties = list(),
    directory = NULL,

    initialize = function(uri = NULL, properties = list()) {
      self$uri <- uri
      self$properties <- properties
      self$directory <- NULL
    },

    to_directory = function(
      file_id_variable = NULL,
      file_name_variable = NULL
    ) {
      if (inherits(self, "Upload")) {
        abort_redivis_value_error(
          "Listing files on uploads is not currently supported."
        )
      }
      if (inherits(self, "ReadStream")) {
        abort_redivis_value_error(
          "Listing files on ReadStreams is not supported."
        )
      }

      is_query <- inherits(self, "Query")
      is_table <- inherits(self, "Table")

      # We need to check that the query has finished.
      # However, for tables, we don't need to fetch the table metadata before building the directory
      #   and it's important that we don't, since the cached_directories map is based on the user-provided URI
      # TODO: in the future, we should map all URIs to a canonical URI for caching
      if (is_query) {
        check_is_ready(self)
      }

      if (is.null(self$directory) && !is.null(cached_directories[[self$uri]])) {
        self$directory <- cached_directories[[self$uri]]
      }

      # Queries are immutable — return the cached directory if available
      if (is_query && !is.null(self$directory)) {
        return(self$directory)
      }

      # Capture current time in ms before the request is sent
      current_timestamp_ms <- as.numeric(Sys.time()) * 1000

      req_headers <- list()
      if (!is.null(self$directory$last_cached_at)) {
        req_headers[["If-Modified-Since"]] <- format(
          as.POSIXct(
            self$directory$last_cached_at / 1000,
            origin = "1970-01-01",
            tz = "GMT"
          ),
          "%a, %d %b %Y %H:%M:%S GMT"
        )
      }

      query_params <- list(format = "arrow")
      if (!is.null(file_id_variable)) {
        query_params$fileIdVariable <- file_id_variable
      }
      if (!is.null(file_name_variable)) {
        query_params$fileNameVariable <- file_name_variable
      }

      name_variable <- if (!is.null(file_name_variable)) {
        file_name_variable
      } else {
        "file_name"
      }

      res <- make_request(
        method = "GET",
        path = str_interp("${self$uri}/rawFiles"),
        headers = req_headers,
        query = query_params,
        parse_response = FALSE
      )

      start_time <- Sys.time()

      # 304 Not Modified — return cached directory
      if (httr2::resp_status(res) == 304L) {
        self$directory$last_cached_at <- current_timestamp_ms
        return(self$directory)
      }

      dir <- Directory$new(
        path = "/",
        query = if (is_query) self else NULL,
        table = if (is_table) self else NULL
      )

      raw_bytes <- httr2::resp_body_raw(res)
      file_specs <- arrow::read_ipc_stream(raw_bytes, as_data_frame = TRUE)
      # IMPORTANT: need to explicitly set int64 as numeric to avoid purrr::transpose casting to an int32, which causes overflow issues when > 2^31-1
      int64_cols <- vapply(file_specs, inherits, logical(1), "integer64")
      file_specs[int64_cols] <- lapply(file_specs[int64_cols], as.numeric)

      # Convert to a list of rows upfront; each element is a named list of scalars
      row_list <- purrr::transpose(as.list(file_specs))

      for (file_spec in row_list) {
        f <- File$new(
          id = file_spec[["file_id"]],
          name = file_spec[[name_variable]],
          table = if (is_table) self else NULL,
          query = if (is_query) self else NULL,
          properties = file_spec
        )
        add_directory_file(dir, f)
      }

      self$directory <- dir
      self$directory$last_cached_at <- current_timestamp_ms
      cached_directories[[self$uri]] <- dir
      dir
    },

    download = function(
      path = NULL,
      format = 'csv',
      overwrite = FALSE,
      progress = TRUE,
      max_parallelization = NULL,
      max_concurrency = NULL
    ) {
      if (inherits(self, "ReadStream")) {
        abort_redivis_value_error("Cannot call $download() on a ReadStream.")
      }
      check_is_ready(self)
      res <- make_request(
        method = "POST",
        path = paste0(self$uri, "/exports"),
        payload = list(format = format)
      )
      export_job <- Export$new(
        table = if (inherits(self, "Table")) self,
        query = if (inherits(self, "Query")) self,
        upload = if (inherits(self, "Upload")) self,
        properties = res
      )

      export_job$download_files(
        path = path,
        overwrite = overwrite,
        progress = progress,
        max_parallelization = max_parallelization,
        max_concurrency = max_concurrency
      )
    },

    to_arrow_dataset = function(
      max_results = NULL,
      variables = NULL,
      progress = TRUE,
      batch_preprocessor = NULL,
      max_parallelization = parallelly::availableCores()
    ) {
      params <- get_table_request_params(self, max_results, variables)

      make_rows_request(
        uri = params$uri,
        instance = self,
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
      params <- get_table_request_params(self, max_results, variables)

      make_rows_request(
        uri = params$uri,
        instance = self,
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
      params <- get_table_request_params(self, max_results, variables)

      make_rows_request(
        uri = params$uri,
        instance = self,
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
      params <- get_table_request_params(self, max_results, variables)

      df <- make_rows_request(
        uri = params$uri,
        instance = self,
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
        abort_redivis_error(
          "The sf package must be installed to use the to_sf_tibble() method."
        )
      }
      params <- get_table_request_params(
        self,
        max_results,
        variables,
        geography_variable
      )

      if (is.null(params$geography_variable)) {
        abort_redivis_not_found_error(
          description = 'Unable to find geography variable in table'
        )
      }

      df <- make_rows_request(
        uri = params$uri,
        instance = self,
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
      params <- get_table_request_params(self, max_results, variables)

      make_rows_request(
        uri = params$uri,
        instance = self,
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
      params <- get_table_request_params(self, max_results, variables)

      make_rows_request(
        uri = params$uri,
        instance = self,
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

    to_read_streams = function(
      target_count = parallelly::availableCores(),
      variables = NULL
    ) {
      if (inherits(self, "ReadStream")) {
        abort_redivis_value_error(
          "Cannot call $to_read_stream() on a ReadStream."
        )
      }
      params <- get_table_request_params(self, variables = variables)
      payload <- list(
        "requestedStreamCount" = target_count,
        format = "arrow"
      )

      if (!is.null(params$selected_variable_names)) {
        payload$selectedVariables <- params$selected_variable_names
      }

      read_session <- make_request(
        method = "post",
        path = str_interp("${self$uri}/readSessions"),
        parse_response = TRUE,
        payload = payload
      )
      read_streams <- lapply(
        read_session$streams,
        function(stream) {
          ReadStream$new(
            id = stream$id,
            table = if (inherits(self, "Table")) self else NULL,
            query = if (inherits(self, "Query")) self else NULL,
            upload = if (inherits(self, "Upload")) self else NULL,
            selected_variable_names = params$selected_variable_names,
            properties = stream
          )
        }
      )
      read_streams
    },

    file = function(path) {
      if (is.null(self$directory)) {
        self$directory <- cached_directories[[self$uri]]
      }
      cache_age_ms <- if (!is.null(self$directory$last_cached_at)) {
        as.numeric(Sys.time()) * 1000 - self$directory$last_cached_at
      } else {
        Inf
      }

      if (is.null(self$directory) || cache_age_ms >= 30 * 1000) {
        self$to_directory()
      }

      node <- self$directory$get(path)
      if (inherits(node, "Directory")) {
        abort_redivis_value_error(str_interp(
          "'${path}' is a directory, not a file"
        ))
      }
      if (is.null(node)) {
        abort_redivis_not_found_error(
          description = str_interp(
            "No file found at path '${path}'"
          )
        )
      }
      node
    },

    list_files = function(
      max_results = NULL,
      file_id_variable = NULL,
      file_name_variable = NULL
    ) {
      self$to_directory(
        file_id_variable = file_id_variable,
        file_name_variable = file_name_variable
      )
      self$directory$list(
        mode = "files",
        recursive = TRUE,
        max_results = max_results
      )
    },

    download_files = function(
      path = getwd(),
      overwrite = FALSE,
      max_results = NULL,
      file_id_variable = NULL,
      progress = TRUE,
      max_parallelization = NULL
    ) {
      self$to_directory(file_id_variable = file_id_variable)
      warning(
        "This method is deprecated. Please use to_directory()$download() instead",
        call. = FALSE
      )
      self$directory$download(
        path = path,
        overwrite = overwrite,
        max_results = max_results,
        progress = progress,
        max_parallelization = max_parallelization
      )
    }
  )
)

check_is_ready <- function(instance) {
  if (inherits(instance, "Query")) {
    query_wait_for_finish(instance)
  } else if (
    inherits(instance, "Table") && is.null(instance$properties$container)
  ) {
    instance$get()
  } else if (inherits(instance, "Upload")) {
    if (
      is.null(instance$properties$status) ||
        !identical(instance$properties$status, "completed")
    ) {
      instance$get()
    }
    if (!identical(instance$properties$status, "completed")) {
      abort_redivis_value_error(str_interp(
        "Cannot read data from an upload with status: ${instance$properties$status}"
      ))
    }
  }
}

get_table_request_params <- function(
  instance,
  max_results = NULL,
  variables = NULL,
  geography_variable = NULL,
  refresh = TRUE
) {
  if (inherits(instance, "ReadStream")) {
    res <- get_table_request_params(
      if (!is.null(instance$table)) {
        instance$table
      } else if (!is.null(instance$upload)) {
        instance$upload
      } else {
        instance$query
      },
      max_results = max_results,
      variables = instance$selected_variable_names,
      geography_variable = geography_variable,
      # The parent was fetched when the read session was created, and
      # refetching it for every stream would multiply requests by the stream
      # count
      refresh = FALSE
    )
    res$use_export_api <- FALSE
    return(res)
  }

  if (inherits(instance, "Table") && refresh) {
    # Refetch rather than trusting properties cached on this object by an
    # earlier call, since the read depends on them being current: e.g. a
    # listRows read verifies completion against numRows, which changes when the
    # table is written to. For a newly constructed table, this is the same
    # fetch check_is_ready() would have made, so it costs nothing extra.
    instance$get()
  } else {
    check_is_ready(instance)
  }

  # TODO: do we need all this variable fetching complexity still? Can we just handle everything on the BE?
  all_variables <- make_paginated_request(
    path = str_interp("${instance$uri}/variables"),
    page_size = 1000
  )

  if (is.null(variables)) {
    variables_list <- all_variables
  } else {
    variables <- as.list(variables)
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

  selected_variable_names <- if (is.null(variables)) {
    NULL
  } else {
    Map(function(variable_name) variable_name, variables)
  }

  if (!is.null(geography_variable) && geography_variable == '') {
    geography_variable <- NULL
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
  num_bytes <- if (inherits(instance, "Query")) {
    instance$properties$outputNumBytes
  } else {
    instance$properties$numBytes
  }
  should_use_export_api <- isTRUE(as.numeric(num_bytes) > max_streaming_bytes)

  list(
    "max_results" = max_results,
    "uri" = instance$uri,
    "selected_variable_names" = selected_variable_names,
    "variables" = variables_list,
    "geography_variable" = geography_variable,
    "coerce_schema" = inherits(instance, "Table") &&
      instance$properties$container$kind == 'dataset',
    "use_export_api" = should_use_export_api
  )
}
