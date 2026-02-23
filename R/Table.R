#' @include Dataset.R Workflow.R Variable.R Export.R util.R api_request.R
Table <- setRefClass(
  "Table",
  fields = list(
    name = "character",
    dataset = "ANY",
    workflow = "ANY",
    properties = "list",
    qualified_reference = "character",
    scoped_reference = "character",
    uri = "character"
  ),

  methods = c(
    tabular_reader_methods,
    initialize = function(
      ...,
      name = "",
      dataset = NULL,
      workflow = NULL,
      properties = list()
    ) {
      parent_reference <- ""
      parsed_name <- name
      parsed_dataset <- dataset
      parsed_workflow <- workflow
      if (!is.null(dataset)) {
        parent_reference <- str_interp("${dataset$qualified_reference}.")
      } else if (!is.null(workflow)) {
        parent_reference <- str_interp("${workflow$qualified_reference}.")
      } else {
        split <- strsplit(name, "\\.")[[1]]
        if (length(split) == 3) {
          parsed_name <- split[[3]]
          parent_reference <- paste(split[1:2], collapse = '.')
          parsed_dataset <- Dataset$new(name = parent_reference)
          parsed_workflow <- Workflow$new(name = parent_reference)
          parent_reference <- str_interp("${parent_reference}.")
        } else if (Sys.getenv("REDIVIS_DEFAULT_WORKFLOW") != "") {
          parsed_workflow <- Workflow$new(
            name = Sys.getenv("REDIVIS_DEFAULT_WORKFLOW")
          )
        } else if (Sys.getenv("REDIVIS_DEFAULT_DATASET") != "") {
          parsed_dataset <- Dataset$new(
            name = Sys.getenv("REDIVIS_DEFAULT_DATASET")
          )
        } else if (
          name != "" # Need this check otherwise package won't build (?)
        ) {
          stop(
            "Invalid table specifier, must be the fully qualified reference if no dataset or workflow is specified"
          )
        }
      }
      scoped_reference_val <- if (length(properties$scopedReference)) {
        properties$scopedReference
      } else {
        parsed_name
      }
      qualified_reference_val <- if (length(properties$qualifiedReference)) {
        properties$qualifiedReference
      } else {
        str_interp("${parent_reference}${parsed_name}")
      }
      callSuper(
        ...,
        name = parsed_name,
        dataset = parsed_dataset,
        workflow = parsed_workflow,
        qualified_reference = qualified_reference_val,
        scoped_reference = scoped_reference_val,
        uri = str_interp("/tables/${URLencode(qualified_reference_val)}"),
        properties = properties
      )
    },

    show = function() {
      print(str_interp("<Table ${.self$qualified_reference}>"))
    },

    get = function() {
      res <- make_request(path = .self$uri)
      update_table_properties(.self, res)
      .self
    },

    exists = function() {
      res <- make_request(
        method = "HEAD",
        path = .self$uri,
        stop_on_error = FALSE
      )
      if (length(res$error)) {
        if (res$status == 404) {
          return(FALSE)
        } else {
          stop(str_interp("${res$error}: ${res$error_description}"))
        }
      } else {
        return(TRUE)
      }
    },

    update = function(
      name = NULL,
      description = NULL,
      upload_merge_strategy = NULL
    ) {
      payload <- list()
      if (!is.null(name)) {
        payload <- append(payload, list("name" = name))
      }
      if (!is.null(description)) {
        payload <- append(payload, list("description" = description))
      }
      if (!is.null(upload_merge_strategy)) {
        payload <- append(
          payload,
          list("uploadMergeStrategy" = upload_merge_strategy)
        )
      }
      res <- make_request(
        method = "PATCH",
        path = .self$uri,
        payload = payload,
      )
      update_table_properties(.self, res)
      .self
    },

    delete = function() {
      make_request(method = "DELETE", path = .self$uri)
      invisible(NULL)
    },

    list_variables = function(max_results = NULL) {
      variables <- make_paginated_request(
        path = str_interp("${.self$uri}/variables"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(
          name = variable_properties$name,
          table = .self,
          properties = variable_properties
        )
      })
    },

    add_files = function(
      files = NULL,
      directory = NULL,
      progress = TRUE,
      max_parallelization = parallelly::availableCores()
    ) {
      max_parallelization <- min(20, max_parallelization)
      add_table_files <- function() {
        # Ensure exactly one of files or directory is provided
        if (
          (is.null(files) && is.null(directory)) ||
            (!is.null(files) && !is.null(directory))
        ) {
          stop("Either files or directory must be specified")
        }

        total_size <- 0

        if (!is.null(directory)) {
          directory <- normalizePath(directory)
          files_list <- list()
          # Get all files recursively (full names)
          all_files <- list.files(
            path = directory,
            recursive = TRUE,
            full.names = TRUE,
            include.dirs = FALSE
          )
          for (filename in all_files) {
            # Check if it is a file (not a directory)
            size <- file.info(filename)$size
            total_size <- total_size + size
            # Compute the relative path by removing the directory prefix, including trailing slash
            rel_path <- sub(
              paste0("^", str_interp("${directory}/")),
              "",
              filename
            )
            files_list[[length(files_list) + 1]] <- list(
              path = filename,
              name = rel_path,
              size = size
            )
          }
          files <- files_list
        } else {
          files <- lapply(files, map_file)
          for (file in files) {
            total_size <- total_size + file$size
          }
        }

        # progressr takes up an insane amount of memory if a large number of steps are passed
        # Instead, we normalize this to 100, and then compute the steps elswhere
        pb_bytes_multiplier <- 100 / total_size

        pb_bytes <- progressr::progressor(steps = 100)

        # Initialize variables for batching uploads
        current_batch_timestamp <- as.numeric(Sys.time())
        uploaded_files <- list()
        current_batch_files <- list()
        current_temp_uploads_batch <- list()
        target_batch_size <- 1e8
        current_batch_size <- 0
        progress_count <- 0

        # Wrap in tryCatch to ensure that progress bars are closed on error
        result <- tryCatch(
          {
            for (i in seq_along(files)) {
              file <- files[[i]]

              # Every 1000 files, request a new batch of temporary uploads.
              if (((i - 1) %% 1000) == 0) {
                batch_end <- min(i + 999, length(files))
                batch <- files[i:batch_end]
                payload <- list(
                  tempUploads = lapply(batch, function(f) {
                    list(
                      size = f$size,
                      name = f$name,
                      resumable = f$size > 5e7
                    )
                  })
                )
                res <- make_request(
                  method = "POST",
                  path = paste0(.self$uri, "/tempUploads"),
                  payload = payload
                )
                current_temp_uploads_batch <- res$results
              }

              # Assign the corresponding temporary upload from the current batch.
              # Adjust index to be 1-indexed.
              batch_index <- ((i - 1) %% 1000) + 1
              current_batch_files[[length(current_batch_files) + 1]] <- list(
                file = file,
                temp_upload = current_temp_uploads_batch[[batch_index]]
              )
              current_batch_size <- current_batch_size + file$size

              # Check if the current batch should be processed:
              # - when we have 1000 files, or
              # - we are at the last file, or
              # - the batch size exceeds the target.
              if (
                length(current_batch_files) >= 1000 ||
                  i == length(files) ||
                  current_batch_size > target_batch_size
              ) {
                perform_table_parallel_file_upload(
                  batch_files = current_batch_files,
                  max_parallelization = max_parallelization,
                  pb_bytes = pb_bytes,
                  pb_bytes_multiplier = pb_bytes_multiplier
                )

                # Adjust target batch size based on elapsed time
                elapsed <- as.numeric(Sys.time()) - current_batch_timestamp
                if (elapsed > 60) {
                  target_batch_size <- target_batch_size / 2
                } else if (elapsed < 15) {
                  target_batch_size <- target_batch_size * 2
                }

                # Finalize upload by making a request with the batched file info.
                payload <- list(
                  files = lapply(current_batch_files, function(batch_file) {
                    list(
                      name = batch_file$file$name,
                      tempUploadId = batch_file$temp_upload$id
                    )
                  })
                )
                response <- make_request(
                  method = "POST",
                  path = paste0(.self$uri, "/rawFiles"),
                  payload = payload
                )

                # Assume File() is a constructor that creates a File object.
                new_files <- lapply(response$results, function(f) {
                  File$new(id = f$id, table = .self, properties = f)
                })
                uploaded_files <- c(uploaded_files, new_files)

                # Reset batch variables for the next batch.
                current_batch_timestamp <- as.numeric(Sys.time())
                current_batch_files <- list()
                current_batch_size <- 0
              }
            }

            uploaded_files
          },
          error = function(e) {
            stop(e)
          }
        )

        return(result)
      }
      if (progress) {
        progressr::with_progress(add_table_files())
      } else {
        add_table_files()
      }
    },

    list_uploads = function(max_results = NULL) {
      uploads <- make_paginated_request(
        path = str_interp("${.self$uri}/uploads"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(uploads, function(upload_properties) {
        Upload$new(
          name = upload_properties$name,
          table = .self,
          properties = upload_properties
        )
      })
    },

    variable = function(name) {
      Variable$new(name = name, table = .self)
    },

    upload = function(name = "") {
      Upload$new(name = name, table = .self)
    },

    create = function(
      description = NULL,
      upload_merge_strategy = "append",
      is_file_index = FALSE
    ) {
      rectify_ambiguous_table_container(.self)
      payload = list(
        "name" = .self$name,
        "uploadMergeStrategy" = upload_merge_strategy,
        "isFileIndex" = is_file_index
      )
      if (!is.null(description)) {
        payload$description <- description
      }
      res <- make_request(
        method = "POST",
        path = str_interp("${.self$dataset$uri}/tables"),
        payload = payload
      )
      update_table_properties(.self, res)
      .self
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

    download = function(
      path = NULL,
      format = 'csv',
      overwrite = FALSE,
      progress = TRUE
    ) {
      res <- make_request(
        method = "POST",
        path = paste0(.self$uri, "/exports"),
        payload = list(format = format)
      )
      export_job <- Export$new(table = .self, properties = res)

      res <- export_job$download_files(
        path = path,
        overwrite = overwrite,
        progress = progress
      )

      return(res)
    }
  )
)

rectify_ambiguous_table_container <- function(table) {
  if (!is.null(table$dataset) && !is.null(table$workflow)) {
    if (!is.null(table$properties[['container']])) {
      if (table$properties[['container']][['kind']] == 'dataset') {
        table$workflow = NULL
      } else {
        table$dataset = NULL
      }
    } else if (table$dataset$exists()) {
      table$workflow = NULL
    } else {
      table$dataset = NULL
    }
  }
}

perform_table_parallel_file_upload <- function(
  batch_files,
  max_parallelization,
  pb_bytes = NULL,
  pb_bytes_multiplier = 1
) {
  if (parallelly::supportsMulticore()) {
    oplan <- future::plan(future::multicore, workers = max_parallelization)
  } else {
    oplan <- future::plan(future::multisession, workers = max_parallelization)
    # Helpful for testing in dev
    # oplan <- future::plan(future::sequential)
  }

  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(future::plan(oplan), add = TRUE)

  # Need to do this to ensure the methods are available within multisession
  local_perform_resumable_upload <- perform_resumable_upload
  local_perform_standard_upload <- perform_standard_upload

  results <- furrr::future_map(batch_files, function(batch_file) {
    file_obj <- batch_file[["file"]]
    temp_upload <- batch_file[["temp_upload"]]

    # If the file specification has a "path" key, open a connection in binary mode;
    # otherwise, use the provided data.
    if ("path" %in% names(file_obj)) {
      data <- base::file(file_obj$path, "rb")
      on.exit(close(data), add = TRUE)
    } else {
      data <- file_obj$data
    }

    # Depending on whether the upload is resumable, call the appropriate function.
    if (isTRUE(temp_upload[["resumable"]])) {
      local_perform_resumable_upload(
        data = data,
        on_progress = if (is.null(pb_bytes)) {
          NULL
        } else {
          function(bytes) {
            pb_bytes(amount = bytes * pb_bytes_multiplier)
          }
        },
        temp_upload_url = temp_upload[["url"]]
      )
    } else {
      local_perform_standard_upload(
        data = data,
        temp_upload_url = temp_upload[["url"]],
      )
      if (!is.null(pb_bytes)) {
        pb_bytes(amount = file_obj$size * pb_bytes_multiplier)
      }
    }
  })
}


update_table_properties <- function(instance, properties) {
  instance$properties = properties
  instance$qualified_reference = properties$qualifiedReference
  instance$scoped_reference = properties$scopedReference
  instance$name = properties$name
  instance$uri = properties$uri
  rectify_ambiguous_table_container(instance)
}

map_file <- function(file) {
  # If file is a character string, create a list with a 'path' key.
  if (is.character(file)) {
    file <- list(path = file)
  } else {
    file <- as.list(file)
  }

  # If 'name' is not specified in the list
  if (!("name" %in% names(file))) {
    if ("data" %in% names(file)) {
      stop('All file specifications with a "data" key must specify a name')
    }
    file$name <- basename(file$path)
  }

  # If 'data' is provided, convert character data to raw and compute size.
  if ("data" %in% names(file)) {
    if (is.character(file$data)) {
      file$data <- charToRaw(file$data)
    }
    file$size <- length(file$data)
  } else {
    file$size <- file.info(file$path)$size
  }

  return(file)
}
