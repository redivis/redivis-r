#' @include Dataset.R Workflow.R Variable.R Export.R util.R api_request.R TabularReader.R
Table <- R6::R6Class(
  "Table",
  inherit = TabularReader,
  public = list(
    name = NULL,
    dataset = NULL,
    workflow = NULL,
    qualified_reference = NULL,
    scoped_reference = NULL,

    initialize = function(
      name = "",
      dataset = NULL,
      workflow = NULL,
      properties = list()
    ) {
      super$initialize()

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
        } else if (name != "") {
          abort_redivis_value_error(
            "Invalid table specifier, must be the fully qualified reference if no dataset or workflow is specified"
          )
        }
      }
      self$name <- parsed_name
      self$dataset <- parsed_dataset
      self$workflow <- parsed_workflow

      self$scoped_reference <- if (length(properties$scopedReference)) {
        properties$scopedReference
      } else {
        parsed_name
      }
      self$qualified_reference <- if (length(properties$qualifiedReference)) {
        properties$qualifiedReference
      } else {
        str_interp("${parent_reference}${parsed_name}")
      }
      self$uri <- str_interp("/tables/${URLencode(self$qualified_reference)}")
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp("<Table ${self$qualified_reference}>\n"))
      invisible(self)
    },

    get = function() {
      res <- make_request(path = self$uri)
      update_table_properties(self, res)
      self
    },

    exists = function() {
      tryCatch(
        {
          make_request(
            method = "HEAD",
            path = self$uri
          )
          TRUE
        },
        redivis_not_found_error = function(e) {
          FALSE
        }
      )
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
        path = self$uri,
        payload = payload,
      )
      update_table_properties(self, res)
      self
    },

    update_variables = function(variables) {
      make_request(
        method = "PATCH",
        path = str_interp("${self$uri}/variables"),
        payload = list(
          "variables" = variables
        ),
      )
      self
    },

    delete = function() {
      make_request(method = "DELETE", path = self$uri)
      invisible(NULL)
    },

    list_variables = function(max_results = NULL) {
      variables <- make_paginated_request(
        path = str_interp("${self$uri}/variables"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(
          name = variable_properties$name,
          table = self,
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
      dir <- directory # Rename to avoid confusion with self$directory field
      max_parallelization <- min(20, max_parallelization)
      add_table_files <- function() {
        # Ensure exactly one of files or directory is provided
        if (
          (is.null(files) && is.null(dir)) ||
            (!is.null(files) && !is.null(dir))
        ) {
          abort_redivis_value_error(
            "Either files or directory must be specified"
          )
        }

        total_size <- 0

        if (!is.null(dir)) {
          dir <- normalizePath(dir)
          files_list <- list()
          all_files <- list.files(
            path = dir,
            recursive = TRUE,
            full.names = TRUE,
            include.dirs = FALSE
          )
          files_list <- lapply(all_files, function(filename) {
            size <- file.info(filename)$size
            rel_path <- sub(paste0("^", str_interp("${dir}/")), "", filename)
            list(path = filename, name = rel_path, size = size)
          })
          total_size <- sum(vapply(files_list, function(f) f$size, numeric(1)))
          files <- files_list
        } else {
          files <- lapply(files, map_file)
          for (file in files) {
            total_size <- total_size + file$size
          }
        }

        pb_bytes_multiplier <- 100 / total_size
        pb_bytes <- progressr::progressor(steps = 100)

        current_batch_timestamp <- as.numeric(Sys.time())
        uploaded_files <- list()
        current_batch_files <- list()
        current_temp_uploads_batch <- list()
        target_batch_size <- 1e8
        current_batch_size <- 0
        progress_count <- 0

        for (i in seq_along(files)) {
          file <- files[[i]]

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
              path = paste0(self$uri, "/tempUploads"),
              payload = payload
            )
            current_temp_uploads_batch <- res$results
          }

          batch_index <- ((i - 1) %% 1000) + 1
          current_batch_files[[length(current_batch_files) + 1]] <- list(
            file = file,
            temp_upload = current_temp_uploads_batch[[batch_index]]
          )
          current_batch_size <- current_batch_size + file$size

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

            elapsed <- as.numeric(Sys.time()) - current_batch_timestamp
            if (elapsed > 60) {
              target_batch_size <- target_batch_size / 2
            } else if (elapsed < 15) {
              target_batch_size <- target_batch_size * 2
            }

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
              path = paste0(self$uri, "/rawFiles"),
              payload = payload
            )

            new_files <- lapply(response$results, function(f) {
              File$new(id = f$id, table = self, properties = f)
            })
            uploaded_files <- c(uploaded_files, new_files)

            current_batch_timestamp <- as.numeric(Sys.time())
            current_batch_files <- list()
            current_batch_size <- 0
          }
        }

        uploaded_files
      }
      if (progress) {
        progressr::with_progress(add_table_files())
      } else {
        add_table_files()
      }
    },

    list_uploads = function(max_results = NULL) {
      uploads <- make_paginated_request(
        path = str_interp("${self$uri}/uploads"),
        page_size = 100,
        max_results = max_results
      )
      purrr::map(uploads, function(upload_properties) {
        Upload$new(
          name = upload_properties$name,
          table = self,
          properties = upload_properties
        )
      })
    },

    variable = function(name) {
      Variable$new(name = name, table = self)
    },

    upload = function(name = "") {
      Upload$new(name = name, table = self)
    },

    create = function(
      description = NULL,
      upload_merge_strategy = "append",
      is_file_index = FALSE
    ) {
      rectify_ambiguous_table_container(self)
      payload = list(
        "name" = self$name,
        "uploadMergeStrategy" = upload_merge_strategy,
        "isFileIndex" = is_file_index
      )
      if (!is.null(description)) {
        payload$description <- description
      }
      res <- make_request(
        method = "POST",
        path = str_interp("${self$dataset$uri}/tables"),
        payload = payload
      )
      update_table_properties(self, res)
      self
    },

    download = function(
      path = NULL,
      format = 'csv',
      overwrite = FALSE,
      progress = TRUE,
      max_parallelization = NULL
    ) {
      res <- make_request(
        method = "POST",
        path = paste0(self$uri, "/exports"),
        payload = list(format = format)
      )
      export_job <- Export$new(table = self, properties = res)

      res <- export_job$download_files(
        path = path,
        overwrite = overwrite,
        progress = progress,
        max_parallelization = max_parallelization
      )

      return(res)
    }
  )
)

rectify_ambiguous_table_container <- function(table) {
  if (!is.null(table$dataset) && !is.null(table$workflow)) {
    if (!is.null(table$properties[['container']])) {
      if (table$properties[['container']][['kind']] == 'dataset') {
        table$workflow <- NULL
      } else {
        table$dataset <- NULL
      }
    } else if (table$dataset$exists()) {
      table$workflow <- NULL
    } else {
      table$dataset <- NULL
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
  }

  on.exit(future::plan(oplan), add = TRUE)

  local_perform_resumable_upload <- perform_resumable_upload
  local_perform_standard_upload <- perform_standard_upload

  results <- furrr::future_map(batch_files, function(batch_file) {
    file_obj <- batch_file[["file"]]
    temp_upload <- batch_file[["temp_upload"]]

    if ("path" %in% names(file_obj)) {
      data <- base::file(file_obj$path, "rb")
      on.exit(close(data), add = TRUE)
    } else {
      data <- file_obj$data
    }

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
  instance$properties <- properties
  instance$qualified_reference <- properties$qualifiedReference
  instance$scoped_reference <- properties$scopedReference
  instance$name <- properties$name
  instance$uri <- properties$uri
  rectify_ambiguous_table_container(instance)
}

map_file <- function(file) {
  if (is.character(file)) {
    file <- list(path = file)
  } else {
    file <- as.list(file)
  }

  if (!("name" %in% names(file))) {
    if ("data" %in% names(file)) {
      abort_redivis_value_error(
        'All file specifications with a "data" key must specify a name'
      )
    }
    file$name <- basename(file$path)
  }

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
