#' @include Table.R api_request.R util.R
Export <- R6::R6Class(
  "Export",
  public = list(
    table = NULL,
    properties = NULL,
    uri = NULL,

    initialize = function(table = NULL, properties = list()) {
      self$table <- table
      self$uri <- properties$uri
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp(
        "<Export ${self$uri} on ${self$table$qualified_reference}>\n"
      ))
      invisible(self)
    },

    get = function(wait_for_statistics = FALSE) {
      self$properties <- make_request(path = self$uri)
      self$uri <- self$properties$uri
      self
    },

    download_files = function(
      path = NULL,
      overwrite = FALSE,
      max_parallelization = NULL,
      progress = TRUE
    ) {
      if (progress) {
        progressr::with_progress(self$wait_for_finish())
      } else {
        self$wait_for_finish()
      }

      file_count <- self$properties$fileCount
      is_dir <- FALSE
      if (is.null(path) || (file.exists(path) && file.info(path)$isdir)) {
        is_dir <- TRUE
        if (is.null(path)) {
          path <- getwd()
        }
        if (file_count > 1) {
          escaped_table_name <- tolower(stringr::str_replace_all(
            self$properties$table$name,
            "\\W+",
            "_"
          ))
          path <- file.path(path, escaped_table_name)
        }
      } else if (
        grepl("[/\\]$", path) || (!file.exists(path) && !grepl("\\.", path))
      ) {
        is_dir <- TRUE
      } else if (file_count > 1) {
        abort_redivis_value_error(sprintf(
          "Path '%s' is a file, but the export consists of multiple files. Please specify the path to a directory",
          path
        ))
      }

      if (is_dir) {
        dir.create(path, showWarnings = FALSE, recursive = TRUE)
        if (file_count == 1) {
          escaped_table_name <- tolower(stringr::str_replace_all(
            self$properties$table$name,
            "\\W+",
            "_"
          ))
          path <- file.path(
            path,
            str_interp("${escaped_table_name}.${self$properties$format}")
          )
        }
      } else {
        dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
      }

      if (file_count == 1) {
        args <- list(
          uri = self$uri,
          download_path = path,
          overwrite = overwrite,
          size = self$properties$size,
          md5_hash = NULL
        )
        if (progress) {
          progressr::with_progress(do.call(perform_retryable_download, args))
        } else {
          do.call(perform_retryable_download, args)
        }
      } else {
        part_indices <- seq_len(file_count)

        args <- list(
          uris = purrr::map(part_indices, function(i) {
            paste0(self$uri, "/download?filePart=", i - 1)
          }),
          download_paths = purrr::map_chr(part_indices, function(i) {
            filename <- str_interp(
              "${formatC(i - 1, width = 6, flag = '0')}.${self$properties$format}"
            )
            file.path(path, filename)
          }),
          overwrite = overwrite,
          max_parallelization = max_parallelization,
          total_bytes = self$properties$size
        )
        if (progress) {
          progressr::with_progress(do.call(perform_parallel_download, args))
        } else {
          do.call(perform_parallel_download, args)
        }
      }
      path
    },

    wait_for_finish = function() {
      iter_count <- 0
      pb <- progressr::progressor(steps = 100)
      pb(message = "Preparing download...")
      previous_progress <- 0
      while (TRUE) {
        if (self$properties$status == "completed") {
          if (previous_progress != 100) {
            pb(
              amount = 100 - previous_progress,
              message = "Preparing download..."
            )
          }
          break
        } else if (self$properties$status == "failed") {
          abort_redivis_job_error(
            message = self$properties$errorMessage,
            kind = self$properties$kind,
            status = self$properties$status
          )
        } else if (self$properties$status == "cancelled") {
          abort_redivis_job_error(
            message = "Export job was cancelled",
            kind = self$properties$kind,
            status = self$properties$status
          )
        } else {
          iter_count <- iter_count + 1
          if (round(self$properties$percentCompleted) > previous_progress) {
            pb(
              amount = round(self$properties$percentCompleted) -
                previous_progress,
              message = "Preparing download..."
            )
            previous_progress <- round(self$properties$percentCompleted)
          }
          Sys.sleep(min(iter_count * 0.5, 2))
          self$get()
        }
      }
    }
  )
)
