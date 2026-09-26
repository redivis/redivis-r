#' @include Table.R api_request.R util.R
Export <- R6::R6Class(
  "Export",
  public = list(
    table = NULL,
    query = NULL,
    upload = NULL,
    properties = NULL,
    uri = NULL,

    initialize = function(
      table = NULL,
      query = NULL,
      upload = NULL,
      properties = list()
    ) {
      self$table <- table
      self$query <- query
      self$upload <- upload
      self$uri <- properties$uri
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp("<Export ${self$uri} of ${self$get_download_name()}>\n"))
      invisible(self)
    },

    # The name of the file (or directory, for multi-part exports) that the
    # export is downloaded to when a path to a directory is given
    get_download_name = function() {
      escape <- function(name) {
        tolower(stringr::str_replace_all(name, "\\W+", "_"))
      }
      if (!is.null(self$table)) {
        escape(self$table$properties$name %||% self$table$name %||% "table")
      } else if (!is.null(self$query)) {
        finished_at <- self$query$properties$finishedAt
        if (is.null(finished_at)) {
          return("query")
        }
        finished_at <- as.POSIXct(
          as.numeric(finished_at) / 1000,
          origin = "1970-01-01"
        )
        paste0(
          "query_",
          format(finished_at, "%Y-%m-%dT%H_%M_%S_"),
          sprintf("%06d", round(as.numeric(finished_at) %% 1 * 1e6))
        )
      } else if (!is.null(self$upload)) {
        escape(self$upload$properties$name %||% self$upload$name %||% "upload")
      } else {
        escape(self$properties$table$name %||% "export")
      }
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
      max_concurrency = NULL,
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
          path <- file.path(path, self$get_download_name())
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
          path <- file.path(
            path,
            str_interp("${self$get_download_name()}.${self$properties$format}")
          )
        }
      } else {
        dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
      }

      if (file_count == 1) {
        args <- list(
          # NB: the export itself lives at self$uri; its contents are here
          uri = paste0(self$uri, "/download?filePart=0"),
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
          max_concurrency = max_concurrency,
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
      pb(amount = 0, message = "Preparing download...")
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
