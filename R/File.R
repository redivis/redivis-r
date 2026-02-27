#' @include api_request.R util.R retryable_download.R
#' @useDynLib redivis, .registration = TRUE
#' @importFrom R6 R6Class
File <- R6::R6Class(
  "File",
  public = list(
    id = NULL,
    path = NULL,
    name = NULL,
    size = NULL,
    hash = NULL,
    added_at = NULL,
    properties = NULL,
    table = NULL,
    query = NULL,
    directory = NULL,

    initialize = function(
      id = "",
      name = "",
      properties = list(),
      table = NULL,
      query = NULL,
      directory = NULL
    ) {
      self$id <- id
      self$path <- name
      self$name <- basename(name)
      self$table <- table
      self$query <- query
      self$directory <- directory
      self$properties <- properties
      self$size <- properties[["size"]]
      added <- properties[["added_at"]]
      if (!is.null(added)) {
        self$added_at <- as.POSIXct(added, origin = "1970-01-01", tz = "UTC")
      }
      file_hash <- properties[["md5_hash"]]
      if (!is.null(file_hash)) {
        self$hash <- base64enc::base64decode(file_hash)
      }
    },

    print = function(...) {
      cat(str_interp("<File ${self$path}>\n"))
      invisible(self)
    },

    get = function() {
      warning(
        "This method is deprecated. Nothing to fetch; all file metadata already exists at file.properties",
        call. = FALSE
      )
      self
    },

    download = function(
      path = NULL,
      overwrite = FALSE,
      progress = TRUE
    ) {
      download_path <- path
      is_dir <- FALSE

      if (is.null(download_path)) {
        download_path <- getwd()
        is_dir <- TRUE
      } else if (endsWith(download_path, '/')) {
        is_dir <- TRUE
        download_path <- stringr::str_sub(
          download_path,
          1,
          nchar(download_path) - 1
        )
      } else if (dir.exists(download_path)) {
        is_dir <- TRUE
      }

      if (is_dir) {
        file_name <- file.path(download_path, self$name)
      } else {
        file_name <- download_path
      }

      args <- list(
        uri = str_interp("/rawFiles/${self$id}"),
        download_path = file_name,
        overwrite = overwrite,
        size = self$size,
        md5_hash = self$hash
      )
      if (progress) {
        progressr::with_progress(do.call(perform_retryable_download, args))
      } else {
        do.call(perform_retryable_download, args)
      }
    },

    read = function(as_text = FALSE, start_byte = 0, end_byte = NULL) {
      res <- make_request(
        method = "GET",
        path = str_interp("/rawFiles/${self$id}"),
        parse_response = FALSE,
        start_byte = start_byte,
        end_byte = end_byte
      )
      if (as_text) httr2::resp_body_string(res) else httr2::resp_body_raw(res)
    },

    stream = function(callback, start_byte = 0, end_byte = NULL) {
      make_request(
        method = "GET",
        path = str_interp("/rawFiles/${self$id}"),
        parse_response = FALSE,
        stream_callback = callback,
        start_byte = start_byte,
        end_byte = end_byte
      )
    },

    open = function(mode = "rb") {
      if (!mode %in% c("r", "rb", "rt")) {
        abort_redivis_error(
          str_interp(
            "Unsupported mode '${mode}'. Only 'r', 'rb', and 'rt' are supported."
          )
        )
      }
      if (mode == "rt") {
        mode <- "r"
      }
      file_size <- self$size
      file_id <- self$id
      file_path <- self$path

      env <- new.env(parent = emptyenv())
      env$stream_con <- NULL
      env$buffer <- raw(0)
      env$idle_timer <- NULL
      env$last_timer_reset <- NULL
      env$IDLE_TIMEOUT_SECS <- 30

      env$cancel_timer <- function() {
        if (!is.null(env$idle_timer)) {
          env$idle_timer()
          env$idle_timer <- NULL
        }
      }

      env$reset_timer <- function() {
        now <- proc.time()[["elapsed"]]
        if (
          !is.null(env$last_timer_reset) &&
            (now - env$last_timer_reset) < 1
        ) {
          return(invisible(NULL))
        }
        env$cancel_timer()
        env$last_timer_reset <- now
        env$idle_timer <- later::later(
          function() {
            env$close_stream()
          },
          delay = env$IDLE_TIMEOUT_SECS
        )
      }

      env$open_stream <- function(start_byte) {
        if (!is.null(env$stream_con)) {
          try(close(env$stream_con), silent = TRUE)
        }
        env$buffer <- raw(0)

        env$stream_con <- make_request(
          method = "GET",
          path = paste0("/rawFiles/", file_id),
          as_connection = TRUE,
          start_byte = start_byte
        )
        env$reset_timer()
      }

      env$read_stream <- function(n) {
        tryCatch(
          {
            while (length(env$buffer) < n) {
              chunk <- httr2::resp_stream_raw(env$stream_con, kb = 64)
              if (length(chunk) == 0) {
                break
              }
              env$buffer <- c(env$buffer, chunk)
            }
          },
          error = function(e) {
            env$close_stream()
          }
        )

        env$reset_timer()

        take <- min(n, length(env$buffer))
        if (take == 0) {
          return(raw(0))
        }

        result <- env$buffer[seq_len(take)]
        env$buffer <- env$buffer[-seq_len(take)]
        result
      }

      env$close_stream <- function() {
        env$cancel_timer()
        if (!is.null(env$stream_con)) {
          try(close(env$stream_con), silent = TRUE)
          env$stream_con <- NULL
        }
        env$buffer <- raw(0)
        env$last_timer_reset <- NULL
      }

      .Call(
        "C_redivis_connection",
        env,
        paste0("redivis://", file_path),
        as.double(file_size),
        mode
      )
    }
  )
)

#' Open a connection to a Redivis file
#'
#' Makes \code{open(file)} behave like \code{file$open()}, returning a
#' read-only connection backed by HTTP range requests.
#'
#' @param con A Redivis File object
#' @param mode Character string. \code{"rb"} (default) for binary mode,
#'   \code{"r"} or \code{"rt"} for text mode.
#' @param ... Ignored (present for compatibility with the generic)
#' @return A readable connection backed by HTTP range requests
#' @export
open.File <- function(con, mode = "rb", ...) {
  con$open(mode)
}
