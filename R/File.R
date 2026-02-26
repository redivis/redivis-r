#' @include api_request.R util.R retryable_download.R
#' @useDynLib redivis, .registration = TRUE
File <- setRefClass(
  "File",
  fields = list(
    id = "character",
    path = "character",
    name = "character",
    size = "ANY",
    hash = "ANY",
    properties = "list",
    table = "ANY",
    query = "ANY",
    directory = "ANY"
  ),
  methods = list(
    initialize = function(
      ...,
      id = "",
      name = "",
      properties = list(),
      table = NULL,
      query = NULL,
      directory = NULL
    ) {
      if (is.null(table) && is.null(query)) {
        abort_redivis_value_error(
          "All files must either belong to a table or query."
        )
      }

      id <<- id
      path <<- name
      name <<- basename(name)
      table <<- table
      query <<- query
      directory <<- directory
      properties <<- properties

      size <<- properties[["size"]]
      file_hash <- properties[["md5_hash"]]
      if (!is.null(file_hash)) {
        hash <<- base64enc::base64decode(file_hash)
      }
    },

    show = function() {
      print(str_interp(
        "<File ${.self$path}>"
      ))
    },

    get = function() {
      warning(
        "This method is deprecated. Nothing to fetch; all file metadata already exists at file.properties",
        call. = FALSE
      )
      .self
    },

    download = function(
      path = NULL,
      overwrite = FALSE,
      progress = TRUE
    ) {
      download_path <- path
      is_dir = FALSE

      if (is.null(download_path)) {
        download_path <- getwd()
        is_dir <- TRUE
      } else if (endsWith(download_path, '/')) {
        is_dir <- TRUE
        download_path <- stringr::str_sub(
          download_path,
          1,
          nchar(download_path) - 1
        ) # remove trailing "/", as this screws up file.path()
      } else if (dir.exists(download_path)) {
        is_dir <- TRUE
      }

      if (is_dir) {
        file_name <- file.path(download_path, .self$name)
      } else {
        file_name <- download_path
      }

      args = list(
        uri = str_interp("/rawFiles/${.self$id}"),
        download_path = file_name,
        overwrite = overwrite,
        size = .self$size,
        md5_hash = .self$hash
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
        path = str_interp("/rawFiles/${.self$id}"),
        parse_response = FALSE,
        start_byte = start_byte,
        end_byte = end_byte
      )
      if (as_text) httr2::resp_body_string(res) else httr2::resp_body_raw(res)
    },

    stream = function(callback, start_byte = 0, end_byte = NULL) {
      make_request(
        method = "GET",
        path = str_interp("/rawFiles/${.self$id}"),
        parse_response = FALSE,
        stream_callback = callback,
        start_byte = start_byte,
        end_byte = end_byte
      )
    },

    open = function(mode = "rb") {
      # Normalize mode: "r" -> "r", "rb" -> "rb", "rt" -> "r"
      # Only read modes are supported
      if (!mode %in% c("r", "rb", "rt")) {
        abort_redivis_error(
          str_interp(
            "Unsupported mode '${mode}'. Only 'r', 'rb', and 'rt' are supported."
          )
        )
      }
      # Normalize: "rt" is just text mode "r"
      if (mode == "rt") {
        mode <- "r"
      }
      file_size <- .self$size
      file_id <- .self$id
      file_path <- .self$path

      # Mutable state for the streaming HTTP connection
      env <- new.env(parent = emptyenv())
      env$stream_con <- NULL # the httr2 streaming response object
      env$buffer <- raw(0) # leftover bytes from last chunk
      env$idle_timer <- NULL # handle from later::later()
      env$last_timer_reset <- NULL
      env$IDLE_TIMEOUT_SECS <- 30 # close stream after 30s

      # Cancel any pending idle-timeout timer
      env$cancel_timer <- function() {
        if (!is.null(env$idle_timer)) {
          env$idle_timer()
          env$idle_timer <- NULL
        }
      }

      # Schedule (or reschedule) the idle-timeout timer
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
        # Close any existing stream first
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
        # Read from the HTTP stream until we have n bytes (or EOF)
        tryCatch(
          {
            while (length(env$buffer) < n) {
              chunk <- httr2::resp_stream_raw(env$stream_con, kb = 64)
              if (length(chunk) == 0) {
                break
              } # EOF
              env$buffer <- c(env$buffer, chunk)
            }
          },
          error = function(e) {
            # Connection broken (network drop, server timeout, etc.)
            # Close the dead stream so the C layer will re-open on retry
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
setMethod("open", "File", function(con, mode = "rb", ...) {
  con$open(mode)
})
