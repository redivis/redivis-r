# R/exceptions.R

#' Abort with a Redivis error
#' @keywords internal
abort_redivis_error <- function(
  message,
  class = "redivis_error",
  ...,
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  rlang::abort(
    message = message,
    class = class,
    ...,
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- APIError ----

abort_redivis_api_error <- function(
  message,
  status_code,
  description = "",
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  display_msg <- str_interp(
    "[${status_code} ${message}] ${description}"
  )
  rlang::abort(
    message = display_msg,
    class = c("redivis_api_error", "redivis_error"),
    status_code = status_code,
    description = description,
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- NotFoundError ----

abort_redivis_not_found_error <- function(
  message = "",
  status_code = 404L,
  description = "",
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  display_msg <- description
  rlang::abort(
    message = display_msg,
    class = c("redivis_not_found_error", "redivis_api_error", "redivis_error"),
    status_code = status_code,
    description = description,
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- AuthorizationError ----

abort_redivis_authorization_error <- function(
  message,
  status_code,
  description = "",
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  display_msg <- paste0("[", status_code, " ", message, "] ", description)
  rlang::abort(
    message = display_msg,
    class = c(
      "redivis_authorization_error",
      "redivis_api_error",
      "redivis_error"
    ),
    status_code = status_code,
    description = description,
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- NetworkError ----

abort_redivis_network_error <- function(
  message = "A network error occurred",
  original_exception = NULL,
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  display_msg <- if (!is.null(original_exception)) {
    paste0(message, ": ", conditionMessage(original_exception))
  } else {
    message
  }
  rlang::abort(
    message = display_msg,
    class = c(
      "redivis_network_error",
      "redivis_error"
    ),
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}


# ---- ValueError ----
# (Avoid naming collision with base::ValueError; use redivis_value_error)

abort_redivis_value_error <- function(message, call = rlang::caller_env()) {
  info <- redivis_stack_depth("redivis")

  rlang::abort(
    message = message,
    class = c(
      "redivis_value_error",
      "redivis_error"
    ),
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- JobError ----

abort_redivis_job_error <- function(
  message = NULL,
  kind = NULL,
  status = "status unknown",
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  final_message <- message %||% paste0("Job finished with status: ", status)
  display_msg <- paste0("[", kind, " ", status, "] ", final_message)
  rlang::abort(
    message = display_msg,
    class = c(
      "redivis_job_error",
      "redivis_error"
    ),
    kind = kind,
    status = status,
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

# ---- DeprecationError ----

abort_redivis_deprecation_error <- function(
  message,
  call = rlang::caller_env()
) {
  info <- redivis_stack_depth("redivis")
  rlang::abort(
    message = message,
    class = c(
      "redivis_deprecation_error",
      "redivis_error"
    ),
    trace = rlang::trace_back(bottom = sys.frame(info$first_in_pkg)),
    call = rlang::caller_env(info$last_in_pkg - info$first_in_pkg - 1)
  )
}

redivis_stack_depth <- function(pkg = "redivis") {
  calls <- sys.calls()
  frames <- sys.frames()

  # Identify namespace for each frame, if any
  ns <- vapply(
    frames,
    function(env) {
      nsenv <- tryCatch(getNamespaceName(env), error = function(e) {
        NA_character_
      })
      # getNamespaceName() works for namespace envs; many frames aren't namespace envs
      if (is.na(nsenv)) {
        # fall back: where is this frame located?
        # (tends to return "redivis" for functions defined in your namespace)
        tryCatch(environmentName(topenv(env)), error = function(e) {
          NA_character_
        })
      } else {
        nsenv
      }
    },
    character(1)
  )

  in_pkg <- which(ns == pkg)
  last_in_pkg <- if (length(in_pkg)) max(in_pkg) else 0L
  first_in_pkg <- if (length(in_pkg)) min(in_pkg) else 0L

  list(
    calls = calls,
    namespaces = ns,
    last_in_pkg = last_in_pkg,
    first_in_pkg = first_in_pkg
  )
}

#' Raise an API error based on response details
#'
#' @param response_json Optional parsed JSON list (e.g. httr2::resp_body_json()).
#' @param response_text Optional raw response text (character scalar).
#' @param response Optional response object with a status code.
#'
#' @keywords internal
raise_api_error <- function(
  response_json = NULL,
  response_text = NULL,
  response = NULL,
  call = rlang::caller_env()
) {
  status_code <- NULL
  if (!is.null(response)) {
    # Support both httr2 responses (httr2::resp_status) and raw status codes
    status_code <- tryCatch(
      httr2::resp_status(response),
      error = function(e) response$status_code %||% response$status %||% NULL
    )
  }
  if (is.null(status_code) && !is.null(response_json)) {
    status_code <- response_json$status %||% NULL
  }
  status_code <- as.integer(status_code)

  err <- if (!is.null(response_json)) {
    response_json$error %||% "api_error"
  } else {
    "api_error"
  }

  description <- if (!is.null(response_json)) {
    response_json$error_description %||% response_text %||% ""
  } else {
    response_text %||% ""
  }

  if (identical(status_code, 404L)) {
    abort_redivis_not_found_error(
      message = err,
      status_code = 404L,
      description = description,
      call = call
    )
  } else if (identical(status_code, 403L)) {
    abort_redivis_authorization_error(
      message = err,
      status_code = 403L,
      description = description,
      call = call
    )
  } else {
    abort_redivis_api_error(
      message = err,
      status_code = status_code %||% NA_integer_,
      description = description,
      call = call
    )
  }

  invisible(NULL)
}
