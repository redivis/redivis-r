#' @include util.R
NULL

# Accept class instances (e.g., redivis$workflow("owner.name")) as well as
# reference strings
parse_config_reference <- function(value, name) {
  if (is.character(value)) {
    return(parse_config_string(value, name))
  }
  reference <- if (inherits(value, "R6")) {
    value$qualified_reference %||% value$name
  }
  if (!is.character(reference) || length(reference) != 1) {
    abort_redivis_value_error(str_interp(
      "${name} must be a reference string, or a Redivis object"
    ))
  }
  reference
}

parse_config_string <- function(value, name) {
  if (!is.character(value) || length(value) != 1 || is.na(value) || !nzchar(value)) {
    abort_redivis_value_error(str_interp("${name} must be a non-empty string"))
  }
  value
}

parse_config_url <- function(value, name) {
  if (!grepl("^https?://[^/]", parse_config_string(value, name))) {
    abort_redivis_value_error(str_interp(
      '${name} must be an http(s) URL, but got "${value}"'
    ))
  }
  sub("/+$", "", value)
}

# Environment variables are strings, so these are also accepted
parse_config_positive_integer <- function(value, name) {
  if (is.character(value) && length(value) == 1 && grepl("^\\s*\\d+\\s*$", value)) {
    value <- as.numeric(value)
  }
  if (
    !is.numeric(value) || length(value) != 1 || is.na(value) ||
      value != round(value) || value < 1
  ) {
    abort_redivis_value_error(str_interp(
      "${name} must be a positive integer, but got ${format(value)}"
    ))
  }
  as.integer(value)
}

parse_config_boolean <- function(value, name) {
  if (is.character(value) && length(value) == 1) {
    normalized <- tolower(trimws(value))
    if (normalized %in% c("1", "true", "yes", "on")) {
      return(TRUE)
    }
    if (normalized %in% c("0", "false", "no", "off")) {
      return(FALSE)
    }
  }
  if (!is.logical(value) || length(value) != 1 || is.na(value)) {
    abort_redivis_value_error(str_interp(
      "${name} must be TRUE or FALSE, but got ${format(value)}"
    ))
  }
  value
}

# Each setting falls back to its environment variable when it hasn't been set
# via redivis$config(), and then to its default, if it has one. Environment
# variable names are always REDIVIS_ followed by the setting's name in
# uppercase; any others are older names that are still honored.
config_settings <- list(
  default_workflow = list(env = "REDIVIS_DEFAULT_WORKFLOW", parse = parse_config_reference),
  default_dataset = list(env = "REDIVIS_DEFAULT_DATASET", parse = parse_config_reference),
  default_user = list(env = "REDIVIS_DEFAULT_USER", parse = parse_config_reference),
  default_organization = list(env = "REDIVIS_DEFAULT_ORGANIZATION", parse = parse_config_reference),
  api_endpoint = list(
    env = "REDIVIS_API_ENDPOINT",
    parse = parse_config_url,
    default = "https://redivis.com/api/v1"
  ),
  config_dir = list(env = "REDIVIS_CONFIG_DIR", parse = parse_config_string),
  tmp_dir = list(env = c("REDIVIS_TMP_DIR", "REDIVIS_TMPDIR"), parse = parse_config_string),
  max_parallelization = list(
    env = "REDIVIS_MAX_PARALLELIZATION",
    parse = parse_config_positive_integer
  ),
  progress = list(env = "REDIVIS_PROGRESS", parse = parse_config_boolean, default = TRUE)
)

# Only static state lives here; see the note at the top of auth.R
config_overrides <- new.env(parent = emptyenv())

# The implementation of redivis$config(), documented there
set_config <- function(
  default_workflow,
  default_dataset,
  default_user,
  default_organization,
  api_endpoint,
  config_dir,
  tmp_dir,
  max_parallelization,
  progress
) {
  passed <- names(as.list(match.call()))[-1]
  args <- environment()
  # Validate everything before setting anything, so that an invalid value
  # doesn't leave a partial update
  parsed <- lapply(stats::setNames(nm = passed), function(key) {
    value <- get(key, envir = args, inherits = FALSE)
    if (is.null(value)) NULL else config_settings[[key]]$parse(value, key)
  })
  for (key in passed) {
    if (is.null(parsed[[key]])) {
      if (exists(key, envir = config_overrides, inherits = FALSE)) {
        rm(list = key, envir = config_overrides)
      }
    } else {
      assign(key, parsed[[key]], envir = config_overrides)
    }
  }

  current <- lapply(stats::setNames(nm = names(config_settings)), get_config)
  if (length(passed)) invisible(current) else current
}

get_config <- function(key) {
  if (exists(key, envir = config_overrides, inherits = FALSE)) {
    return(get(key, envir = config_overrides, inherits = FALSE))
  }
  setting <- config_settings[[key]]
  for (env_variable in setting$env) {
    if (has_env(env_variable)) {
      return(setting$parse(Sys.getenv(env_variable), env_variable))
    }
  }
  setting$default
}

# Returns an argument's value or, when it's NULL, its configured default, and
# then the fallback
resolve_config <- function(key, value, fallback = NULL) {
  if (!is.null(value)) {
    return(value)
  }
  get_config(key) %||% fallback
}
