# Point the client at the environment the live tests run against (see
# helper-live.R), for the rest of the run. Tests against the mock API point it
# elsewhere for their duration.
local({
  config <- live_config()
  ambient <- stats::setNames(
    rep(NA_character_, length(live_ambient_variables)),
    live_ambient_variables
  )
  withr::local_envvar(
    c(
      REDIVIS_API_ENDPOINT = config$api_endpoint,
      REDIVIS_CONFIG_DIR = config$config_dir,
      ambient
    ),
    .local_envir = testthat::teardown_env()
  )

  # The tests must never touch the developer's own credentials
  own_credentials <- file.path(path.expand("~"), ".redivis", "r_credentials")
  own_credentials_modified <- file.mtime(own_credentials)
  test_credentials <- get_credentials_file()
  if (
    !identical(
      normalizePath(own_credentials, mustWork = FALSE),
      normalizePath(test_credentials, mustWork = FALSE)
    )
  ) {
    withr::defer(
      if (!identical(file.mtime(own_credentials), own_credentials_modified)) {
        warning(
          "The tests modified ", own_credentials, ", which they should never ",
          "touch. Its credentials may now be for the test environment.",
          call. = FALSE
        )
      },
      envir = testthat::teardown_env()
    )
  }

  # Credentials already loaded in this session (e.g., by an interactive
  # devtools::load_all()) may be for another environment
  previous_credentials <- auth_vars$cached_credentials
  auth_vars$cached_credentials <- NULL
  withr::defer(
    auth_vars$cached_credentials <- previous_credentials,
    envir = testthat::teardown_env()
  )
})
