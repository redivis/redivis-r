# Helpers for the live tests (test-live-*.R), which run against a real Redivis
# API: by default the dev environment, at https://local.host:8443. See
# CONTRIBUTING.md for how to choose another, and the options below.
#
# A fresh environment has no data in it, so the tests upload their own. Each
# test calls live_api() (or one of the fixtures below, which call it), which
# skips the test if the API can't be reached. The first call checks that the
# client is logged in, logging in interactively if need be. Everything the
# tests create lives in datasets named with a per-run prefix, which are
# deleted as the tests that made them finish, or at the end of the run.
# Datasets left behind by an interrupted run are deleted by a later one.

# Named environments that REDIVIS_TEST_ENV accepts, in addition to an API URL,
# with defaults for the options below. The dev environment's user accounts are
# limited to 50 datasets, so the tests create theirs in an organization.
live_environments <- list(
  dev = list(
    api_endpoint = "https://local.host:8443/api/v1",
    owner = "demo",
    organization = "demo"
  ),
  prod = list(api_endpoint = "https://redivis.com/api/v1")
)
live_default_environment <- "dev"

live_resource_prefix <- "rtest_"
# Datasets from an earlier run older than this are assumed to be abandoned
live_stale_after_hours <- 12
# Uploads and other jobs normally finish in well under a minute
live_job_timeout_seconds <- 15 * 60

# Variables that change how references resolve. A developer's shell (or a
# Redivis notebook) may well set them, but the tests must run the same anyway.
live_ambient_variables <- c(
  "REDIVIS_DEFAULT_USER",
  "REDIVIS_DEFAULT_ORGANIZATION",
  "REDIVIS_DEFAULT_WORKFLOW",
  "REDIVIS_DEFAULT_DATASET",
  "REDIVIS_DEFAULT_NOTEBOOK"
)

env_or_null <- function(name) {
  value <- Sys.getenv(name)
  if (nzchar(value)) value
}

# The settings for this run, from REDIVIS_TEST_ENV and the other REDIVIS_TEST_*
# variables
live_config <- function() {
  environment <- Sys.getenv("REDIVIS_TEST_ENV", live_default_environment)
  settings <- if (environment %in% names(live_environments)) {
    live_environments[[environment]]
  } else if (grepl("^https?://", environment)) {
    api_endpoint <- sub("/+$", "", environment)
    # A URL for a named environment gets its defaults too
    named <- Filter(
      function(s) identical(s$api_endpoint, api_endpoint),
      live_environments
    )
    if (length(named)) named[[1]] else list(api_endpoint = api_endpoint)
  } else {
    stop(
      "Unknown REDIVIS_TEST_ENV '", environment, "': expected one of ",
      paste(names(live_environments), collapse = ", "), ", or an API URL",
      call. = FALSE
    )
  }

  list(
    api_endpoint = settings$api_endpoint,
    config_dir = live_config_dir(),
    owner = env_or_null("REDIVIS_TEST_OWNER") %||% settings$owner,
    organization = env_or_null("REDIVIS_TEST_ORGANIZATION") %||%
      settings$organization,
    keep_resources = isTRUE(as.logical(Sys.getenv("REDIVIS_TEST_KEEP_RESOURCES"))),
    run_slow = isTRUE(as.logical(Sys.getenv("REDIVIS_TEST_SLOW")))
  )
}

# The config directory for the live tests. Their credentials are kept apart
# from the developer's own, even for redivis.com. Within it, the client keeps
# each environment's apart (e.g., the dev environment's in
# ~/.redivis/test/environments/local.host_8443/).
live_config_dir <- function() {
  config_dir <- Sys.getenv("REDIVIS_TEST_CONFIG_DIR")
  path.expand(if (nzchar(config_dir)) config_dir else "~/.redivis/test")
}

# State shared by every test in the run (helper files are sourced once, into an
# environment that's the parent of each test file's)
live_state <- new.env(parent = emptyenv())
live_state$fixtures <- list()

# The live environment the test runs against, once it's verified to be
# reachable and the client has credentials for it. Skips the test otherwise.
live_api <- function() {
  testthat::skip_on_cran()
  if (is.null(live_state$context) && is.null(live_state$skip_reason)) {
    live_state$context <- tryCatch(
      start_live_run(),
      error = function(e) list(error = e)
    )
  }
  if (!is.null(live_state$skip_reason)) {
    testthat::skip(live_state$skip_reason)
  }
  if (!is.environment(live_state$context)) {
    rlang::abort(
      "Couldn't start the live tests",
      parent = live_state$context$error
    )
  }
  live_state$context
}

# Parts of the client run in other R processes (parallel reads and file
# uploads, via future), which load redivis from the library, rather than the
# working copy that devtools::load_all() loaded. They'd test the installed
# version instead, which may not even keep its credentials where setup-live.R
# says to. So install the working copy to a temporary library, for them to load.
use_working_copy_in_subprocesses <- function() {
  if (
    !isNamespaceLoaded("pkgload") ||
      !pkgload::is_dev_package("redivis")
  ) {
    return(invisible())
  }
  # Installing compiles the package in place, so install a copy of it, in case
  # other test processes are doing the same
  source <- file.path(tempdir(), "redivis-working-copy", "redivis")
  library <- file.path(tempdir(), "redivis-working-copy", "library")
  dir.create(source, recursive = TRUE, showWarnings = FALSE)
  dir.create(library, showWarnings = FALSE)
  package <- pkgload::pkg_path(testthat::test_path())
  for (entry in c("DESCRIPTION", "NAMESPACE", "LICENSE", "R", "src")) {
    file.copy(file.path(package, entry), source, recursive = TRUE)
  }
  unlink(list.files(
    file.path(source, "src"),
    pattern = "\\.(o|so|dll|dylib)$",
    full.names = TRUE
  ))
  callr::rcmd(
    "INSTALL",
    c(
      "--no-docs",
      "--no-multiarch",
      "--no-test-load",
      paste0("--library=", library),
      source
    ),
    fail_on_status = TRUE
  )
  withr::local_libpaths(
    library,
    action = "prefix",
    .local_envir = testthat::teardown_env()
  )
}

# Refreshes credentials that would expire during the run, so that parallel test
# processes don't all refresh them at once midway, which the API may answer
# with a 503. They may still all start at once, so a 503 is retried, after
# rereading the credentials, which another process may have refreshed.
refresh_expiring_credentials <- function(attempts = 5) {
  if (!is.na(Sys.getenv("REDIVIS_API_TOKEN", unset = NA))) {
    return(invisible())
  }
  for (attempt in seq_len(attempts)) {
    auth_vars$cached_credentials <- NULL
    load_cached_credentials()
    expires_at <- auth_vars$cached_credentials$expires_at
    if (is.null(expires_at) || expires_at > as.numeric(Sys.time()) + 30 * 60) {
      return(invisible())
    }
    refreshed <- tryCatch(
      {
        refresh_credentials()
        TRUE
      },
      redivis_api_error = function(e) {
        if (!isTRUE(e$status_code == 503) || attempt == attempts) {
          stop(e)
        }
        FALSE
      }
    )
    if (refreshed) {
      return(invisible())
    }
    Sys.sleep(stats::runif(1, 1, 5))
  }
}

start_live_run <- function() {
  config <- live_config()
  # setup-live.R points the client at the environment under test
  stopifnot(
    identical(Sys.getenv("REDIVIS_API_ENDPOINT"), config$api_endpoint),
    identical(get_redivis_dir(), config$config_dir)
  )

  reachable <- tryCatch(
    {
      curl::curl_fetch_memory(
        config$api_endpoint,
        handle = curl::new_handle(
          timeout = 10,
          ssl_verifypeer = get_verify_ssl()
        )
      )
      TRUE
    },
    error = function(e) conditionMessage(e)
  )
  if (!isTRUE(reachable)) {
    live_state$skip_reason <- sprintf(
      "the Redivis API at %s is unreachable (%s)",
      config$api_endpoint,
      reachable
    )
    return(NULL)
  }

  use_working_copy_in_subprocesses()

  if (has_credentials()) {
    refresh_expiring_credentials()
  } else {
    if (nzchar(Sys.getenv("CI"))) {
      stop(
        "No Redivis credentials are available. Set REDIVIS_API_TOKEN to run ",
        "the live tests non-interactively.",
        call. = FALSE
      )
    }
    if (identical(Sys.getenv("TESTTHAT_IS_PARALLEL"), "true")) {
      stop(
        "No Redivis credentials are available, and a parallel test process ",
        "can't log in. Run a live test serially first, to log in.",
        call. = FALSE
      )
    }
    message("Logging in to ", config$api_endpoint, " for the live tests")
    # Everything the tests need, so that they never prompt for more mid-run
    redivis$authenticate(
      scope = c("data.edit", "workflow.write", "organization.write")
    )
  }

  user <- redivis$current_user()
  owner <- if (is.null(config$owner) || identical(config$owner, user$name)) {
    user
  } else {
    organization <- redivis$organization(config$owner)
    if (organization$exists()) organization else redivis$user(config$owner)
  }

  context <- new.env(parent = emptyenv())
  context$config <- config
  context$user <- user
  context$owner <- owner
  context$run_id <- sprintf(
    "%s%s_%d",
    live_resource_prefix,
    format(Sys.time(), "%Y%m%d_%H%M%S"),
    Sys.getpid()
  )
  context$created_datasets <- list()

  delete_stale_resources(owner)
  withr::defer(
    for (dataset in context$created_datasets) {
      tryCatch(
        delete_live_dataset(dataset),
        redivis_error = function(e) {
          message("Couldn't delete ", dataset$qualified_reference, ": ", conditionMessage(e))
        }
      )
    },
    envir = testthat::teardown_env()
  )
  context
}

# A name unique to this run, which marks a resource as the tests'
live_unique_name <- function(label = "") {
  live <- live_api()
  paste0(
    live$run_id,
    "_",
    if (nzchar(label)) paste0(label, "_"),
    substr(gsub("-", "", uuid::UUIDgenerate()), 1, 6)
  )
}

# Deletes a dataset the tests created, unless they're to be kept
delete_live_dataset <- function(dataset) {
  if (!live_config()$keep_resources) {
    delete_dataset(dataset)
  }
}

# Deletes a dataset, waiting out a release that's still in progress
delete_dataset <- function(dataset, timeout = 120) {
  # The dataset as a whole, rather than whichever version it references
  reference <- sub(
    ":(v\\d+_\\d+|next|current)$",
    "",
    dataset$qualified_reference
  )
  deadline <- Sys.time() + timeout
  repeat {
    deleted <- tryCatch(
      {
        redivis$dataset(reference)$delete()
        TRUE
      },
      redivis_not_found_error = function(e) TRUE,
      redivis_api_error = function(e) {
        if (
          !grepl("process of being released", conditionMessage(e)) ||
            Sys.time() > deadline
        ) {
          stop(e)
        }
        FALSE
      }
    )
    if (deleted) {
      return(invisible())
    }
    Sys.sleep(2)
  }
}

# Deletes the datasets and workflows left behind by an interrupted run
delete_stale_resources <- function(owner) {
  stale_before <- (as.numeric(Sys.time()) - live_stale_after_hours * 3600) * 1000
  is_stale <- function(resource) {
    startsWith(resource$name, live_resource_prefix) &&
      isTRUE(resource$properties$createdAt < stale_before)
  }
  delete_stale <- function(resource, delete) {
    tryCatch(
      delete(resource),
      redivis_error = function(e) {
        message(
          "Couldn't delete stale resource ", resource$qualified_reference, ": ",
          conditionMessage(e)
        )
      }
    )
  }

  for (dataset in Filter(is_stale, owner$list_datasets())) {
    delete_stale(dataset, delete_dataset)
  }
  for (workflow in Filter(is_stale, owner$list_workflows())) {
    delete_stale(workflow, function(workflow) workflow$delete())
  }
}

# ---- Fixtures ----------------------------------------------------------------

# Creates a fixture once per run, and returns it to each test that asks. A
# fixture that fails to be created fails each of those tests too, rather than
# being retried by each.
live_fixture <- function(name, create) {
  live_api()
  if (is.null(live_state$fixtures[[name]])) {
    live_state$fixtures[[name]] <- tryCatch(
      list(value = create()),
      error = function(e) list(error = e)
    )
  }
  fixture <- live_state$fixtures[[name]]
  if (!is.null(fixture$error)) {
    rlang::abort(
      sprintf("The %s fixture couldn't be created", name),
      parent = fixture$error
    )
  }
  fixture$value
}

# Creates a dataset that's deleted at the end of the run
create_live_dataset <- function(label = "", ...) {
  live <- live_api()
  dataset <- live$owner$dataset(live_unique_name(label))$create(...)
  live$created_datasets[[length(live$created_datasets) + 1]] <- dataset
  dataset
}

# Creates a dataset that's deleted once the calling test finishes
local_live_dataset <- function(label = "", ..., .local_envir = parent.frame()) {
  live <- live_api()
  dataset <- live$owner$dataset(live_unique_name(label))$create(...)
  withr::defer(delete_live_dataset(dataset), envir = .local_envir)
  dataset
}

# An unreleased dataset, for tests to create tables in
live_scratch_dataset <- function() {
  live_fixture("scratch_dataset", function() create_live_dataset("scratch"))
}

# Creates a table in the scratch dataset, which is deleted once the calling
# test finishes
local_live_table <- function(label = "table", ..., .local_envir = parent.frame()) {
  table <- live_scratch_dataset()$table(live_unique_name(label))$create(...)
  withr::defer(
    if (!live_config()$keep_resources && table$exists()) {
      table$delete()
    },
    envir = .local_envir
  )
  table
}

# A dataset holding tables of known contents, for tests to read. Each table's
# upload is started here, and waited for by the fixture for that table, so that
# they're processed concurrently.
live_sample_dataset <- function() {
  live_sample_data()$dataset
}

live_sample_data <- function() {
  live_fixture("sample_data", create_sample_data)
}

create_sample_data <- function() {
  dataset <- create_live_dataset("sample", description = "Sample data for tests")
  tables <- list()
  uploads <- list()

  tables$tiny <- dataset$table("tiny")$create(description = "tiny.csv")
  uploads$tiny <- tables$tiny$upload()$create(
    tiny_csv_path(),
    wait_for_finish = FALSE,
    progress = FALSE
  )

  tables$typed <- dataset$table("typed")$create()
  uploads$typed <- tables$typed$upload("typed")$create(
    typed_data(),
    wait_for_finish = FALSE,
    progress = FALSE
  )

  tables$geo <- dataset$table("geo")$create()
  uploads$geo <- tables$geo$upload("features.geojson")$create(
    geojson_raw(),
    type = "geojson",
    wait_for_finish = FALSE,
    progress = FALSE
  )

  files_dir <- withr::local_tempdir(.local_envir = testthat::teardown_env())
  write_files(files_dir, raw_files())
  tables$files <- dataset$table("files")$create(is_file_index = TRUE)
  tables$files$add_files(directory = files_dir, progress = FALSE)

  list(dataset = dataset, tables = tables, uploads = uploads)
}

live_sample_table <- function(name) {
  live_fixture(paste0("table_", name), function() {
    sample_data <- live_sample_data()
    if (!is.null(sample_data$uploads[[name]])) {
      wait_for_upload(sample_data$uploads[[name]])
    }
    sample_data$tables[[name]]$get()
  })
}

# fixtures/tiny.csv, uploaded as a CSV
live_tiny_table <- function() live_sample_table("tiny")

# typed_data(), uploaded as a data frame (i.e., as parquet)
live_typed_table <- function() live_sample_table("typed")

# geo_features, uploaded as GeoJSON
live_geo_table <- function() live_sample_table("geo")

# A file index table holding raw_files()
live_files_table <- function() live_sample_table("files")

# Adds a table with a few rows to a dataset, which it needs to be released
add_small_table <- function(dataset, name = "t") {
  table <- dataset$table(name)$create()
  table$upload("small.csv")$create(small_csv, progress = FALSE)
  table
}

# Creates a workflow, which is deleted (with everything in it) once the calling
# test finishes
local_live_workflow <- function(label = "workflow", ..., .local_envir = parent.frame()) {
  live <- live_api()
  workflow <- live$owner$workflow(live_unique_name(label))$create(...)
  withr::defer(
    if (!live_config()$keep_resources) {
      tryCatch(workflow$delete(), redivis_not_found_error = function(e) NULL)
    },
    envir = .local_envir
  )
  workflow
}

# An organization the test user belongs to
live_test_organization <- function() {
  live <- live_api()
  if (is.null(live$config$organization)) {
    testthat::skip("needs an organization; set REDIVIS_TEST_ORGANIZATION")
  }
  redivis$organization(live$config$organization)
}

skip_unless_slow <- function() {
  if (!live_config()$run_slow) {
    testthat::skip("slow; set REDIVIS_TEST_SLOW=true to run")
  }
}

# Polls until condition() is TRUE, for things that become true eventually, such
# as a new dataset appearing in its owner's listing. Returns whether it did.
eventually <- function(condition, timeout = 60, interval = 2) {
  deadline <- Sys.time() + timeout
  repeat {
    if (isTRUE(condition())) {
      return(TRUE)
    }
    if (Sys.time() > deadline) {
      return(FALSE)
    }
    Sys.sleep(interval)
  }
}

# Wait for an upload created with wait_for_finish = FALSE, raising if it fails
wait_for_upload <- function(upload, timeout = live_job_timeout_seconds) {
  deadline <- Sys.time() + timeout
  repeat {
    status <- upload$get()$properties$status
    if (identical(status, "completed")) {
      return(invisible(upload))
    }
    if (identical(status, "failed")) {
      abort_redivis_job_error(
        message = upload$properties$errorMessage,
        kind = upload$properties$kind,
        status = status
      )
    }
    if (Sys.time() > deadline) {
      stop(sprintf("%s didn't finish within %ds (%s)", upload$uri, timeout, status))
    }
    Sys.sleep(2)
  }
}
