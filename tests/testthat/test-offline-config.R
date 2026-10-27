# Library-wide settings, via redivis$config() and their environment variables

local_clean_config <- function(env = parent.frame()) {
  withr::local_envvar(
    REDIVIS_DEFAULT_WORKFLOW = NA,
    REDIVIS_DEFAULT_DATASET = NA,
    REDIVIS_MAX_PARALLELIZATION = NA,
    REDIVIS_PROGRESS = NA,
    REDIVIS_TMP_DIR = NA,
    REDIVIS_TMPDIR = NA,
    .local_envir = env
  )
  withr::defer(
    do.call(
      redivis$config,
      stats::setNames(
        rep(list(NULL), length(config_settings)),
        names(config_settings)
      )
    ),
    envir = env
  )
}

test_that("settings fall back to environment variables", {
  local_clean_config()
  expect_null(redivis$config()$default_workflow)
  withr::local_envvar(REDIVIS_DEFAULT_WORKFLOW = "jane.from_env")
  expect_equal(redivis$config()$default_workflow, "jane.from_env")
})

test_that("set values take precedence, until cleared with NULL", {
  local_clean_config()
  withr::local_envvar(REDIVIS_DEFAULT_WORKFLOW = "jane.from_env")
  expect_equal(
    redivis$config(default_workflow = "jane.from_config")$default_workflow,
    "jane.from_config"
  )
  # Arguments that aren't passed leave their values as they are
  expect_equal(redivis$config()$default_workflow, "jane.from_config")
  expect_equal(
    redivis$config(default_workflow = NULL)$default_workflow,
    "jane.from_env"
  )
})

test_that("setting values returns the configuration invisibly", {
  local_clean_config()
  expect_visible(redivis$config())
  expect_invisible(redivis$config(progress = FALSE))
})

test_that("unknown options are rejected", {
  local_clean_config()
  expect_error(redivis$config(default_table = "x"), "unused argument")
})

test_that("values are validated before any are set", {
  local_clean_config()
  invalid <- list(
    list(max_parallelization = 0),
    list(max_parallelization = 2.5),
    list(api_endpoint = "redivis.com"),
    list(progress = "sometimes"),
    list(default_workflow = "jane.wf", default_dataset = 12)
  )
  for (args in invalid) {
    expect_error(do.call(redivis$config, args), class = "redivis_value_error")
  }
  expect_null(redivis$config()$default_workflow)
})

test_that("Redivis objects are accepted as references", {
  local_clean_config()
  config <- redivis$config(default_workflow = redivis$workflow("jane.wf"))
  expect_equal(config$default_workflow, "jane.wf")
  expect_equal(redivis$current_workflow()$qualified_reference, "jane.wf")
})

test_that("environment variables are parsed", {
  local_clean_config()
  withr::local_envvar(REDIVIS_MAX_PARALLELIZATION = "4", REDIVIS_PROGRESS = "false")
  expect_identical(redivis$config()$max_parallelization, 4L)
  expect_false(redivis$config()$progress)

  withr::local_envvar(REDIVIS_MAX_PARALLELIZATION = "many")
  expect_error(redivis$config(), "REDIVIS_MAX_PARALLELIZATION", class = "redivis_value_error")
})

test_that("tmp_dir honors the legacy environment variable", {
  local_clean_config()
  withr::local_envvar(REDIVIS_TMPDIR = "/some/dir")
  expect_equal(redivis$config()$tmp_dir, "/some/dir")
})

test_that("arguments take precedence over the configuration", {
  local_clean_config()
  expect_true(resolve_config("progress", NULL))
  expect_equal(resolve_config("max_parallelization", NULL, 7), 7)

  redivis$config(progress = FALSE, max_parallelization = 3)
  expect_false(resolve_config("progress", NULL))
  expect_true(resolve_config("progress", TRUE))
  expect_equal(resolve_config("max_parallelization", NULL, 7), 3)
})

test_that("the default workflow qualifies tables", {
  local_clean_config()
  redivis$config(default_workflow = "owner.wf")
  expect_equal(redivis$table("t")$qualified_reference, "owner.wf.t")
})

test_that("credentials are reloaded when config_dir changes", {
  local_mock_api(authenticated = FALSE)
  local_clean_config()
  dir <- withr::local_tempdir()
  for (name in c("a", "b")) {
    dir.create(file.path(dir, name))
    credentials <- fake_credentials()
    credentials$access_token <- paste0("token_", name)
    writeLines(
      jsonlite::toJSON(credentials, auto_unbox = TRUE),
      file.path(dir, name, "r_credentials")
    )
  }

  redivis$config(config_dir = file.path(dir, "a"))
  expect_true(has_credentials())
  expect_equal(auth_vars$cached_credentials$access_token, "token_a")

  redivis$config(config_dir = file.path(dir, "b"))
  expect_true(has_credentials())
  expect_equal(auth_vars$cached_credentials$access_token, "token_b")
})
