# Reference parsing, URIs, and the behavior of class methods, with the API
# stubbed out.

# Stub out the API, recording each request, and answering with respond(method,
# path), or just the request's URI
local_api <- function(respond = NULL, env = parent.frame()) {
  requests <- new.env()
  requests$log <- character()
  requests$payloads <- list()
  local_mocked_bindings(
    make_request = function(method = "GET", path = "", payload = NULL, ...) {
      requests$log <- c(requests$log, paste(toupper(method), path))
      requests$payloads <- c(requests$payloads, list(payload))
      response <- if (!is.null(respond)) respond(toupper(method), path)
      response %||% list(uri = path)
    },
    poll_sleep = function(seconds) invisible(NULL),
    .env = env
  )
  requests
}

# --- References ----------------------------------------------------------------

test_that("references can have dotted version tags", {
  dataset <- Dataset$new("owner.ds:v1.0")
  expect_equal(dataset$name, "ds")
  expect_equal(dataset$version_tag, "v1_0")
  expect_equal(dataset$qualified_reference, "owner.ds:v1_0")

  table <- Table$new("owner.ds:v1.0.table")
  expect_equal(table$name, "table")
  expect_equal(table$dataset$qualified_reference, "owner.ds:v1_0")
  expect_equal(table$uri, "/tables/owner.ds:v1_0.table")
})

test_that("references can be scoped by a default user or organization", {
  withr::local_envvar(REDIVIS_DEFAULT_USER = "someone")
  expect_equal(Dataset$new("ds")$qualified_reference, "someone.ds")
  expect_equal(Workflow$new("wf")$qualified_reference, "someone.wf")

  withr::local_envvar(
    REDIVIS_DEFAULT_USER = NA,
    REDIVIS_DEFAULT_ORGANIZATION = "org"
  )
  expect_equal(Dataset$new("ds")$organization$name, "org")
})

test_that("tables are qualified by the default workflow or dataset", {
  withr::local_envvar(
    REDIVIS_DEFAULT_WORKFLOW = NA,
    REDIVIS_DEFAULT_DATASET = "owner.ds:v1.0"
  )
  table <- Table$new("table")
  expect_equal(table$qualified_reference, "owner.ds:v1_0.table")
  expect_equal(table$uri, "/tables/owner.ds:v1_0.table")

  withr::local_envvar(REDIVIS_DEFAULT_WORKFLOW = "owner.wf")
  table <- Table$new("table")
  expect_equal(table$qualified_reference, "owner.wf.table")
  expect_equal(table$uri, "/workflows/owner.wf/tables/table")
})

test_that("names are encoded in URIs", {
  table <- Table$new("owner.ds.table")
  expect_equal(
    table$variable("a/b")$uri,
    "/tables/owner.ds.table/variables/a%2Fb"
  )

  workflow <- Workflow$new("owner.wf")
  expect_equal(
    Parameter$new("p/q", workflow = workflow)$uri,
    "/workflows/owner.wf/parameters/p%2Fq"
  )
  expect_equal(
    Parameter$new("owner.wf.param")$uri,
    "/workflows/owner.wf/parameters/param"
  )

  requests <- local_api()
  User$new("me")$secret("a/b")$get_value()
  Dataset$new("ds", user = User$new("a/b"))$create()
  expect_equal(
    requests$log,
    c("GET /users/me/secrets/a%2Fb", "POST /users/a%2Fb/datasets")
  )
})

# --- Versions ------------------------------------------------------------------

test_that("the versions before the first and after the latest are NULL", {
  local_api(function(method, path) {
    list(uri = path, previousVersion = NULL, nextVersion = NULL)
  })
  dataset <- Dataset$new("owner.ds:v1.0")
  expect_null(dataset$previous_version())
  expect_null(dataset$next_version())
})

test_that("a dataset's previous version is the dataset at that version", {
  local_api(function(method, path) {
    list(uri = path, previousVersion = list(tag = "v0.9"))
  })
  previous <- Dataset$new("owner.ds:v1.0")$previous_version()
  expect_equal(previous$qualified_reference, "owner.ds:v0_9")
})

# --- Transforms and notebooks -------------------------------------------------

test_that("an unnamed transform is named when it's created", {
  transform <- Workflow$new("owner.wf")$transform()
  expect_null(transform$qualified_reference)
  expect_null(transform$uri)

  requests <- local_api(function(method, path) {
    list(
      name = "Transform 1",
      qualifiedReference = "owner.wf.transform_1",
      scopedReference = "transform_1",
      uri = "/transforms/owner.wf.transform_1"
    )
  })
  transform$create(source_table = "owner.ds.table")
  expect_false("name" %in% names(requests$payloads[[1]]))
  expect_equal(transform$uri, "/transforms/owner.wf.transform_1")
})

# Responses to a run and the polls that follow it
local_run <- function(final_status, env = parent.frame()) {
  responses <- list(
    list(uri = "/x", currentJob = list(id = "j2", status = "queued")),
    # Not yet reflecting the new run
    list(uri = "/x", lastRunJob = list(id = "j1", status = "completed")),
    list(
      uri = "/x",
      currentJob = list(id = "j2", status = "running"),
      lastRunJob = list(id = "j1", status = "completed")
    ),
    list(
      uri = "/x",
      lastRunJob = list(id = "j2", status = final_status, kind = "transform")
    )
  )
  requests <- local_api(
    function(method, path) responses[[length(requests$log)]],
    env = env
  )
  requests
}

test_that("a run waits for the job it started", {
  requests <- local_run("completed")
  Transform$new("owner.wf.x")$run()
  expect_length(requests$log, 4)

  requests <- local_run("completed")
  Notebook$new("owner.wf.x")$run()
  expect_length(requests$log, 4)
})

test_that("a cancelled run raises", {
  local_run("cancelled")
  expect_error(
    Transform$new("owner.wf.x")$run(),
    "cancelled",
    class = "redivis_job_error"
  )

  local_run("cancelled")
  expect_error(
    Notebook$new("owner.wf.x")$run(),
    "cancelled",
    class = "redivis_job_error"
  )
})

# --- Datasources ----------------------------------------------------------------

datasource <- function() {
  Datasource$new("owner.ds", workflow = Workflow$new("owner.wf"))
}

local_sampled_datasource <- function(env = parent.frame()) {
  local_api(
    function(method, path) {
      if (method == "GET") {
        list(
          uri = path,
          sourceDataset = list(qualifiedReference = "owner.ds:v1_0:sample")
        )
      }
    },
    env = env
  )
}

test_that("a datasource update only sends what changed", {
  requests <- local_api()
  datasource()$update(mapped_tables = list(list("a", "b")))
  expect_equal(requests$payloads[[1]], list(mappedTables = list(a = "b")))

  requests <- local_api()
  datasource()$update(source_workflow = "owner.other_wf")
  expect_equal(requests$payloads[[1]], list(sourceWorkflow = "owner.other_wf"))
})

test_that("a datasource's sample is kept when its version changes", {
  requests <- local_sampled_datasource()
  datasource()$update(version = "2")
  expect_equal(
    requests$payloads[[2]],
    list(sourceDataset = "owner.ds:v2:sample")
  )

  requests <- local_sampled_datasource()
  datasource()$update(sample = FALSE)
  expect_equal(requests$payloads[[2]], list(sourceDataset = "owner.ds:v1_0"))
})
