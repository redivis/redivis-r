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

test_that("references without an owner are rejected", {
  withr::local_envvar(
    REDIVIS_DEFAULT_USER = NA,
    REDIVIS_DEFAULT_ORGANIZATION = NA,
    REDIVIS_DEFAULT_WORKFLOW = NA,
    REDIVIS_DEFAULT_DATASET = NA
  )
  expect_error(Dataset$new("ds"), class = "redivis_value_error")
  expect_error(Workflow$new("wf"), class = "redivis_value_error")
  expect_error(Table$new("table"), class = "redivis_value_error")
  expect_error(Parameter$new("param"), class = "redivis_value_error")
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

test_that("a version's tag is the dotted form, while references use v1_0", {
  for (tag in c("v1.0", "v1_0", "1.0")) {
    version <- Dataset$new("owner.ds")$version(tag)
    expect_equal(version$tag, "v1.0")
    expect_equal(version$uri, "/datasets/owner.ds/versions/v1_0")
    expect_equal(version$dataset()$qualified_reference, "owner.ds:v1_0")
  }
  expect_equal(Dataset$new("owner.ds:v1_0")$version()$tag, "v1.0")
  expect_equal(Dataset$new("owner.ds")$version("next")$tag, "next")
  expect_output(
    print(Dataset$new("owner.ds:v1_0")$version()),
    "<Version owner.ds:v1_0>",
    fixed = TRUE
  )
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

test_that("updates send their fields as the API names them", {
  requests <- local_api(function(method, path) {
    list(uri = "/datasets/owner.ds", qualifiedReference = "owner.ds", name = "ds")
  })
  dataset <- Dataset$new("owner.ds")
  dataset$properties$owner <- list(kind = "user")
  dataset$update(public_access_level = "overview")
  dataset$release(release_notes = "Notes")
  Table$new("owner.ds.t")$variable("v")$update(
    value_labels = list(list(value = "a", label = "A"))
  )

  payload_of <- function(request) {
    requests$payloads[[match(request, requests$log)]]
  }
  expect_equal(
    payload_of("PATCH /datasets/owner.ds"),
    list(publicAccessLevel = "overview")
  )
  expect_equal(
    payload_of("POST /datasets/owner.ds/versions/next/release"),
    list(releaseNotes = "Notes")
  )
  expect_equal(
    payload_of("PATCH /tables/owner.ds.t/variables/v"),
    list(valueLabels = list(list(value = "a", label = "A")))
  )
})

test_that("releases, unreleases, and deletions wait for finalization when asked", {
  polls <- 0
  requests <- local_api(function(method, path) {
    if (grepl("/versions/", path)) {
      # The version is still finalizing in the response to the request, and the
      # first poll after it
      finalizing <- method != "GET" || polls < 1
      if (method == "GET") polls <<- polls + 1
      return(list(
        tag = "v1.0",
        uri = "/datasets/owner.ds/versions/v1_0",
        datasetUri = "/datasets/owner.ds:v1_0",
        isFinalizing = finalizing
      ))
    }
    list(uri = path, qualifiedReference = "owner.ds:v1_0", name = "ds")
  })
  version_polls <- function() {
    sum(requests$log == "GET /datasets/owner.ds/versions/v1_0")
  }

  Dataset$new("owner.ds")$release(wait_for_finalization = TRUE)
  expect_equal(version_polls(), 2)

  polls <- 0
  Dataset$new("owner.ds:v1_0")$unrelease(wait_for_finalization = TRUE)
  expect_equal(version_polls(), 4)
  expect_true(
    "POST /datasets/owner.ds:v1_0/versions/current/unrelease" %in% requests$log
  )

  polls <- 0
  version <- Dataset$new("owner.ds")$version("v1_0")
  version$delete(wait_for_finalization = TRUE)
  expect_false(version$properties$isFinalizing)
  expect_equal(version_polls(), 6)

  # By default, they return as soon as the request has been made
  Dataset$new("owner.ds")$release()
  expect_equal(version_polls(), 6)
})

test_that("a workflow is created by its owner", {
  workflow_properties <- function(owner_kind) {
    function(method, path) {
      if (method == "HEAD") {
        abort_redivis_not_found_error(description = "Not found")
      }
      list(
        uri = "/workflows/owner.wf",
        qualifiedReference = "owner.wf",
        scopedReference = "wf",
        name = "wf",
        owner = list(kind = owner_kind, name = "owner")
      )
    }
  }

  requests <- local_api(workflow_properties("user"))
  workflow <- Workflow$new("owner.wf")$create(description = "A workflow")
  expect_equal(
    requests$log,
    c("HEAD /organizations/owner/datasets", "POST /users/owner/workflows")
  )
  expect_equal(
    requests$payloads[[2]],
    list(name = "wf", isPublic = FALSE, description = "A workflow")
  )
  expect_null(workflow$organization)
  expect_equal(workflow$qualified_reference, "owner.wf")

  requests <- local_api(workflow_properties("organization"))
  workflow <- redivis$organization("owner")$workflow("wf")$create(is_public = TRUE)
  expect_equal(requests$log, "POST /organizations/owner/workflows")
  expect_equal(requests$payloads[[1]], list(name = "wf", isPublic = TRUE))
  expect_null(workflow$user)
})

test_that("workflows, transforms, and datasources can be deleted", {
  requests <- local_api()
  workflow <- Workflow$new("owner.wf")

  expect_null(workflow$datasource("owner.ds")$delete())
  expect_null(Transform$new("owner.wf.t")$delete())
  expect_null(workflow$delete())

  expect_equal(
    requests$log,
    c(
      "DELETE /workflows/owner.wf/dataSources/owner.ds",
      "DELETE /transforms/owner.wf.t",
      "DELETE /workflows/owner.wf"
    )
  )
})
