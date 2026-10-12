canonical_uri <- "/tables/demo.omop:abcd:v1_0.person:efgh"
workflow_uri <- "/workflows/owner.workflow:abcd/tables/person"

# Stub out the API, recording each request as "METHOD path". Fetching a table
# through its workflow returns a table inherited from a datasource.
local_requests <- function(env = parent.frame()) {
  requests <- new.env()
  requests$log <- character(0)
  local_mocked_bindings(
    make_request = function(method = "GET", path = "", ...) {
      requests$log <- c(requests$log, paste(toupper(method), path))
      if (path %in% c(workflow_uri, canonical_uri)) {
        list(
          kind = "table",
          name = "Person",
          scopedReference = "person:efgh",
          qualifiedReference = "demo.omop:abcd:v1_0.person:efgh",
          uri = canonical_uri,
          container = list(kind = "dataset")
        )
      } else {
        list()
      }
    },
    .env = env
  )
  requests
}

test_that("workflow tables are resolved within their workflow", {
  workflow <- redivis$workflow("owner.workflow:abcd")
  expect_equal(workflow$table("person")$uri, workflow_uri)
  expect_equal(
    workflow$table("Condition occurrence")$uri,
    "/workflows/owner.workflow:abcd/tables/Condition%20occurrence"
  )
  expect_equal(
    redivis$dataset("owner.dataset:abcd")$table("person")$uri,
    "/tables/owner.dataset:abcd.person"
  )
})

test_that("reads use the workflow-scoped URI", {
  requests <- local_requests()
  table <- redivis$workflow("owner.workflow:abcd")$table("person")
  table$exists()
  table$variable("person_id")$get()
  expect_equal(
    requests$log,
    c(paste("HEAD", workflow_uri), paste0("GET ", workflow_uri, "/variables/person_id"))
  )
})

test_that("writes fetch the table's canonical URI first", {
  workflow <- redivis$workflow("owner.workflow:abcd")

  requests <- local_requests()
  workflow$table("person")$update(description = "x")
  expect_equal(requests$log, c(paste("GET", workflow_uri), paste("PATCH", canonical_uri)))

  requests <- local_requests()
  workflow$table("person")$update_variables(list())
  expect_equal(
    requests$log,
    c(paste("GET", workflow_uri), paste0("PATCH ", canonical_uri, "/variables"))
  )

  requests <- local_requests()
  workflow$table("person")$variable("person_id")$update(label = "x")
  expect_equal(
    requests$log,
    c(paste("GET", workflow_uri), paste0("PATCH ", canonical_uri, "/variables/person_id"))
  )

  # Tables that already have their canonical URI aren't fetched again
  requests <- local_requests()
  table <- workflow$table("person")
  table$get()
  table$update(description = "x")
  table$update_variables(list())
  expect_equal(requests$log, c(
    paste("GET", workflow_uri),
    paste("PATCH", canonical_uri),
    paste0("PATCH ", canonical_uri, "/variables")
  ))
})
