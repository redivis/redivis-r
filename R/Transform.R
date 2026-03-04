#' @include Table.R Workflow.R api_request.R
Transform <- R6::R6Class(
  "Transform",
  public = list(
    name = NULL,
    workflow = NULL,
    properties = NULL,
    qualified_reference = NULL,
    scoped_reference = NULL,
    uri = NULL,

    initialize = function(
      name = "",
      workflow = NULL,
      properties = list()
    ) {
      parent_reference <- ""
      parsed_name <- name
      parsed_workflow <- workflow
      if (is.null(parsed_workflow)) {
        split <- strsplit(name, "\\.")[[1]]
        if (length(split) == 3) {
          parsed_name <- split[[3]]
          parsed_workflow <- Workflow$new(
            name = paste(split[1:2], collapse = '.')
          )
        } else if (Sys.getenv("REDIVIS_DEFAULT_WORKFLOW") != "") {
          parsed_workflow <- Workflow$new(
            name = Sys.getenv("REDIVIS_DEFAULT_WORKFLOW")
          )
        } else if (name != "") {
          abort_redivis_value_error(
            "Invalid transform specifier, must be the fully qualified reference if no dataset or workflow is specified"
          )
        }
      }
      parent_reference <- str_interp("${parsed_workflow$qualified_reference}.")
      scoped_reference_val <- if (length(properties$scopedReference)) {
        properties$scopedReference
      } else {
        parsed_name
      }
      qualified_reference_val <- if (length(properties$qualifiedReference)) {
        properties$qualifiedReference
      } else {
        str_interp("${parent_reference}${parsed_name}")
      }
      self$name <- parsed_name
      self$workflow <- parsed_workflow
      self$qualified_reference <- qualified_reference_val
      self$scoped_reference <- scoped_reference_val
      self$uri <- str_interp(
        "/transforms/${URLencode(qualified_reference_val)}"
      )
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp("<Transform ${self$qualified_reference}>\n"))
      invisible(self)
    },

    exists = function() {
      tryCatch(
        {
          make_request(
            method = "HEAD",
            path = self$uri
          )
          TRUE
        },
        redivis_not_found_error = function(e) {
          FALSE
        }
      )
    },

    get = function() {
      self$properties <- make_request(path = self$uri)
      self$uri <- self$properties$uri
      self
    },

    update = function(name = NULL, source_table = NULL) {
      payload <- list()

      if (!is.null(name)) {
        payload$name <- name
      }

      if (!is.null(source_table)) {
        if (is(source_table, "Table")) {
          payload$sourceTable <- source_table$qualified_reference
        } else {
          payload$sourceTable <- source_table
        }
      }
      self$properties <- make_request(
        method = "PATCH",
        path = self$uri,
        payload = payload
      )
      self$uri <- self$properties$uri
      self
    },

    source_tables = function() {
      warning(
        "Deprecation warning: The source_tables() method has been renamed to referenced_tables(); please use this instead. This method will be removed in the future.",
        call. = FALSE
      )
      return(self$referenced_tables())
    },

    source_table = function() {
      self$get()
      Table$new(
        name = self$properties[["sourceTable"]],
        properties = self$properties[["sourceTable"]][["qualifiedReference"]]
      )
    },

    referenced_tables = function() {
      self$get()

      lapply(self$properties[["referencedTables"]], function(source_table) {
        Table$new(
          name = source_table[["qualifiedReference"]],
          properties = source_table
        )
      })
    },

    output_table = function() {
      self$get()
      output_table_properties <- self$properties[["outputTable"]]
      Table$new(
        name = output_table_properties[["qualifiedReference"]],
        properties = output_table_properties
      )
    },

    run = function(wait_for_finish = TRUE) {
      self$properties <- make_request(
        method = "POST",
        path = str_interp("${self$uri}/run")
      )
      self$uri <- self$properties$uri

      if (wait_for_finish) {
        repeat {
          Sys.sleep(2)
          self$get()

          current_job <- self$properties[["currentJob"]]
          if (is.null(current_job)) {
            current_job <- self$properties[["lastRunJob"]]
          }

          if (
            !is.null(current_job) &&
              current_job[["status"]] %in% c("completed", "failed", "cancelled")
          ) {
            if (current_job[["status"]] != "completed") {
              abort_redivis_job_error(
                message = current_job$errorMessage,
                kind = current_job$kind,
                status = current_job$status
              )
            }
            break
          }
        }
      }
      self
    },

    cancel = function() {
      self$properties <- make_request(
        method = "POST",
        path = str_interp("${self$uri}/cancel")
      )
      self$uri <- self$properties$uri
      self
    }
  )
)
