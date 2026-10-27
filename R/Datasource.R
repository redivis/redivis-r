#' @include Dataset.R Workflow.R api_request.R
Datasource <- R6::R6Class(
  "Datasource",
  public = list(
    source = NULL,
    workflow = NULL,
    properties = NULL,
    uri = NULL,
    initialize = function(source = "", workflow = NULL, properties = list()) {
      parsed_source <- source
      parsed_workflow <- workflow

      if (is.null(parsed_workflow)) {
        default_workflow <- get_config("default_workflow")
        if (!is.null(default_workflow)) {
          parsed_workflow <- Workflow$new(name = default_workflow)
        } else {
          abort_redivis_value_error(
            "A workflow must be specified in the Datasource constructor, or set as the default via redivis$config(default_workflow = ...) or the REDIVIS_DEFAULT_WORKFLOW environment variable"
          )
        }
      }

      if (inherits(source, "Dataset") || inherits(source, "Workflow")) {
        parsed_source <- source$qualified_reference
      }

      self$source <- parsed_source
      self$workflow <- parsed_workflow
      self$uri <- str_interp(
        "${parsed_workflow$uri}/dataSources/${parsed_source}"
      )
      self$properties <- properties
    },

    print = function(...) {
      cat(str_interp("<Datasource ${self$uri}>\n"))
      invisible(self)
    },

    get = function() {
      self$properties <- make_request(path = self$uri)
      self$uri <- self$properties$uri
      self
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

    create = function() {
      payload <- list()

      if (Dataset$new(name = self$source)$exists()) {
        payload$sourceDataset <- self$source
      } else {
        payload$sourceWorkflow <- self$source
      }

      self$properties <- make_request(
        method = "POST",
        path = str_interp("${self$workflow$uri}/dataSources"),
        payload = payload
      )
      self$uri <- self$properties[["uri"]]

      self
    },

    delete = function() {
      make_request(method = "DELETE", path = self$uri)
      invisible(NULL)
    },

    update = function(
      source_dataset = NULL,
      source_workflow = NULL,
      sample = NULL,
      version = NULL,
      mapped_tables = NULL
    ) {
      if (inherits(source_workflow, "Workflow")) {
        source_workflow <- source_workflow$qualified_reference
      }
      if (inherits(source_dataset, "Dataset")) {
        source_dataset <- source_dataset$qualified_reference
      }

      if (is.list(mapped_tables) && is.null(names(mapped_tables))) {
        mapped_tables_dict <- list()
        for (pair in mapped_tables) {
          source_table <- pair[[1]]
          dest_table <- pair[[2]]
          if (inherits(source_table, "Table")) {
            source_table <- source_table$scoped_reference
          }
          if (inherits(dest_table, "Table")) {
            dest_table <- dest_table$scoped_reference
          }
          mapped_tables_dict[[source_table]] <- dest_table
        }
        mapped_tables <- mapped_tables_dict
      }

      if (!is.null(sample) || !is.null(version)) {
        if (is.null(source_dataset) && is.null(source_workflow)) {
          self$get()
          sd <- self$properties[["sourceDataset"]]
          source_dataset <- if (!is.null(sd)) {
            sd[["qualifiedReference"]]
          } else {
            NULL
          }
        }

        if (is.null(source_dataset) || !is.null(source_workflow)) {
          if (!is.null(sample) && isTRUE(sample)) {
            abort_redivis_value_error(
              "Sampling is not applicable to datasources that reference a workflow"
            )
          }
          if (!is.null(version)) {
            abort_redivis_value_error(
              "Versions are not applicable to datasources that reference a workflow"
            )
          }
        } else {
          # Keep the existing sample setting, unless it's being changed
          is_sample <- if (!is.null(sample)) {
            isTRUE(sample)
          } else {
            grepl(":sample\\b", source_dataset)
          }
          source_dataset <- gsub(":sample\\b", "", source_dataset)

          if (!is.null(version)) {
            if (inherits(version, "Version")) {
              version <- version$tag
            }
            source_dataset <- Dataset$new(
              name = source_dataset,
              version = version
            )$qualified_reference
          }

          if (is_sample) {
            source_dataset <- paste0(source_dataset, ":sample")
          }
        }
      }

      # Only change what was specified
      payload <- list(
        sourceDataset = source_dataset,
        sourceWorkflow = source_workflow,
        mappedTables = mapped_tables
      )
      self$properties <- make_request(
        method = "PATCH",
        path = self$uri,
        payload = Filter(Negate(is.null), payload)
      )
      self$uri <- self$properties[["uri"]]

      self
    },

    source_dataset = function() {
      if (!("sourceDataset" %in% names(self$properties))) {
        self$get()
      }
      source_dataset <- self$properties$sourceDataset
      if (is.null(source_dataset)) {
        abort_redivis_value_error(
          "This datasource doesn't have a source dataset. Use the source_workflow() method instead."
        )
      }
      Dataset$new(
        name = source_dataset[["qualifiedReference"]],
        properties = source_dataset
      )
    },

    source_workflow = function() {
      if (!("sourceWorkflow" %in% names(self$properties))) {
        self$get()
      }
      source_workflow <- self$properties$sourceWorkflow
      if (is.null(source_workflow)) {
        abort_redivis_value_error(
          "This datasource doesn't have a source workflow. Use the source_dataset() method instead."
        )
      }
      Workflow$new(
        name = source_workflow[["qualifiedReference"]],
        properties = source_workflow
      )
    }
  )
)
