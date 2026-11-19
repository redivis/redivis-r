#' @include Table.R Variable.R api_request.R TabularReader.R imports.R
Upload <- R6::R6Class(
  "Upload",
  inherit = TabularReader,
  public = list(
    name = NULL,
    table = NULL,

    initialize = function(name = "", properties = list(), table) {
      super$initialize()
      self$name <- name
      self$table <- table
      self$properties <- properties

      if (!is.null(properties$uri)) {
        self$uri <- properties$uri
      } else {
        self$uri <- upload_uri(table, name)
      }
    },

    print = function(...) {
      cat(str_interp(
        "<Upload ${self$table$qualified_reference}.${self$name}>\n"
      ))
      invisible(self)
    },

    get = function() {
      if (self$name == "" && is.null(self$properties$uri)) {
        abort_redivis_value_error(
          'Cannot get an upload without a specified name'
        )
      }
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

    delete = function() {
      make_request(method = "DELETE", path = self$uri)
      invisible(NULL)
    },

    variable = function(name) {
      Variable$new(name = name, upload = self)
    },

    list_variables = function(max_results = NULL) {
      variables <- make_paginated_request(
        path = str_interp("${self$uri}/variables"),
        page_size = 100,
        max_results = max_results
      )
      lapply(variables, function(variable_properties) {
        Variable$new(
          name = variable_properties$name,
          upload = self,
          properties = variable_properties
        )
      })
    },

    insert_rows = function(rows) {
      if (is.character(rows)) {
        rows <- jsonlite::fromJSON(rows)
      } else if (!is(rows, "data.frame")) {
        abort_redivis_value_error(
          'Invalid type for "rows" argument. Must either be a data.frame or JSON string.'
        )
      }

      make_request(
        method = "POST",
        path = str_interp("${self$uri}/rows"),
        payload = list(
          "rows" = rows
        )
      )
    },

    create = function(
      content = NULL,
      type = NULL,
      transfer_specification = NULL,
      delimiter = NULL,
      schema = NULL,
      metadata = NULL,
      has_header_row = TRUE,
      skip_bad_records = FALSE,
      has_quoted_newlines = NULL,
      quote_character = NULL,
      escape_character = NULL,
      null_markers = NULL,
      rename_on_conflict = FALSE,
      replace_on_conflict = FALSE,
      allow_jagged_rows = FALSE,
      if_not_exists = FALSE,
      remove_on_fail = FALSE,
      wait_for_finish = TRUE,
      raise_on_fail = TRUE,
      progress = NULL
    ) {
      progress <- resolve_config("progress", progress)

      if (isTRUE(replace_on_conflict) && isTRUE(rename_on_conflict)) {
        abort_redivis_value_error(
          "Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be TRUE."
        )
      }

      is_stream <- identical(type, "stream")
      if (is_stream && !is.null(content)) {
        abort_redivis_value_error(
          "Stream uploads can't be created with content. Create the upload, then add rows to it with insert_rows()."
        )
      }

      # The path of the file being uploaded, if content is one
      content_path <- NULL
      if (is.character(content) && base::file.exists(content)) {
        content_path <- content
      } else if (inherits(content, "connection")) {
        content_path <- get_conn_name(content)
      }

      if (self$name == "" && !is.null(content_path)) {
        self$name <- basename(content_path)
      }

      ## Check for a conflicting upload before uploading anything
      if (self$name != "") {
        if (is.null(self$properties$uri)) {
          self$uri <- upload_uri(self$table, self$name)
        }
        if (self$exists()) {
          if (if_not_exists) {
            return(self)
          } else if (isTRUE(replace_on_conflict)) {
            self$delete()
          } else if (!isTRUE(rename_on_conflict)) {
            abort_redivis_value_error(sprintf(
              "An upload with the name %s already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=TRUE or replace_on_conflict=TRUE.",
              self$name
            ))
          }
        }
      }

      ## Uploaded content is either a file, or a raw vector held in memory
      if (is.character(content)) {
        if (!base::file.exists(content)) {
          if (nchar(content) < 200) {
            abort_redivis_value_error(str_interp(
              "No file found at path provided for `content` argument: ${content}"
            ))
          } else {
            abort_redivis_value_error(
              "No file found at path provided for `content` argument. To upload data as content, make sure that the `content` argument is a raw vector."
            )
          }
        }
      } else if (inherits(content, "connection")) {
        content <- content_path
      } else if (!is.null(content) && !is.raw(content)) {
        content <- convert_data_to_parquet(content)
        on.exit(base::file.remove(content), add = TRUE)
        type <- "parquet"
      }

      if (!is.null(schema) && !is_stream) {
        warning(
          "The schema option is ignored for uploads that aren't of type `stream`",
          call. = FALSE
        )
      }

      ## Stream uploads, and uploads created without data, are created directly
      if (is_stream || (is.null(content) && is.null(transfer_specification))) {
        response <- make_request(
          method = "POST",
          path = paste0(self$table$uri, "/uploads"),
          payload = list(
            name = self$name,
            type = type,
            schema = schema,
            metadata = metadata,
            hasHeaderRow = has_header_row,
            skipBadRecords = skip_bad_records,
            hasQuotedNewlines = has_quoted_newlines,
            allowJaggedRows = allow_jagged_rows,
            quoteCharacter = quote_character,
            escapeCharacter = escape_character,
            nullMarkers = null_markers,
            delimiter = delimiter
          )
        )
        self$properties <- response
        self$uri <- self$properties$uri
        return(self)
      }

      ## Anything else is imported: from the content, or a transfer's source
      tabular_options <- list(
        type = type,
        metadata = metadata,
        hasHeaderRow = has_header_row,
        skipBadRecords = skip_bad_records,
        hasQuotedNewlines = has_quoted_newlines,
        allowJaggedRows = allow_jagged_rows,
        quoteCharacter = quote_character,
        escapeCharacter = escape_character,
        nullMarkers = null_markers,
        delimiter = delimiter
      )
      name <- if (self$name != "") self$name

      if (!is.null(transfer_specification)) {
        import <- create_import(
          self$table,
          c(
            list(
              name = name,
              sourceType = transfer_specification$sourceType,
              sourcePaths = list(transfer_specification$sourcePath),
              identity = transfer_specification$identity
            ),
            tabular_options
          )
        )
      } else {
        size <- import_source_size(content)
        md5 <- compute_md5_base64(content)
        import <- create_import(
          self$table,
          c(
            list(
              name = name,
              sourceType = "local",
              totalFileCount = 1,
              totalSize = size
            ),
            tabular_options
          )
        )

        on_progress <- NULL
        if (progress) {
          pbar_bytes <- txtProgressBar(min = 0, max = max(size, 1), style = 3)
          on_progress <- function(num_bytes) {
            setTxtProgressBar(pbar_bytes, num_bytes)
          }
          on.exit(close(pbar_bytes), add = TRUE)
        }

        file_name <- if (!is.null(content_path)) {
          basename(content_path)
        } else if (!is.null(import$name)) {
          import$name
        } else {
          str_interp("data.${type %||% 'csv'}")
        }
        upload_import_file(
          import,
          name = file_name,
          source = content,
          size = size,
          md5 = md5,
          on_progress = on_progress
        )
        import <- finalize_import(import)
      }

      # The API renames the upload if its name was taken. The upload is still
      # addressed by name, as any other is.
      private$import_uri <- import$uri
      self$name <- import$name %||% self$name
      self$uri <- upload_uri(self$table, self$name)
      self$properties <- import

      if (wait_for_finish) {
        import <- wait_for_import(import, raise_on_fail = FALSE)
        if (import$status %in% c("failed", "cancelled")) {
          self$properties <- import
          if (remove_on_fail) {
            tryCatch(self$delete(), redivis_not_found_error = function(e) NULL)
          }
          if (raise_on_fail) {
            abort_redivis_job_error(
              message = import$errorMessage,
              kind = import$kind,
              status = import$status
            )
          }
        } else {
          self$get()
        }
      }

      return(self)
    }
  ),
  private = list(
    # The import that created this upload, if it was created through one
    import_uri = NULL
  )
)

# The URI of a table's upload, by name
upload_uri <- function(table, name) {
  escaped_name <- gsub('\\.', '_', name)
  str_interp(
    "${table$uri}/uploads/${URLencode(escaped_name, reserved=TRUE)}"
  )
}
