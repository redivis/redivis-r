#' @include Table.R Variable.R api_request.R
Upload <- setRefClass("Upload",
   fields = list(name="character", table="Table", properties="list"),
   methods = list(
     show = function(){
       print(str_interp("<Upload ${get_upload_uri(.self)}>"))
     },
    get = function(){
      .self$properties = make_request(path=get_upload_uri(.self))
      .self
    },
    exists = function(){
      res <- make_request(method="HEAD", path=get_upload_uri(.self), stop_on_error=FALSE)
      if (length(res$error)){
        if (res$status == 404){
          return(FALSE)
        } else {
          stop(str_interp("${res$error}: ${res$error_description}"))
        }
      } else {
        return(TRUE)
      }
    },

    delete = function(){
      make_request(method="DELETE", path=get_upload_uri(.self))
      invisible(NULL)
    },

    list_variables = function(max_results){
      variables <- make_paginated_request(
        path=str_interp("${get_upload_uri(.self)}/variables"),
        page_size=100,
        max_results=max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(name=variable_properties$name, upload=.self, properties=variable_properties)
      })
    },

    insert_rows = function(rows, update_schema=FALSE){
      if (is.character(rows)){
        rows <- jsonlite::fromJSON(rows)
      } else if (!is(rows, "data.frame")) {
        stop('Invalid type for "rows" argument. Must either be a data.frame or JSON string.')
      }

      make_request(
        method="POST",
        path=str_interp("${get_upload_uri(.self)}/rows"),
        payload=list(
          "rows"=rows,
          "updateSchema"=update_schema
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
      rename_on_conflict = FALSE,
      replace_on_conflict = FALSE,
      allow_jagged_rows = FALSE,
      if_not_exists = FALSE,
      remove_on_fail = FALSE,
      wait_for_finish = TRUE,
      raise_on_fail = TRUE,
      progress = TRUE
    ) {

      MAX_SIMPLE_UPLOAD_SIZE=2**20 # 1MB
      MIN_RESUMABLE_UPLOAD_SIZE=2**24 # 16MB

      temp_upload_id <- NULL

      ## If `content` is a file path (a string) then open it as a binary connection
      if (is.character(content) && file.exists(content)) {
        if (base::file.exists(content)){
          f <- base::file(content, open="rb", blocking=FALSE)
          on.exit(close(f))
          content <- f
        } else {
          if (nchar(content) < 200){
            stop(str_interp("No file found at path provided for `content` argument: ${content}"))
          } else {
            stop("No file found at path provided for `content` argument. To upload data as content, make sure that the `content` argument is a raw vector.")
          }
        }
      }

      ## Check if content is large enough to need a temporary upload.
      ## Here we check if content is a connection (a file-like object) and get its size;
      ## otherwise, we use the length() to determine the size of a raw vector.
      if (!is.null(content) &&
          (
            (inherits(content, "connection") &&
             file.exists(get_conn_name(content)) &&
             file.info(get_conn_name(content))$size > MAX_SIMPLE_UPLOAD_SIZE) ||
            (!inherits(content, "connection") && length(content) > MAX_SIMPLE_UPLOAD_SIZE)
          )
      ) {

        ## Determine the file size
        size <- if (inherits(content, "connection")) {
          file.info(get_conn_name(content))$size
        } else {
          length(content)
        }

        ## Request a temporary upload (assumes make_request is defined)
        res <- make_request(
          method = "POST",
          path = str_interp("${.self$table$uri}/tempUploads"),
          payload = list(
            tempUploads = list(list(size = size, name = .self$name, resumable = size >= MIN_RESUMABLE_UPLOAD_SIZE))
          )
        )
        temp_upload <- res$results[[1]]

        pbar_bytes <- NULL
        on_progress <- NULL
        if (progress) {
          ## Create a progress bar (using txtProgressBar from the utils package)
          pbar_bytes <- txtProgressBar(min = 0, max = size, style = 3)
          on_progress <- function(num_bytes){
            setTxtProgressBar(pbar_bytes, num_bytes)
          }
          on.exit(close(pbar_bytes))
        }

        if (isTRUE(temp_upload$resumable)) {
          perform_resumable_upload(
            data = content,
            on_progress = on_progress,
            temp_upload_url = temp_upload$url
          )
        } else {
          perform_standard_upload(
            data = content,
            temp_upload_url = temp_upload$url,
            on_progress = on_progress
          )
        }

        if (!is.null(on_progress)){
          on_progress(size)
        }

        temp_upload_id <- temp_upload$id
        content <- NULL
      }

      if (!is.null(schema) && type != "stream") {
        warning("The schema option is ignored for uploads that aren't of type `stream`")
      }

      exists <- .self$exists()

      if (if_not_exists && exists) {
        return(.self)
      }

      if (isTRUE(replace_on_conflict) && isTRUE(rename_on_conflict)) {
        stop("Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be TRUE.")
      }

      if (exists) {
        if (isTRUE(replace_on_conflict)) {
          .self$delete()
        } else if (!isTRUE(rename_on_conflict)) {
          stop(sprintf("An upload with the name %s already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=TRUE or replace_on_conflict=TRUE.", .self$name))
        }
      }

      files <- NULL
      payload <- list(
        name = .self$name,
        type = type,
        schema = schema,
        metadata = metadata,
        hasHeaderRow = has_header_row,
        skipBadRecords = skip_bad_records,
        hasQuotedNewlines = has_quoted_newlines,
        allowJaggedRows = allow_jagged_rows,
        quoteCharacter = quote_character,
        escapeCharacter = escape_character,
        delimiter = delimiter,
        tempUploadId = temp_upload_id,
        transferSpecification = transfer_specification
      )

      if (!is.null(content)) {
        data <- NULL
        if (inherits(content, "connection")){
          data <- httr::upload_file(get_conn_name(content))
        } else {
          data <- curl::form_data(content)
        }
        # HTTR doesn't seem to provided a way to do multipart file uploads without everything first existing as a file on disk
        # See: https://stackoverflow.com/questions/31080363/how-to-post-multipart-related-content-with-httr-for-google-drive-api
        metadata_file <- tempfile()
        writeLines(jsonlite::toJSON(payload, auto_unbox = TRUE, null="null"), metadata_file)
        on.exit(unlink(metadata_file))

        files <- list(metadata = httr::upload_file(metadata_file, type="application/json"), data=data)
        payload <- NULL
      }

      response <- make_request(
        method = "POST",
        path = paste0(.self$table$uri, "/uploads"),
        payload = payload,
        files = files
      )

      .self$properties <- response

      ## Wait for the upload to finish if required
      tryCatch({
        if ((!is.null(content) || !is.null(temp_upload_id) || !is.null(transfer_specification)) && wait_for_finish) {
          repeat {
            Sys.sleep(2)
            .self$get()
            status <- .self$properties$status
            if (status == "completed" || status == "failed") {
              if (status == "failed" && raise_on_fail) {
                stop(.self$properties$errorMessage)
              }
              break
            } else {
              # message("Upload is still in progress...")
            }
          }
        }
      }, error = function(e) {
        if (remove_on_fail) {
          .self$delete()
        }
        stop(e)
      })

      return(.self)
    }
  )
)

get_upload_uri = function(upload){
  if (!is.null(upload$properties$uri)){
    return(upload$properties$uri)
  }
  escaped_name = gsub('\\.', '_', upload$name)
  str_interp("${upload$table$uri}/uploads/${URLencode(escaped_name, reserved=TRUE)}")
}

