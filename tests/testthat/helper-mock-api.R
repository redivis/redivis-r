# A minimal stand-in for the Redivis API, for tests that need no server or
# credentials. It runs in a background R process (the client blocks on its
# requests, so it can't be served from the test process), and implements just
# enough of the API to exercise the read paths (listRows, read sessions,
# exports, raw files), imports, retries, and authentication, with knobs for
# injecting failures.
#
# Tests call local_mock_api(), which points the client at the mock with a clean
# state, and returns helpers for adjusting that state and inspecting requests.

mock_api_server <- function(port) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
  state <- new.env()

  reset_state <- function() {
    rm(list = ls(state), envir = state)
    # The table's contents and metadata
    state$num_rows <- 500
    state$num_bytes <- 1000
    state$container_kind <- "table"
    state$upload_status <- "completed"
    state$query_bytes <- 1000
    state$query_status <- "completed"
    # Whether POST /queries honors listRows, responding with the rows of a
    # small, completed query (as a newer API does)
    state$query_inline_rows <- TRUE
    # The next rows POST /queries responds with stop after this many rows, and
    # without the end-of-stream sentinel
    state$truncate_inline_rows_once <- 0
    state$export_file_count <- 1
    # Reject unauthenticated requests, other than listRows, which (like the
    # real API) can be read anonymously
    state$require_auth <- FALSE
    # Return 503 for the next N requests
    state$fail_503 <- 0
    # Queued responses for the next requests, as list(status, body, headers).
    # A NULL status lets that request through to the normal handler.
    state$response_script <- list()
    # The next listRows response stops after this many rows
    state$truncate_rows_once <- 0
    # A column that listRows and read streams leave out
    state$omit_column <- NULL
    # Raw files by id, as list(content, size, gzipped): their bytes are
    # generated from content (see raw_file_contents)
    state$raw_files <- list()
    # The Range header of each rawFiles request ("none" if absent)
    state$raw_file_ranges <- list()
    # The next rawFiles response stops after this many bytes
    state$truncate_raw_file_once <- 0
    # rawFiles responses ignore the Range header, sending the whole file
    state$ignore_raw_file_range <- FALSE
    # The resumable upload sessions started (see /storage/ below), and whether
    # the first has expired
    state$storage_sessions <- 0
    state$expire_first_storage_session <- FALSE
    # Whether a HEAD request finds an upload (GETs always do)
    state$uploads_exist <- TRUE
    # Imports by id, and the ids of the idempotency keys they were created with
    state$imports <- list()
    state$import_keys <- list()
    # The name imports are given, as when the API renames one on a conflict
    state$import_name <- NULL
    # The statuses that polling a running import returns in turn, ending in
    # "completed" once they run out. A failed import has import_error_message.
    state$import_statuses <- list()
    state$import_error_message <- "Bad data"
    # The files uploaded to imports, as "importId/name" -> list(size, md5)
    state$import_files <- list()
    # Upload sessions by id, as list(import, name, size, md5, received, dead)
    state$upload_sessions <- list()
    # Opening a session again hands back the same one, even once it's dead
    state$reopen_dead_sessions <- FALSE
    # The next chunk sent fails with a 503, without committing anything
    state$fail_chunk_once <- FALSE
    # The next chunk sent only has this many of its bytes committed
    state$partial_commit_once <- 0
    # The next chunk sent finds its session dead
    state$kill_session_once <- FALSE
    # Chunks that broke the protocol's rules, as descriptions
    state$chunk_violations <- list()
    state$requests <- list()
    state$oauth_scopes <- list()
    # How long the tokens that refreshing a login returns are valid for
    state$token_lifetime <- 3600
    # The refreshes of a login, each as the number of requests made before it
    state$token_refreshes <- list()
  }
  reset_state()

  variables <- list(
    list(name = "id", type = "integer", label = "Row id"),
    list(name = "name", type = "string"),
    list(name = "val", type = "float", label = "Half the id")
  )
  # Only returned when the variables list is requested with includeValueLabels
  value_labels <- list(
    id = list(
      list(value = "0", label = "first"),
      list(value = "1", label = "second")
    ),
    name = list(list(value = "row-0", label = "The first row"))
  )
  all_names <- c("id", "name", "val")

  make_batch <- function(names, start, n) {
    ids <- if (n > 0) seq(start, length.out = n) else numeric()
    columns <- list(
      id = arrow::Array$create(ids, type = arrow::int64()),
      name = arrow::Array$create(
        if (n > 0) paste0("row-", ids) else character(),
        type = arrow::utf8()
      ),
      val = arrow::Array$create(ids / 2, type = arrow::float64())
    )
    do.call(arrow::record_batch, columns[names])
  }

  arrow_stream <- function(names, start, count, sentinel) {
    sink <- arrow::BufferOutputStream$create()
    writer <- arrow::RecordBatchStreamWriter$create(
      sink,
      make_batch(names, 0, 0)$schema
    )
    batch_start <- start
    while (batch_start < start + count) {
      n <- min(100, start + count - batch_start)
      writer$write_batch(make_batch(names, batch_start, n))
      batch_start <- batch_start + n
    }
    if (sentinel) {
      # The empty end-of-stream batch that readStreams emit with eosSentinel=true
      writer$write_batch(make_batch(names, 0, 0))
    }
    writer$close()
    as.raw(sink$finish())
  }

  parquet_file <- function(start, count) {
    sink <- arrow::BufferOutputStream$create()
    arrow::write_parquet(
      arrow::arrow_table(make_batch(all_names, start, count)),
      sink
    )
    as.raw(sink$finish())
  }

  # Random bytes, the same for the same content (tests generate them the same
  # way, to compare against), gzip-compressed for a gzipped file
  raw_file_contents <- function(content, size, gzipped = FALSE) {
    set.seed(sum(utf8ToInt(content)))
    bytes <- as.raw(sample.int(256, size, replace = TRUE) - 1L)
    if (!isTRUE(gzipped)) {
      return(bytes)
    }
    path <- tempfile()
    on.exit(unlink(path))
    con <- gzfile(path, "wb")
    writeBin(bytes, con)
    close(con)
    readBin(path, "raw", file.size(path))
  }

  raw_file <- function(file, range) {
    data <- raw_file_contents(file$content, file$size, file$gzipped)
    state$raw_file_ranges[[length(state$raw_file_ranges) + 1]] <- range %||% "none"
    start <- 0
    end <- length(data) - 1
    if (!is.null(range)) {
      bounds <- regmatches(range, regexec("^bytes=([0-9]+)-([0-9]*)$", range))[[1]]
      start <- as.numeric(bounds[2])
      if (nzchar(bounds[3])) {
        end <- min(as.numeric(bounds[3]), end)
      }
    }
    body <- data[seq_len(end - start + 1) + start]
    if (state$truncate_raw_file_once > 0) {
      body <- body[seq_len(min(length(body), state$truncate_raw_file_once))]
      state$truncate_raw_file_once <- 0
    }
    headers <- list(
      "Content-Type" = "application/octet-stream",
      "Accept-Ranges" = "bytes"
    )
    if (!is.null(range)) {
      headers[["Content-Range"]] <- sprintf(
        "bytes %.0f-%.0f/%d",
        start,
        start + length(body) - 1,
        length(data)
      )
    }
    list(status = if (is.null(range)) 200L else 206L, headers = headers, body = body)
  }

  parse_query <- function(query_string) {
    query_string <- sub("^\\?", "", query_string %||% "")
    if (query_string == "") {
      return(list())
    }
    pairs <- strsplit(strsplit(query_string, "&", fixed = TRUE)[[1]], "=")
    stats::setNames(
      lapply(pairs, function(p) utils::URLdecode(p[2] %||% "")),
      vapply(pairs, function(p) utils::URLdecode(p[1]), character(1))
    )
  }

  json <- function(body, status = 200L) {
    list(
      status = as.integer(status),
      headers = list("Content-Type" = "application/json"),
      body = as.character(jsonlite::toJSON(
        body,
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      ))
    )
  }

  binary <- function(body, content_type = "application/vnd.apache.arrow.stream") {
    list(status = 200L, headers = list("Content-Type" = content_type), body = body)
  }

  query_properties <- function() {
    list(
      id = "q1",
      kind = "query",
      uri = "/queries/q1",
      status = state$query_status,
      errorMessage = if (state$query_status == "failed") "Syntax error",
      outputNumBytes = state$query_bytes,
      outputNumRows = state$num_rows,
      finishedAt = 1758800000123
    )
  }

  export_properties <- function() {
    list(
      id = "e1",
      kind = "export",
      uri = "/exports/e1",
      status = "completed",
      fileCount = state$export_file_count,
      format = "parquet",
      size = length(parquet_file(0, state$num_rows))
    )
  }

  md5_base64 <- function(bytes) {
    openssl::base64_encode(as.raw(openssl::md5(bytes)))
  }

  decode_file_name <- function(encoded) {
    padded <- chartr("-_", "+/", encoded)
    padded <- paste0(padded, strrep("=", (4 - nchar(padded) %% 4) %% 4))
    rawToChar(openssl::base64_decode(padded))
  }

  # The response to a chunk, or a query of a session's status: complete, or
  # how much has been committed so far
  upload_session_status <- function(upload_id) {
    session <- state$upload_sessions[[upload_id]]
    committed <- length(session$received)
    if (committed >= session$size) {
      if (!identical(md5_base64(session$received), session$md5)) {
        return(json(list(status = 400, error = "bad_digest"), 400))
      }
      state$import_files[[paste0(session$import, "/", session$name)]] <- list(
        size = committed,
        md5 = session$md5
      )
      return(json(list(ok = TRUE)))
    }
    headers <- list()
    if (committed > 0) {
      headers$Range <- sprintf("bytes=0-%.0f", committed - 1)
    }
    list(status = 308L, headers = headers, body = "")
  }

  import_route <- function(method, path, query, body, body_raw, req) {
    if (grepl("^/api/v1/tables/[^/]+/imports$", path) && method == "POST") {
      request <- jsonlite::fromJSON(body, simplifyVector = FALSE)
      id <- state$import_keys[[request$idempotencyKey]]
      if (is.null(id)) {
        id <- paste0("i", length(state$imports) + 1)
        state$import_keys[[request$idempotencyKey]] <- id
        state$imports[[id]] <- list(
          kind = "import",
          id = id,
          uri = paste0("/imports/", id),
          name = state$import_name %||% request$name %||% "imported",
          # Remote sources are transferred right away
          status = if (identical(request$sourceType %||% "local", "local")) {
            "queued"
          } else {
            "running"
          },
          type = request$type,
          totalFileCount = request$totalFileCount,
          transferredFileCount = 0
        )
      }
      return(json(state$imports[[id]]))
    }

    parts <- strsplit(sub("^/api/v1/imports/", "", path), "/", fixed = TRUE)[[1]]
    id <- parts[[1]]
    import <- state$imports[[id]]
    if (is.null(import)) {
      return(json(list(status = 404, error = "not_found"), 404))
    }

    if (length(parts) == 1 && method == "GET") {
      if (import$status == "running") {
        status <- "completed"
        if (length(state$import_statuses) > 0) {
          status <- state$import_statuses[[1]]
          state$import_statuses <- state$import_statuses[-1]
        }
        import$status <- status
        if (status == "failed") {
          import$errorMessage <- state$import_error_message
        }
        state$imports[[id]] <- import
      }
      return(json(import))
    }

    if (length(parts) == 2 && parts[[2]] == "finalize" && method == "POST") {
      if (import$status == "queued") {
        import$status <- "running"
        state$imports[[id]] <- import
      }
      return(json(import))
    }

    if (length(parts) >= 3 && parts[[2]] == "files") {
      name <- decode_file_name(parts[[3]])

      if (length(parts) == 3 && method == "POST") {
        if (!identical(req$HTTP_CONTENT_MD5, md5_base64(body_raw))) {
          return(list(
            status = 400L,
            headers = list("Content-Type" = "application/xml"),
            body = "<Error><Code>BadDigest</Code></Error>"
          ))
        }
        state$import_files[[paste0(id, "/", name)]] <- list(
          size = length(body_raw),
          md5 = req$HTTP_CONTENT_MD5
        )
        return(json(list(ok = TRUE)))
      }

      if (length(parts) == 4 && parts[[4]] == "uploadSession") {
        if (method == "POST") {
          request <- jsonlite::fromJSON(body, simplifyVector = FALSE)
          # Asking again for the same file returns its session, while it's
          # alive
          upload_id <- NULL
          for (session_id in names(state$upload_sessions)) {
            session <- state$upload_sessions[[session_id]]
            if (
              session$import == id &&
                session$name == name &&
                session$size == request$size &&
                session$md5 == request$md5Hash &&
                (!session$dead || isTRUE(state$reopen_dead_sessions))
            ) {
              upload_id <- session_id
            }
          }
          if (is.null(upload_id)) {
            upload_id <- paste0("s", length(state$upload_sessions) + 1)
            state$upload_sessions[[upload_id]] <- list(
              import = id,
              name = name,
              size = request$size,
              md5 = request$md5Hash,
              received = raw(0),
              dead = FALSE
            )
          }
          return(json(list(
            kind = "importUploadSession",
            name = name,
            size = request$size,
            uri = sprintf(
              "/imports/%s/files/%s/uploadSession?upload_id=%s",
              id,
              parts[[3]],
              upload_id
            )
          )))
        }

        if (method == "PUT") {
          upload_id <- query$upload_id
          session <- state$upload_sessions[[upload_id]]
          if (is.null(session) || isTRUE(session$dead)) {
            return(json(list(status = 410, error = "gone"), 410))
          }
          if (isTRUE(state$kill_session_once)) {
            state$kill_session_once <- FALSE
            state$upload_sessions[[upload_id]]$dead <- TRUE
            return(json(list(status = 410, error = "gone"), 410))
          }
          content_range <- req$HTTP_CONTENT_RANGE %||% ""
          if (!startsWith(content_range, "bytes */")) {
            if (isTRUE(state$fail_chunk_once)) {
              state$fail_chunk_once <- FALSE
              return(json(list(status = 503, error = "unavailable"), 503))
            }
            bounds <- as.numeric(regmatches(
              content_range,
              regexec("^bytes ([0-9]+)-([0-9]+)/([0-9]+)$", content_range)
            )[[1]][-1])
            committed <- length(session$received)
            is_last <- bounds[[2]] == session$size - 1
            if (bounds[[1]] != committed) {
              state$chunk_violations[[length(state$chunk_violations) + 1]] <-
                sprintf("chunk at %.0f, after %.0f committed", bounds[[1]], committed)
            } else if (length(body_raw) != bounds[[2]] - bounds[[1]] + 1) {
              state$chunk_violations[[length(state$chunk_violations) + 1]] <-
                "chunk's length doesn't match its range"
            } else if (!is_last && length(body_raw) %% 262144 != 0) {
              state$chunk_violations[[length(state$chunk_violations) + 1]] <-
                sprintf("chunk of %d bytes isn't a multiple of 256KiB", length(body_raw))
            } else {
              keep <- length(body_raw)
              if (state$partial_commit_once > 0) {
                keep <- min(keep, state$partial_commit_once)
                state$partial_commit_once <- 0
              }
              state$upload_sessions[[upload_id]]$received <- c(
                session$received,
                body_raw[seq_len(keep)]
              )
            }
          }
          return(upload_session_status(upload_id))
        }
      }
    }

    json(list(status = 404, error = "not_found"), 404)
  }

  route <- function(req) {
    method <- req$REQUEST_METHOD
    path <- req$PATH_INFO
    query <- parse_query(req$QUERY_STRING)
    body_raw <- req$rook.input$read()
    # Uploaded files' bodies are binary, and aren't read as text
    body <- tryCatch(rawToChar(body_raw), error = function(e) "")
    authorized <- !is.null(req$HTTP_AUTHORIZATION)

    # Control endpoints for tests
    if (startsWith(path, "/__mock/")) {
      if (path == "/__mock/ping") {
        return(json(list(ok = TRUE)))
      }
      if (path == "/__mock/reset") {
        reset_state()
        return(json(list(ok = TRUE)))
      }
      if (path == "/__mock/state") {
        updates <- jsonlite::fromJSON(body, simplifyVector = FALSE)
        for (name in names(updates)) {
          state[[name]] <- updates[[name]]
        }
        return(json(list(ok = TRUE)))
      }
      if (path == "/__mock/requests") {
        return(json(list(
          requests = state$requests,
          oauth_scopes = state$oauth_scopes,
          raw_file_ranges = state$raw_file_ranges,
          token_refreshes = state$token_refreshes
        )))
      }
      if (path == "/__mock/imports") {
        return(json(list(
          imports = state$imports,
          files = state$import_files,
          chunk_violations = state$chunk_violations
        )))
      }
    }

    # A stand-in for the OAuth device flow, which the client reaches via the
    # API endpoint's origin
    if (path == "/oauth/device_authorization") {
      request <- jsonlite::fromJSON(body, simplifyVector = FALSE)
      state$oauth_scopes[[length(state$oauth_scopes) + 1]] <- request$scope
      return(json(list(
        verification_uri_complete = "https://example.test/verify",
        device_code = "device-code",
        interval = 0
      )))
    }
    if (path == "/oauth/token") {
      state$token_refreshes[[length(state$token_refreshes) + 1]] <-
        length(state$requests)
      payload <- jsonlite::base64url_enc(charToRaw(
        '{"scope":"data.edit workflow.write"}'
      ))
      return(json(list(
        access_token = paste0("header.", sub("=+$", "", payload), ".signature"),
        refresh_token = "refresh-token",
        expires_at = as.numeric(Sys.time()) + state$token_lifetime,
        expires_in = state$token_lifetime
      )))
    }

    content_type <- req$CONTENT_TYPE %||% req$HTTP_CONTENT_TYPE %||% ""
    state$requests[[length(state$requests) + 1]] <- list(
      method = method,
      path = path,
      query = query,
      authorized = authorized,
      # The payloads of JSON requests
      payload = if (startsWith(content_type, "application/json")) {
        jsonlite::fromJSON(body, simplifyVector = FALSE)
      },
      content_range = req$HTTP_CONTENT_RANGE,
      content_md5 = req$HTTP_CONTENT_MD5
    )

    # A stand-in for the signed URLs that temp uploads are sent to. A resumable
    # upload first starts a session, and is then sent to the session's URL.
    if (startsWith(path, "/storage/")) {
      if (method == "POST") {
        state$storage_sessions <- state$storage_sessions + 1
        return(list(
          status = 201L,
          headers = list(
            Location = sprintf(
              "http://%s/storage/session-%d",
              req$HTTP_HOST,
              state$storage_sessions
            )
          ),
          body = ""
        ))
      }
      if (
        isTRUE(state$expire_first_storage_session) &&
          path == "/storage/session-1"
      ) {
        return(json(list(error = "not_found"), 404))
      }
      return(json(list(ok = TRUE, bytes = length(body_raw))))
    }

    if (state$fail_503 > 0) {
      state$fail_503 <- state$fail_503 - 1
      return(json(
        list(status = 503, error = "unavailable", error_description = "busy"),
        503
      ))
    }

    if (length(state$response_script) > 0) {
      scripted <- state$response_script[[1]]
      state$response_script <- state$response_script[-1]
      if (!is.null(scripted$status)) {
        res <- json(scripted$body, scripted$status)
        res$headers <- c(res$headers, scripted$headers)
        return(res)
      }
    } else if (
      isTRUE(state$require_auth) && !authorized && !endsWith(path, "/rows")
    ) {
      return(json(
        list(
          status = 401,
          error = "invalid_token",
          error_description = "No credentials were provided."
        ),
        401
      ))
    }

    if (grepl("^/api/v1/(tables/[^/]+/imports$|imports/)", path)) {
      return(import_route(method, path, query, body, body_raw, req))
    }

    if (startsWith(path, "/api/v1/rawFiles/")) {
      file <- state$raw_files[[utils::URLdecode(sub("^/api/v1/rawFiles/", "", path))]]
      if (is.null(file)) {
        return(json(list(status = 404, error = "not_found"), 404))
      }
      return(raw_file(
        file,
        if (!isTRUE(state$ignore_raw_file_range)) req$HTTP_RANGE
      ))
    }

    num_rows <- state$num_rows

    if (method == "POST") {
      if (path == "/api/v1/queries") {
        list_rows <- jsonlite::fromJSON(body, simplifyVector = FALSE)$listRows
        count <- min(list_rows$maxResults %||% num_rows, num_rows)
        # Like the API, only small results are returned in place of the query,
        # sized by the fraction of rows requested
        is_small <- num_rows == 0 || state$query_bytes * count / num_rows <= 1e8
        if (
          is.null(list_rows) ||
            !isTRUE(state$query_inline_rows) ||
            state$query_status != "completed" ||
            !is_small
        ) {
          res <- json(query_properties())
          res$headers$Location <- "/api/v1/queries/q1"
          return(res)
        }
        sentinel <- isTRUE(list_rows$eosSentinel)
        if (state$truncate_inline_rows_once > 0) {
          count <- min(count, state$truncate_inline_rows_once)
          sentinel <- FALSE
          state$truncate_inline_rows_once <- 0
        }
        # Like BigQuery, selected columns are matched case-insensitively, and
        # come in the table's order
        names <- if (!is.null(list_rows$selectedVariables)) {
          selected <- tolower(unlist(list_rows$selectedVariables))
          all_names[tolower(all_names) %in% selected]
        } else {
          all_names
        }
        res <- binary(arrow_stream(names, 0, count, sentinel = sentinel))
        res$headers$Location <- "/api/v1/queries/q1"
        res$headers[["Content-Location"]] <- "/api/v1/queries/q1/rows"
        return(res)
      }
      if (endsWith(path, "/readSessions")) {
        request <- jsonlite::fromJSON(body, simplifyVector = FALSE)
        if (!is.null(request$maxResults)) {
          num_rows <- min(num_rows, request$maxResults)
        }
        count <- max(1, min(request$requestedStreamCount %||% 2, 2))
        sizes <- if (count == 1) {
          num_rows
        } else {
          c(num_rows %/% 2, num_rows - num_rows %/% 2)
        }
        state$session_sizes <- sizes
        return(json(list(
          numRows = num_rows,
          streams = lapply(seq_len(count), function(i) {
            list(id = paste0("s", i - 1), estimatedRows = sizes[[i]])
          })
        )))
      }
      if (endsWith(path, "/exports")) {
        return(json(export_properties()))
      }
      return(json(list(ok = TRUE, bytes = length(body_raw))))
    }

    if (method == "PATCH") {
      return(json(list(ok = TRUE)))
    }

    if (endsWith(path, "/variables")) {
      results <- variables
      if (identical(query$includeValueLabels, "true")) {
        results <- lapply(results, function(v) {
          v$valueLabels <- value_labels[[v$name]]
          v
        })
      }
      return(json(list(results = results, nextPageToken = NULL)))
    }

    if (endsWith(path, "/rows")) {
      names <- if (!is.null(query$selectedVariables)) {
        strsplit(query$selectedVariables, ",")[[1]]
      } else {
        all_names
      }
      names <- setdiff(names, unlist(state$omit_column))
      count <- min(as.numeric(query$maxResults %||% num_rows), num_rows)
      # Only queries' rows support eosSentinel, which a truncated response
      # lacks
      sentinel <- identical(query$eosSentinel, "true")
      if (state$truncate_rows_once > 0) {
        count <- min(count, state$truncate_rows_once)
        sentinel <- FALSE
        state$truncate_rows_once <- 0
      }
      return(binary(arrow_stream(names, 0, count, sentinel = sentinel)))
    }

    if (startsWith(path, "/api/v1/readStreams/")) {
      # Stream sN covers the rows after streams s0..s(N-1)
      sizes <- state$session_sizes %||% num_rows
      index <- as.integer(sub(".*/s", "", path))
      start <- sum(sizes[seq_len(index)])
      offset <- as.numeric(query$offset %||% 0)
      return(binary(arrow_stream(
        setdiff(all_names, unlist(state$omit_column)),
        start + offset,
        sizes[[index + 1]] - offset,
        sentinel = identical(query$eosSentinel, "true")
      )))
    }

    if (path == "/api/v1/exports/e1") {
      return(json(export_properties()))
    }
    if (path == "/api/v1/exports/e1/download") {
      part <- as.integer(query$filePart %||% 0)
      parts <- state$export_file_count
      per_part <- ceiling(num_rows / parts)
      start <- part * per_part
      return(binary(
        parquet_file(start, max(0, min(per_part, num_rows - start))),
        "application/octet-stream"
      ))
    }

    if (path == "/api/v1/queries/q1") {
      return(json(query_properties()))
    }

    if (grepl("/uploads/", path, fixed = TRUE)) {
      if (method == "HEAD" && !isTRUE(state$uploads_exist)) {
        return(json(list(status = 404, error = "not_found"), 404))
      }
      completed <- state$upload_status == "completed"
      return(json(list(
        uri = "/tables/a.b.mock/uploads/u1",
        name = "My Upload",
        status = state$upload_status,
        numRows = num_rows,
        numBytes = if (completed) state$num_bytes
      )))
    }

    if (path == "/api/v1/tables/a.b.mock") {
      return(json(list(
        kind = "table",
        name = "mock",
        qualifiedReference = "a.b.mock",
        scopedReference = "mock",
        uri = "/tables/a.b.mock",
        numRows = num_rows,
        numBytes = state$num_bytes,
        container = list(kind = state$container_kind)
      )))
    }

    json(list(status = 404, error = "not_found"), 404)
  }

  handle <- function(req) {
    res <- route(req)
    # httpuv mishandles keep-alive after an httr2 HEAD request (which curl
    # sends as a custom request), so don't keep those connections open
    if (req$REQUEST_METHOD == "HEAD") {
      res$body <- ""
      res$headers[["Connection"]] <- "close"
    }
    res
  }

  httpuv::runServer("127.0.0.1", port, list(call = handle))
}

mock_api_env <- new.env()

start_mock_api <- function() {
  if (!is.null(mock_api_env$url) && mock_api_env$process$is_alive()) {
    return(mock_api_env$url)
  }
  port <- httpuv::randomPort()
  process <- callr::r_bg(mock_api_server, args = list(port = port))
  url <- paste0("http://127.0.0.1:", port)

  deadline <- Sys.time() + 30
  repeat {
    up <- tryCatch(
      curl::curl_fetch_memory(paste0(url, "/__mock/ping"))$status_code == 200,
      error = function(e) FALSE
    )
    if (up) {
      break
    }
    if (!process$is_alive() || Sys.time() > deadline) {
      stop("The mock API didn't start: ", paste(process$read_all_error_lines(), collapse = "\n"))
    }
    Sys.sleep(0.1)
  }

  mock_api_env$url <- url
  mock_api_env$process <- process
  withr::defer(process$kill(), envir = testthat::teardown_env())
  url
}

mock_api_control <- function(path, body = NULL) {
  handle <- curl::new_handle()
  if (!is.null(body)) {
    curl::handle_setopt(
      handle,
      postfields = as.character(jsonlite::toJSON(
        body,
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      ))
    )
  }
  res <- curl::curl_fetch_memory(paste0(mock_api_env$url, path), handle = handle)
  jsonlite::fromJSON(rawToChar(res$content), simplifyVector = FALSE)
}

# Credentials for the client to send. The token's payload holds the default
# scope, so the client never tries to upgrade it.
fake_credentials <- function() {
  payload <- sub(
    "=+$",
    "",
    jsonlite::base64url_enc(charToRaw('{"scope":"data.edit workflow.write"}'))
  )
  list(
    access_token = paste0("header.", payload, ".signature"),
    refresh_token = "refresh-token",
    expires_at = as.numeric(Sys.time()) + 3600,
    expires_in = 3600
  )
}

# Point the client at a freshly reset mock API, as an authenticated user (or,
# with authenticated = FALSE, with no credentials at all). Retries don't wait,
# and an unexpected interactive login fails the test.
local_mock_api <- function(authenticated = TRUE, env = parent.frame()) {
  testthat::skip_on_cran()
  testthat::skip_if_not_installed("callr")
  testthat::skip_if_not_installed("httpuv")

  url <- start_mock_api()
  mock_api_control("/__mock/reset", body = list())

  home <- withr::local_tempdir(.local_envir = env)
  # Credentials go to the temporary home, never the live tests' (see
  # setup-live.R)
  withr::local_envvar(
    REDIVIS_API_ENDPOINT = paste0(url, "/api/v1"),
    HOME = home,
    REDIVIS_CONFIG_DIR = NA,
    REDIVIS_API_TOKEN = NA,
    REDIVIS_DEFAULT_NOTEBOOK = NA,
    .local_envir = env
  )
  # Never open a real browser for an OAuth login
  withr::local_options(browser = function(...) invisible(NULL), .local_envir = env)

  previous_credentials <- auth_vars$cached_credentials
  previous_credentials_file <- auth_vars$cached_credentials_file
  auth_vars$cached_credentials <- if (authenticated) fake_credentials()
  auth_vars$cached_credentials_file <- NULL
  withr::defer(
    {
      auth_vars$cached_credentials <- previous_credentials
      auth_vars$cached_credentials_file <- previous_credentials_file
    },
    envir = env
  )

  testthat::local_mocked_bindings(
    retry_sleep = function(seconds) invisible(NULL),
    poll_sleep = function(seconds) invisible(NULL),
    perform_oauth_login = function(...) {
      stop("an interactive OAuth login was triggered")
    },
    .env = env
  )

  list(
    set = function(...) mock_api_control("/__mock/state", body = list(...)),
    requests = function() mock_api_control("/__mock/requests")$requests,
    oauth_scopes = function() unlist(mock_api_control("/__mock/requests")$oauth_scopes),
    token_refreshes = function() {
      as.integer(unlist(mock_api_control("/__mock/requests")$token_refreshes))
    },
    raw_file_ranges = function() {
      unlist(mock_api_control("/__mock/requests")$raw_file_ranges)
    },
    imports = function() mock_api_control("/__mock/imports"),
    paths = function(pattern = NULL) {
      paths <- vapply(
        mock_api_control("/__mock/requests")$requests,
        function(r) r$path,
        character(1)
      )
      if (is.null(pattern)) paths else paths[grepl(pattern, paths, fixed = TRUE)]
    },
    authorized = function() {
      vapply(
        mock_api_control("/__mock/requests")$requests,
        function(r) isTRUE(r$authorized),
        logical(1)
      )
    }
  )
}

mock_table <- function() {
  Table$new(name = "a.b.mock")
}
