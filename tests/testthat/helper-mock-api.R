# A minimal stand-in for the Redivis API, for tests that need no server or
# credentials. It runs in a background R process (the client blocks on its
# requests, so it can't be served from the test process), and implements just
# enough of the API to exercise the read paths (listRows, read sessions,
# exports, raw files), retries, and authentication, with knobs for injecting
# failures.
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
    state$export_file_count <- 1
    # Reject unauthenticated requests, other than listRows, which (like the
    # real API) can be read anonymously
    state$require_auth <- FALSE
    # Return 503 for the next N requests
    state$fail_503 <- 0
    # Queued responses for the next requests, as list(status, body). A NULL
    # status lets that request through to the normal handler.
    state$response_script <- list()
    # The next listRows response stops after this many rows
    state$truncate_rows_once <- 0
    # A column that listRows and read streams leave out
    state$omit_column <- NULL
    # Raw files by id, as list(content, size): their bytes are generated from
    # content (see raw_file_contents)
    state$raw_files <- list()
    # The Range header of each rawFiles request ("none" if absent)
    state$raw_file_ranges <- list()
    # The next rawFiles response stops after this many bytes
    state$truncate_raw_file_once <- 0
    state$requests <- list()
    state$oauth_scopes <- list()
  }
  reset_state()

  variables <- list(
    list(name = "id", type = "integer"),
    list(name = "name", type = "string"),
    list(name = "val", type = "float")
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
  # way, to compare against)
  raw_file_contents <- function(content, size) {
    set.seed(sum(utf8ToInt(content)))
    as.raw(sample.int(256, size, replace = TRUE) - 1L)
  }

  raw_file <- function(file, range) {
    data <- raw_file_contents(file$content, file$size)
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
    headers <- list("Content-Type" = "application/octet-stream")
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

  route <- function(req) {
    method <- req$REQUEST_METHOD
    path <- req$PATH_INFO
    query <- parse_query(req$QUERY_STRING)
    body_raw <- req$rook.input$read()
    body <- rawToChar(body_raw)
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
          raw_file_ranges = state$raw_file_ranges
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
      payload <- jsonlite::base64url_enc(charToRaw(
        '{"scope":"data.edit workflow.write"}'
      ))
      return(json(list(
        access_token = paste0("header.", sub("=+$", "", payload), ".signature"),
        refresh_token = "refresh-token",
        expires_at = as.numeric(Sys.time()) + 3600,
        expires_in = 3600
      )))
    }

    state$requests[[length(state$requests) + 1]] <- list(
      method = method,
      path = path,
      query = query,
      authorized = authorized
    )

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
        return(json(scripted$body, scripted$status))
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

    if (startsWith(path, "/api/v1/rawFiles/")) {
      file <- state$raw_files[[utils::URLdecode(sub("^/api/v1/rawFiles/", "", path))]]
      if (is.null(file)) {
        return(json(list(status = 404, error = "not_found"), 404))
      }
      return(raw_file(file, req$HTTP_RANGE))
    }

    num_rows <- state$num_rows

    if (method == "POST") {
      if (path == "/api/v1/queries") {
        return(json(query_properties()))
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
      return(json(list(results = variables, nextPageToken = NULL)))
    }

    if (endsWith(path, "/rows")) {
      names <- if (!is.null(query$selectedVariables)) {
        strsplit(query$selectedVariables, ",")[[1]]
      } else {
        all_names
      }
      names <- setdiff(names, unlist(state$omit_column))
      count <- min(as.numeric(query$maxResults %||% num_rows), num_rows)
      if (state$truncate_rows_once > 0) {
        count <- min(count, state$truncate_rows_once)
        state$truncate_rows_once <- 0
      }
      return(binary(arrow_stream(names, 0, count, sentinel = FALSE)))
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
  withr::local_envvar(
    REDIVIS_API_ENDPOINT = paste0(url, "/api/v1"),
    HOME = home,
    REDIVIS_API_TOKEN = NA,
    REDIVIS_DEFAULT_NOTEBOOK = NA,
    .local_envir = env
  )
  # Never open a real browser for an OAuth login
  withr::local_options(browser = function(...) invisible(NULL), .local_envir = env)

  previous_credentials <- auth_vars$cached_credentials
  auth_vars$cached_credentials <- if (authenticated) fake_credentials()
  withr::defer(auth_vars$cached_credentials <- previous_credentials, envir = env)

  testthat::local_mocked_bindings(
    retry_sleep = function(seconds) invisible(NULL),
    perform_oauth_login = function(...) {
      stop("an interactive OAuth login was triggered")
    },
    .env = env
  )

  list(
    set = function(...) mock_api_control("/__mock/state", body = list(...)),
    requests = function() mock_api_control("/__mock/requests")$requests,
    oauth_scopes = function() unlist(mock_api_control("/__mock/requests")$oauth_scopes),
    raw_file_ranges = function() {
      unlist(mock_api_control("/__mock/requests")$raw_file_ranges)
    },
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
