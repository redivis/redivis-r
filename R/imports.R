#' @include api_request.R util.R
NULL

# Imports bring local files and remote sources into a table: an import is
# created, its files are uploaded to it (for local files), and it's finalized,
# after which the API processes it in the background. Upload$create() and
# Table$add_files() go through these helpers.

# Files up to this size are sent in a single request; larger ones are sent in
# chunks to a resumable upload session. These are variables, rather than inline
# constants, so the tests can lower them.
IMPORT_DIRECT_UPLOAD_MAX_BYTES <- 32 * 2^20
# A chunk's size adapts to the connection's speed, within these bounds, as a
# power of two MiB. Every chunk but the last must be a multiple of 256KiB.
IMPORT_CHUNK_START_BYTES <- 8 * 2^20
IMPORT_CHUNK_MIN_BYTES <- 2^20
IMPORT_CHUNK_MAX_BYTES <- 128 * 2^20
# How many times in a row a chunk can fail without any progress before the
# upload gives up
IMPORT_MAX_CHUNK_FAILURES <- 10
# How many times a file's session can be replaced. A session also dies when the
# file fails its md5 check on completion (e.g., it changed while it was
# uploaded), which uploading it again is unlikely to fix.
IMPORT_MAX_SESSION_REPLACEMENTS <- 3

# A session that was cancelled, expired, or otherwise ended answers a chunk or
# a status query with one of these
DEAD_UPLOAD_SESSION_STATUSES <- c(400, 404, 410, 499)

# Creates an import on a table, given the request's payload (in the API's
# camelCase). NULL fields are left out, rather than sent as null.
create_import <- function(table, payload) {
  # Writes must never go through a workflow-scoped table URI
  resolve_table_for_write(table)
  payload <- Filter(Negate(is.null), payload)
  # Generated once, so that retrying the request can't create a second import
  payload$idempotencyKey <- uuid4()
  make_request(
    method = "POST",
    path = str_interp("${table$uri}/imports"),
    payload = payload,
    idempotent = TRUE
  )
}

# Uploads a file to an import, as a raw vector or the path to a file. Its size
# and md5 (base64-encoded) are computed when not given. on_progress, if given,
# is called with the number of bytes uploaded so far.
upload_import_file <- function(
  import,
  name,
  source,
  size = NULL,
  md5 = NULL,
  on_progress = NULL
) {
  size <- size %||% import_source_size(source)
  md5 <- md5 %||% compute_md5_base64(source)
  file_path <- str_interp(
    "${import$uri}/files/${encode_import_file_name(name)}"
  )

  if (size <= IMPORT_DIRECT_UPLOAD_MAX_BYTES) {
    # Uploading the same name again replaces the file, so this is safe to retry
    make_request(
      method = "POST",
      path = file_path,
      body = read_import_source(source, 0, size),
      headers = list(`Content-MD5` = md5),
      parse_response = FALSE,
      idempotent = TRUE
    )
  } else {
    upload_import_file_in_chunks(file_path, source, size, md5, on_progress)
  }

  if (!is.null(on_progress)) {
    on_progress(size)
  }
  invisible(NULL)
}

# Sends a file to a resumable upload session, in chunks. The upload resumes
# from what the session has committed, after a failure or when the session was
# started before (e.g. by an earlier attempt).
upload_import_file_in_chunks <- function(
  file_path,
  source,
  size,
  md5,
  on_progress = NULL
) {
  # Asking for a session again with the same size and md5 returns the same one
  # while it's alive, and a new one once it isn't
  open_session <- function() {
    make_request(
      method = "POST",
      path = str_interp("${file_path}/uploadSession"),
      payload = list(size = size, md5Hash = md5),
      idempotent = TRUE
    )$uri
  }
  session_uri <- open_session()

  chunk_size <- min(
    max(IMPORT_CHUNK_START_BYTES, IMPORT_CHUNK_MIN_BYTES),
    IMPORT_CHUNK_MAX_BYTES
  )
  committed <- 0
  failures <- 0
  session_replacements <- 0
  # Start by asking the session what it already has
  needs_status_query <- TRUE

  repeat {
    sending <- !needs_status_query
    start <- committed
    body <- if (sending) {
      read_import_source(source, start, min(chunk_size, size - start))
    }
    started_at <- Sys.time()
    outcome <- tryCatch(
      put_import_chunk(
        # The session's URI is relative to the API's
        paste0(get_config("api_endpoint"), session_uri),
        size = size,
        start = start,
        body = body
      ),
      error = function(e) e
    )

    if (
      inherits(outcome, "error") ||
        outcome$status %in% c(408, 429) ||
        outcome$status >= 500
    ) {
      failures <- failures + 1
      if (failures > IMPORT_MAX_CHUNK_FAILURES) {
        abort_redivis_network_error(
          "A network error occurred. Upload failed after too many retries.",
          original_exception = if (inherits(outcome, "error")) {
            outcome
          } else {
            simpleError(str_interp("Received status code ${outcome$status}"))
          }
        )
      }
      retry_sleep(min(failures, 30))
      chunk_size <- max(IMPORT_CHUNK_MIN_BYTES, chunk_size / 2)
      needs_status_query <- TRUE
      next
    }

    if (outcome$status %in% DEAD_UPLOAD_SESSION_STATUSES) {
      new_session_uri <- open_session()
      if (identical(new_session_uri, session_uri)) {
        # The API only hands back a dead session when the import itself can no
        # longer accept files
        abort_redivis_error(str_interp(
          "The upload session for this file has ended (status ${outcome$status}), since the import was cancelled or expired. Imports must be finalized within 20 hours of being created."
        ))
      }
      session_replacements <- session_replacements + 1
      if (session_replacements > IMPORT_MAX_SESSION_REPLACEMENTS) {
        abort_redivis_error(str_interp(
          "The upload failed on ${IMPORT_MAX_SESSION_REPLACEMENTS + 1} upload sessions (the last ended with status ${outcome$status}). The file may have changed while it was uploaded."
        ))
      }
      # The new session starts from nothing
      session_uri <- new_session_uri
      committed <- 0
      failures <- 0
      needs_status_query <- TRUE
      next
    }

    if (!(outcome$status %in% c(200, 201, 308))) {
      raise_api_error(response_text = outcome$body, response = outcome$response)
    }

    # Uploads advance by what the session has committed, which can be less
    # than what was sent
    if (outcome$committed > committed) {
      failures <- 0
    } else if (sending) {
      failures <- failures + 1
      if (failures > IMPORT_MAX_CHUNK_FAILURES) {
        abort_redivis_network_error(
          "Upload failed, since the server stopped accepting its data."
        )
      }
    }
    committed <- outcome$committed
    needs_status_query <- FALSE
    if (!is.null(on_progress)) {
      on_progress(committed)
    }
    if (committed >= size) {
      break
    }

    if (sending) {
      elapsed <- as.numeric(Sys.time() - started_at, units = "secs")
      if (elapsed > 8) {
        chunk_size <- max(IMPORT_CHUNK_MIN_BYTES, chunk_size / 2)
      } else if (elapsed < 4) {
        chunk_size <- min(IMPORT_CHUNK_MAX_BYTES, chunk_size * 2)
      }
    }
  }
  invisible(NULL)
}

# Sends a chunk to an upload session, or without a body, asks how much of the
# file the session has. The session's URI is the credential, so the request
# must never carry the user's token.
put_import_chunk <- function(url, size, start = 0, body = NULL) {
  content_range <- if (is.null(body)) {
    sprintf("bytes */%s", format(size, scientific = FALSE))
  } else {
    sprintf(
      "bytes %s-%s/%s",
      format(start, scientific = FALSE),
      format(start + length(body) - 1, scientific = FALSE),
      format(size, scientific = FALSE)
    )
  }
  res <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_headers(
      `Content-Range` = content_range,
      `Content-Length` = format(length(body), scientific = FALSE)
    ) |>
    httr2::req_body_raw(body %||% raw(0), type = "application/octet-stream") |>
    httr2::req_timeout(3600) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_perform()

  status <- httr2::resp_status(res)
  list(
    status = status,
    response = res,
    committed = if (status %in% c(200, 201)) {
      size
    } else if (status == 308) {
      get_committed_bytes(httr2::resp_header(res, "Range"))
    },
    body = if (status >= 400) {
      tryCatch(httr2::resp_body_string(res), error = function(e) "")
    }
  )
}

finalize_import <- function(import) {
  make_request(
    method = "POST",
    path = str_interp("${import$uri}/finalize"),
    idempotent = TRUE
  )
}

# Polls an import until it completes, fails, or is cancelled, returning its
# final properties. A failed or cancelled import raises a job error, unless
# raise_on_fail is FALSE.
wait_for_import <- function(import, raise_on_fail = TRUE) {
  delay <- 2
  while (!(import$status %in% c("completed", "failed", "cancelled"))) {
    poll_sleep(delay)
    delay <- min(delay * 1.5, 30)
    import <- make_request(path = import$uri, idempotent = TRUE)
  }
  if (raise_on_fail && import$status %in% c("failed", "cancelled")) {
    abort_redivis_job_error(
      message = import$errorMessage,
      kind = import$kind,
      status = import$status
    )
  }
  import
}

# A file's name in an import's file URLs: the unpadded base64url of its UTF-8
# bytes
encode_import_file_name <- function(name) {
  encoded <- openssl::base64_encode(charToRaw(enc2utf8(name)))
  sub("=+$", "", chartr("+/", "-_", encoded))
}

# The base64-encoded md5 of a raw vector or a file. openssl hashes a file
# connection incrementally (in 512KiB reads), so it's never read into memory
# all at once.
compute_md5_base64 <- function(source) {
  digest <- if (is.raw(source)) {
    openssl::md5(source)
  } else {
    # An unopened connection, which openssl opens and closes. raw = TRUE hashes
    # the file's bytes as stored, which is what's uploaded: otherwise opening it
    # decompresses a gzip, bzip2, or xz file (e.g. a .csv.gz or .rds), and the
    # digest wouldn't match
    openssl::md5(base::file(source, raw = TRUE))
  }
  openssl::base64_encode(as.raw(digest))
}

import_source_size <- function(source) {
  if (is.raw(source)) length(source) else file.info(source)$size
}

# length bytes of a raw vector or a file, starting at offset
read_import_source <- function(source, offset, length) {
  if (is.raw(source)) {
    return(source[seq_len(length) + offset])
  }
  con <- base::file(source, open = "rb")
  on.exit(close(con), add = TRUE)
  if (offset > 0) {
    seek(con, where = offset, origin = "start")
  }
  readBin(con, raw(), n = length)
}

# A random (version 4) UUID
uuid4 <- function() {
  bytes <- openssl::rand_bytes(16)
  bytes[7] <- as.raw(bitwOr(bitwAnd(as.integer(bytes[7]), 0x0f), 0x40))
  bytes[9] <- as.raw(bitwOr(bitwAnd(as.integer(bytes[9]), 0x3f), 0x80))
  hex <- paste(as.character(bytes), collapse = "")
  paste(
    substring(hex, c(1, 9, 13, 17, 21), c(8, 12, 16, 20, 32)),
    collapse = "-"
  )
}
