# NB: temp upload URLs are signed, so they need no other credentials. They must
# never be sent the user's Redivis token, since they're proxied to Google Cloud
# Storage.

MAX_UPLOAD_RETRIES <- 10

perform_resumable_upload <- function(
  data,
  temp_upload_url = NULL,
  on_progress = NULL
) {
  retry_count <- 0
  start_byte <- 0

  if (inherits(data, "connection")) {
    file_size <- file.info(get_conn_name(data))$size
  } else {
    file_size <- length(data)
    data <- rawConnection(data)
    on.exit(close(data), add = TRUE)
  }

  resumable_url <- initiate_resumable_upload(file_size, temp_upload_url)

  repeat {
    chunk_size <- file_size - start_byte

    start_byte <- tryCatch(
      {
        # See curl::curl_upload https://github.com/jeroen/curl/blob/master/R/upload.R#L17
        bytes_read <- 0
        h <- curl::new_handle(
          upload = TRUE,
          filetime = FALSE,
          readfunction = function(n) {
            if (bytes_read + n > chunk_size) {
              n <- chunk_size - bytes_read
            }
            bytes_read <<- bytes_read + n
            if (!is.null(on_progress)) {
              on_progress(start_byte + bytes_read)
            }
            readBin(data, raw(), n = n)
          },
          seekfunction = function(offset) {
            bytes_read <<- offset
            if (!is.null(on_progress)) {
              on_progress(start_byte + bytes_read)
            }
            seek(data, where = start_byte + offset)
          },
          forbid_reuse = FALSE,
          verbose = FALSE,
          infilesize_large = chunk_size,
          followlocation = TRUE
        )

        curl::handle_setheaders(
          h,
          `Content-Length` = format(chunk_size, scientific = FALSE),
          # An empty upload has no byte range to send
          `Content-Range` = if (file_size > 0) {
            sprintf(
              "bytes %s-%s/%s",
              format(start_byte, scientific = FALSE),
              format(file_size - 1, scientific = FALSE),
              format(file_size, scientific = FALSE)
            )
          } else {
            "bytes */0"
          }
        )

        res <- curl::curl_fetch_memory(resumable_url, handle = h)

        if (res$status_code >= 400) {
          stop(str_interp(
            'Received status code ${res$status_code}: ${rawToChar(res$content)}'
          ))
        }

        if (res$status_code == 308) {
          # GCS only persisted part of the data; send the rest
          get_committed_bytes(curl::parse_headers_list(res$headers)[["range"]])
        } else {
          file_size
        }
      },
      error = function(e) {
        if (retry_count >= MAX_UPLOAD_RETRIES) {
          abort_redivis_network_error(
            str_interp(
              "A network error occurred. Upload failed after too many retries."
            ),
            original_exception = e
          )
        }

        retry_count <<- retry_count + 1
        retry_sleep(retry_count)
        cat("A network error occurred. Retrying resumable upload.\n")
        committed_bytes <- retry_partial_upload(
          file_size = file_size,
          resumable_url = resumable_url
        )
        if (is.null(committed_bytes)) {
          # The upload session expired, so start a new one
          resumable_url <<- initiate_resumable_upload(
            file_size,
            temp_upload_url
          )
          committed_bytes <- 0
        }
        committed_bytes
      }
    )

    if (!is.null(on_progress)) {
      on_progress(start_byte)
    }
    if (start_byte >= file_size) {
      break
    }
    seek(data, where = start_byte, origin = "start")
  }
}


initiate_resumable_upload <- function(
  size,
  temp_upload_url,
  retry_count = 0
) {
  tryCatch(
    {
      req <- httr2::request(temp_upload_url) |>
        httr2::req_method("POST") |>
        httr2::req_headers(
          `x-upload-content-length` = format(size, scientific = FALSE),
          `x-goog-resumable` = "start",
          `content-length` = "0" # IMPORTANT: GCS requires a content-length header for the initiation request, even though the body is empty
        ) |>
        httr2::req_error(is_error = function(resp) FALSE)

      res <- httr2::req_perform(req)

      if (httr2::resp_status(res) >= 400) {
        stop(sprintf(
          "Upload initiation failed with status %s",
          httr2::resp_status(res)
        ))
      }

      httr2::resp_header(res, "location")
    },
    error = function(e) {
      if (retry_count >= MAX_UPLOAD_RETRIES) {
        abort_redivis_network_error(
          "A network error occurred. Upload failed after too many retries.",
          original_exception = e
        )
      }
      retry_sleep(retry_count + 1)
      initiate_resumable_upload(
        size,
        temp_upload_url,
        retry_count = retry_count + 1
      )
    }
  )
}

# Asks GCS how much of a resumable upload it has received. Returns the byte to
# resume from (the file size if the upload is complete), or NULL if the upload
# session no longer exists, and a new one must be started.
retry_partial_upload <- function(
  retry_count = 0,
  file_size,
  resumable_url
) {
  tryCatch(
    {
      req <- httr2::request(resumable_url) |>
        httr2::req_method("PUT") |>
        httr2::req_headers(
          `Content-Length` = "0",
          `Content-Range` = sprintf(
            "bytes */%s",
            format(file_size, scientific = FALSE)
          )
        ) |>
        httr2::req_body_raw(raw(0)) |>
        httr2::req_error(is_error = function(resp) FALSE)

      res <- httr2::req_perform(req)
      status <- httr2::resp_status(res)

      if (status %in% c(404, 410)) {
        NULL
      } else if (status %in% c(200, 201)) {
        file_size
      } else if (status == 308) {
        get_committed_bytes(httr2::resp_header(res, "Range"))
      } else {
        0
      }
    },
    error = function(e) {
      cat("A network error occurred when trying to resume an upload.\n")
      if (retry_count >= MAX_UPLOAD_RETRIES) {
        stop(e)
      }
      retry_sleep(retry_count + 1)
      retry_partial_upload(
        retry_count = retry_count + 1,
        file_size = file_size,
        resumable_url = resumable_url
      )
    }
  )
}

# The number of bytes GCS has persisted, from the Range header of its 308
# response to an upload
get_committed_bytes <- function(range_header) {
  # If GCS hasn't received any bytes, the header will be missing
  if (is.null(range_header)) {
    return(0)
  }
  last_byte <- stringr::str_match(range_header, "bytes=0-(\\d+)")[, 2]
  if (is.na(last_byte)) {
    abort_redivis_error("An unknown error occurred. Please try again.")
  }
  as.numeric(last_byte) + 1
}

perform_standard_upload <- function(
  data,
  temp_upload_url = NULL,
  retry_count = 0,
  on_progress = NULL
) {
  original_url <- temp_upload_url
  tryCatch(
    {
      body <- if (
        inherits(data, "connection") && file.exists(get_conn_name(data))
      ) {
        readBin(get_conn_name(data), "raw", file.info(get_conn_name(data))$size)
      } else if (is.raw(data)) {
        data
      } else {
        charToRaw(as.character(data))
      }

      req <- httr2::request(temp_upload_url) |>
        httr2::req_method("PUT") |>
        httr2::req_body_raw(body) |>
        httr2::req_error(is_error = function(resp) FALSE)

      res <- httr2::req_perform(req)
      if (httr2::resp_status(res) >= 400) {
        stop(sprintf(
          "Upload failed with status %s",
          httr2::resp_status(res)
        ))
      }
    },
    error = function(e) {
      if (retry_count > 20) {
        cat("A network error occurred. Upload failed after too many retries.\n")
        stop(e)
      }
      retry_sleep(retry_count + 1)
      # Recursively call the function with incremented retry count
      perform_standard_upload(data, original_url, retry_count + 1, on_progress)
    }
  )
}
