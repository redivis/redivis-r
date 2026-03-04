perform_resumable_upload <- function(
  data,
  temp_upload_url = NULL,
  on_progress = NULL
) {
  retry_count <- 0
  start_byte <- 0
  file_size <- 0
  headers <- c()

  if (inherits(data, "connection")) {
    file_size <- file.info(get_conn_name(data))$size
  } else {
    file_size <- length(data)
    data <- rawConnection(data)
  }
  chunk_size <- file_size

  headers <- get_authorization_header()

  resumable_url <- initiate_resumable_upload(
    file_size,
    temp_upload_url,
    headers
  )

  # handle empty upload for start_byte == 0
  while (start_byte < file_size || start_byte == 0) {
    chunk_size <- min(file_size - start_byte, chunk_size)
    end_byte <- min(start_byte + chunk_size - 1, file_size - 1)

    tryCatch(
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
              on_progress(bytes_read)
            }
            readBin(data, raw(), n = n)
          },
          seekfunction = function(offset) {
            bytes_read <<- offset
            if (!is.null(on_progress)) {
              on_progress(bytes_read)
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
          `Content-Length` = toString(end_byte - start_byte + 1),
          `Content-Range` = sprintf(
            "bytes %s-%s/%s",
            start_byte,
            end_byte,
            file_size
          ),
          Authorization = headers[["Authorization"]]
        )

        res <- curl::curl_fetch_memory(resumable_url, handle = h)

        if (res$status_code >= 400) {
          stop(str_interp(
            'Received status code ${res$status_code}: ${rawToChar(res$content)}'
          ))
        }

        start_byte <- start_byte + chunk_size
        retry_count <- 0
      },
      error = function(e) {
        if (retry_count > 10) {
          abort_redivis_network_error(
            str_interp(
              "A network error occurred. Upload failed after too many retries."
            ),
            original_exception = e
          )
        }

        retry_count <<- retry_count + 1
        Sys.sleep(retry_count)
        cat("A network error occurred. Retrying resumable upload.\n")
        start_byte <<- retry_partial_upload(
          file_size = file_size,
          resumable_url = resumable_url,
          headers = headers
        )
        if (!is.null(on_progress)) {
          on_progress(bytes_read)
        }
        seek(data, where = start_byte, origin = "start")
      }
    )
  }
}


initiate_resumable_upload <- function(
  size,
  temp_upload_url,
  headers,
  retry_count = 0
) {
  tryCatch(
    {
      req <- httr2::request(temp_upload_url) |>
        httr2::req_method("POST") |>
        httr2::req_headers(
          `x-upload-content-length` = as.character(size),
          `x-goog-resumable` = "start",
          `content-length` = "0", # IMPORTANT: GCS requires a content-length header for the initiation request, even though the body is empty
          !!!as.list(headers)
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
      if (retry_count > 10) {
        abort_redivis_network_error(
          "A network error occurred. Upload failed after too many retries.",
          original_exception = e
        )
      }
      Sys.sleep(retry_count + 1)
      initiate_resumable_upload(
        size,
        temp_upload_url,
        headers,
        retry_count = retry_count + 1
      )
    }
  )
}

retry_partial_upload <- function(
  retry_count = 0,
  file_size,
  resumable_url,
  headers
) {
  tryCatch(
    {
      req <- httr2::request(resumable_url) |>
        httr2::req_method("PUT") |>
        httr2::req_headers(
          !!!as.list(headers),
          `Content-Length` = "0",
          `Content-Range` = sprintf("bytes */%s", file_size)
        ) |>
        httr2::req_body_raw(raw(0)) |>
        httr2::req_error(is_error = function(resp) FALSE)

      res <- httr2::req_perform(req)
      status <- httr2::resp_status(res)

      if (status == 404) {
        return(0)
      }

      if (status %in% c(200, 201)) {
        return(file_size)
      } else if (status == 308) {
        range_header <- httr2::resp_header(res, "Range")
        if (!is.null(range_header)) {
          match <- stringr::str_extract(
            range_header,
            "bytes=0-(\\d+)",
            group = 1
          )
          as.numeric(match) + 1
        } else {
          # If GCS hasn't received any bytes, the header will be missing
          return(0)
        }
      } else {
        return(0)
      }
    },
    error = function(e) {
      cat("A network error occurred when trying to resume an upload.\n")
      if (retry_count > 10) {
        stop(e)
      }
      Sys.sleep(retry_count + 1)
      retry_partial_upload(
        retry_count = retry_count + 1,
        file_size = file_size,
        resumable_url = resumable_url,
        headers = headers
      )
    }
  )
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

      headers <- get_authorization_header()

      req <- httr2::request(temp_upload_url) |>
        httr2::req_method("PUT") |>
        httr2::req_headers(!!!as.list(headers)) |>
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
      Sys.sleep(retry_count + 1)
      # Recursively call the function with incremented retry count
      perform_standard_upload(data, original_url, retry_count + 1, on_progress)
    }
  )
}
