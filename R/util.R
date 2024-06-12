
get_arrow_schema <- function(variables){
  schema <- purrr::map(variables, function(variable) {
    if (variable$type == 'integer'){
      arrow::int64()
    } else if (variable$type == 'float'){
      arrow::float64()
    } else if (variable$type == 'boolean'){
      arrow::boolean()
    } else if (variable$type == 'date'){
      arrow::date32()
    } else if (variable$type == 'dateTime'){
      arrow::timestamp(unit="us", timezone="")
    }
    else if (variable$type == 'time'){
      arrow::time64(unit="us")
    }
    else {
      arrow::string()
    }
  })

  names <- purrr::map(variables, function(variable) variable$name)

  arrow::schema(purrr::set_names(schema, names))
}

get_filename_from_content_disposition <- function(s) {
  fname <- stringr::str_match(s, "filename\\*=([^;]+)")[,2]
  if (is.na(fname)) {
    fname <- stringr::str_match(s, "filename=([^;]+)")[,2]
  }
  if (grepl("utf-8''", fname, ignore.case = TRUE)) {
    fname <- URLdecode(sub("utf-8''", '', fname, ignore.case = TRUE))
  }
  fname <- stringr::str_trim(fname)
  fname <- stringr::str_replace_all(fname, '"', '')
  return(fname)
}

perform_resumable_upload <- function(file_path, temp_upload_url=NULL, proxy_url=NULL) {
  retry_count <- 0
  start_byte <- 0
  file_size <- base::file.info(file_path)$size
  chunk_size <- file_size # 2^26 # ~67MB, must be less than 100MB
  headers <- c()

  if (!is.null(proxy_url)){
    headers <- get_authorization_header()
    temp_upload_url = str_interp("${proxy_url}?url=${utils::URLencode(temp_upload_url, reserved=TRUE, repeated=TRUE)}")
  }

  resumable_url <- initiate_resumable_upload(file_size, temp_upload_url, headers)

  con <- base::file(file_path, "rb")
  on.exit(close(con))

  while(
    start_byte < file_size
    || start_byte == 0  # handle empty upload for start_byte == 0
  ) {
    chunk_size <<- min(file_size - start_byte, chunk_size)
    end_byte <- min(start_byte + chunk_size - 1, file_size - 1)

    tryCatch({
      # See curl::curl_upload https://github.com/jeroen/curl/blob/master/R/upload.R#L17
      bytes_read <- 0
      h <- curl::new_handle(
        upload = TRUE,
        filetime = FALSE,
        readfunction = function(n) {
          if (bytes_read + n > chunk_size){
            n <- chunk_size - bytes_read
          }
          bytes_read <<- bytes_read + n
          readBin(con, raw(), n = n)
        },
        seekfunction = function(offset){
          bytes_read <<- offset
          seek(con, where = start_byte + offset)
        },
        forbid_reuse = FALSE,
        verbose = FALSE,
        infilesize_large = chunk_size,
        followlocation=TRUE,
        ssl_verifypeer=0L
      )

      curl::handle_setheaders(h,
                        `Content-Length`=toString(end_byte - start_byte + 1),
                        `Content-Range`=sprintf("bytes %s-%s/%s", start_byte, end_byte, file_size),
                        Authorization=headers[["Authorization"]]
                        )

      res <- curl::curl_fetch_memory(resumable_url, handle = h)

      if (res$status_code >= 400){
        stop(str_interp('Received status code ${res$status_code}: ${rawToChar(res$content)}'))
      }

      start_byte <- start_byte + chunk_size
      retry_count <- 0
    }, error=function(e) {
      if(retry_count > -1) {
        stop(str_interp("A network error occurred. Upload failed after too many retries. Error: ${e}"))
      }

      retry_count <<- retry_count + 1
      Sys.sleep(retry_count / 2)
      cat("A network error occurred. Retrying resumable upload.\n")
      start_byte <<- retry_partial_upload(file_size=file_size, resumable_url=resumable_url, headers=headers)
      seek(con, where=start_byte, origin="start")
    })
  }
}


initiate_resumable_upload <- function(size, temp_upload_url, headers, retry_count=0) {
  tryCatch({
    res <- httr::POST(temp_upload_url,
                httr::add_headers(
                  `x-upload-content-length`=as.character(size),
                  `x-goog-resumable`="start",
                  headers
                )
    )

    if (httr::status_code(res) >= 400){
      # stop_for_status also fails for redirects
      httr::stop_for_status(res)
    }

    res$headers$location
  }, error=function(e) {
    if(retry_count > 10) {
      stop("A network error occurred. Upload failed after too many retries.")
    }
    Sys.sleep(retry_count / 2)
    initiate_resumable_upload(size, temp_upload_url, headers, retry_count=retry_count+1)
  })
}

retry_partial_upload <- function(retry_count=0, file_size, resumable_url, headers) {
  tryCatch({
    res <- httr::PUT(url=resumable_url,
               body="",
               httr::add_headers(headers, c(`Content-Length`="0", `Content-Range`=sprintf("bytes */%s", file_size))))
    if(res$status_code == 404) {
      return(0)
    }

    if(res$status_code %in% c(200, 201)) {
      return(file_size)
    } else if(res$status_code == 308) {
      range_header <- res$headers$Range
      if(!is.null(range_header)) {
        match <- stringr::str_extract(range_header, "bytes=0-(\\d+)", group=1)
        as.numeric(match) + 1
      } else {
        # If GCS hasn't received any bytes, the header will be missing
        return(0)
      }
    } else {
      return(0)
    }
  }, error=function(e) {
    cat("A network error occurred when trying to resume an upload.\n")
    if(retry_count > 10) {
      stop(e)
    }
    Sys.sleep(retry_count / 10)
    retry_partial_upload(retry_count=retry_count + 1, file_size=file_size, resumable_url=resumable_url, headers=headers)
  })
}

perform_standard_upload <- function(file_path, temp_upload_url=NULL, proxy_url=NULL, retry_count=0, progressbar=NULL) {
  original_url=temp_upload_url
  tryCatch({
    prepared_upload <- httr::upload_file(file_path, type = NULL)

    headers = c()

    if (!is.null(proxy_url)){
      headers <- get_authorization_header()
      temp_upload_url = str_interp("${proxy_url}?url=${utils::URLencode(temp_upload_url, reserved=TRUE, repeated=TRUE)}")
    }

    # Perform the HTTP PUT request
    res <- httr::PUT(url = temp_upload_url, body = prepared_upload, httr::add_headers(headers))
    if (httr::status_code(res) >= 400){
      # stop_for_status also fails for redirects
      httr::stop_for_status(res)
    }
  }, error = function(e) {
    if (retry_count > 20) {
      cat("A network error occurred. Upload failed after too many retries.\n")
      stop(e)
    }
    Sys.sleep((retry_count + 1) / 2)
    # Recursively call the function with incremented retry count
    perform_standard_upload(file_path, original_url, proxy_url, retry_count + 1, progressbar)
  })
}
