
#' @importFrom arrow schema
#' @importFrom purrr map
get_arrow_schema <- function(variables){
  schema <- map(variables, function(variable) {
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

  names <- map(variables, function(variable) variable$name)

  arrow::schema(set_names(schema, names))
}

#' @importFrom httr PUT stop_for_status
#' @importFrom curl curl_upload new_handle handle_setheaders
perform_resumable_upload <- function(file_path, temp_upload_url=NULL, proxy_url=NULL) {
  retry_count <- 0
  start_byte <- 0
  file_size <- base::file.info(file_path)$size
  chunk_size <- file_size
  headers <- c()

  if (!is.null(proxy_url)){
    headers <- get_authorization_header()
    temp_upload_url = str_interp("${proxy_url}?url=${utils::URLencode(temp_upload_url, reserved=TRUE, repeated=TRUE)}")
  }

  resumable_url <- initiate_resumable_upload(file_size, temp_upload_url, headers)

  con <- base::file(file_path, "rb")
  # on.exit(close(con))

  while(
    start_byte < file_size
    || start_byte == 0  # handle empty upload for start_byte == 0
  ) {
    end_byte <- min(start_byte + chunk_size - 1, file_size - 1)
    seek(con, where=start_byte, origin="start")

    tryCatch({
      res <- curl::curl_upload(
        file=con,
        verbose=FALSE,
        reuse=TRUE,
        url=resumable_url,
        httpheader=format_request_headers(c(
          headers,
          `Content-Length`=toString(end_byte - start_byte + 1),
          `Content-Range`=sprintf("bytes %s-%s/%s", start_byte, end_byte, file_size)
        )),
        followlocation=TRUE
      )

      if (res$status_code >= 300){
        stop(str_interp('Received status code ${res$status_code}'))
      }

      start_byte <- start_byte + chunk_size
      retry_count <- 0
    }, error=function(e) {
      print('the error is')
      print(e)

      if(retry_count > -1) {
        stop("A network error occurred. Upload failed after too many retries.")
      }
      retry_count <- retry_count + 1
      Sys.sleep(retry_count / 2)
      cat("A network error occurred. Retrying last chunk of resumable upload.\n")
      start_byte <- retry_partial_upload(file_size=file_size, resumable_url=resumable_url, headers=headers)
    })
  }
}

# see https://github.com/jeroen/curl/blob/master/R/handle.R#L81
format_request_headers <- function(x){
  names <- names(x)
  values <- as.character(unlist(x))
  postfix <- ifelse(grepl("^\\s+$", values), ";", paste(":", values))
  paste0(names, postfix)
}

#' @importFrom httr POST stop_for_status
initiate_resumable_upload <- function(size, temp_upload_url, headers, retry_count=0) {
  tryCatch({
    res <- httr::POST(temp_upload_url,
                add_headers(
                  `x-upload-content-length`=as.character(size),
                  `x-goog-resumable`="start",
                  headers
                )
    )

    if (status_code(res) >= 400){
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

#' @importFrom httr PUT stop_for_status
#' @importFrom stringr str_extract
retry_partial_upload <- function(retry_count=0, file_size, resumable_url, headers) {
  tryCatch({
    print(resumable_url)
    res <- httr::PUT(url=resumable_url,
               body="",
               add_headers(headers, c(`Content-Length`="0", `Content-Range`=sprintf("bytes */%s", file_size))))
    if(res$status_code == 404) {
      return(0)
    }

    print(res$status_code)
    print(res$headers)
    if(res$status_code %in% c(200, 201)) {
      return(file_size)
    } else if(res$status_code == 308) {
      range_header <- res$headers$Range
      if(!is.null(range_header)) {
        match <- stringr::str_extract(range_header, "bytes=0-(\\d+)", group=1)
        as.numeric(match) + 1
      } else {
        stop("An unknown error occurred. Please try again.")
      }
    } else {  # If GCS hasn't received any bytes, the header will be missing
      return(0)
    }
  }, error=function(e) {
    print('retry error')
    print(e)
    if(retry_count > 10) {
      stop(e)
    }
    Sys.sleep(retry_count / 10)
    retry_partial_upload(retry_count=retry_count + 1, file_size=file_size, resumable_url=resumable_url, headers=headers)
  })
}

#' @importFrom httr PUT upload_file stop_for_status
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
    res <- httr::PUT(url = temp_upload_url, body = prepared_upload, add_headers(headers))
    if (status_code(res) >= 400){
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
