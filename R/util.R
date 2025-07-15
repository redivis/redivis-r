
globals <- new.env(parent = emptyenv())
globals$printed_warnings <- list()

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

convert_data_to_parquet <- function(data){
  folder <- str_interp('/${get_temp_dir()}/parquet')

  if (!dir.exists(folder)) dir.create(folder, recursive = TRUE)

  temp_file_path <- str_interp('${folder}/${uuid::UUIDgenerate()}')

  if (is(data,"sf")){
    sf_column_name <- attr(data, "sf_column")
    wkt_geopoint <- sapply(sf::st_geometry(data), function(x) sf::st_as_text(x))
    sf::st_geometry(data) <- NULL
    data[sf_column_name] <- wkt_geopoint
  }

  if (is(data, "Dataset")){
    dir.create(temp_file_path)
    arrow::write_dataset(data, temp_file_path, format='parquet', max_partitions=1, basename_template="part-{i}.parquet")
    temp_file_path <- str_interp('${temp_file_path}/part-0.parquet')
  } else {
    arrow::write_parquet(data, sink=temp_file_path)
  }
  temp_file_path
}

get_temp_dir <- function(){
  if (Sys.getenv("REDIVIS_TMPDIR") == ""){
    return(tempdir())
  } else {
    user_suffix <- Sys.info()[["user"]]
    return(file.path(Sys.getenv("REDIVIS_TMPDIR"), str_interp("redivis_${user_suffix}")))
  }
}

get_filename_from_content_disposition <- function(s) {
  fname <- stringr::str_match(s, "filename\\*=([^;]+)")[,2]
  if (is.na(fname) || length(fname) == 0) {
    fname <- stringr::str_match(s, "filename=([^;]+)")[,2]
  }
  if (grepl("utf-8''", fname, ignore.case = TRUE)) {
    fname <- URLdecode(sub("utf-8''", '', fname, ignore.case = TRUE))
  }
  fname <- stringr::str_trim(fname)
  fname <- stringr::str_replace_all(fname, '"', '')
  return(fname)
}

perform_resumable_upload <- function(data, temp_upload_url=NULL, proxy_url=NULL, on_progress=NULL) {
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

  if (!is.null(proxy_url)){
    temp_upload_url = str_interp("${proxy_url}?url=${utils::URLencode(temp_upload_url, reserved=TRUE, repeated=TRUE)}")
  }

  headers <- get_authorization_header()

  resumable_url <- initiate_resumable_upload(file_size, temp_upload_url, headers)

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
          if (!is.null(on_progress)){
            on_progress(bytes_read)
          }
          readBin(data, raw(), n = n)
        },
        seekfunction = function(offset){
          bytes_read <<- offset
          if (!is.null(on_progress)){
            on_progress(bytes_read)
          }
          seek(data, where = start_byte + offset)
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
      Sys.sleep(retry_count)
      cat("A network error occurred. Retrying resumable upload.\n")
      start_byte <<- retry_partial_upload(file_size=file_size, resumable_url=resumable_url, headers=headers)
      if (!is.null(on_progress)){
        on_progress(bytes_read)
      }
      seek(data, where=start_byte, origin="start")
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
    Sys.sleep(retry_count + 1)
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
    Sys.sleep(retry_count + 1)
    retry_partial_upload(retry_count=retry_count + 1, file_size=file_size, resumable_url=resumable_url, headers=headers)
  })
}

perform_standard_upload <- function(data, temp_upload_url=NULL, proxy_url=NULL, retry_count=0, on_progress=NULL) {
  original_url=temp_upload_url
  tryCatch({
    if (inherits(data, "connection") && file.exists(get_conn_name(data))){
      data <- httr::upload_file(get_conn_name(data), type = NULL)
    }

    headers = get_authorization_header()

    if (!is.null(proxy_url)){
      temp_upload_url = str_interp("${proxy_url}?url=${utils::URLencode(temp_upload_url, reserved=TRUE, repeated=TRUE)}")
    }

    # Perform the HTTP PUT request
    res <- httr::PUT(url = temp_upload_url, body = data, httr::add_headers(headers))
    if (httr::status_code(res) >= 400){
      # stop_for_status also fails for redirects
      httr::stop_for_status(res)
    }
  }, error = function(e) {
    print(e)
    if (retry_count > 20) {
      cat("A network error occurred. Upload failed after too many retries.\n")
      stop(e)
    }
    Sys.sleep(retry_count + 1)
    # Recursively call the function with incremented retry count
    perform_standard_upload(data, original_url, proxy_url, retry_count + 1, on_progress)
  })
}

## Helper to extract a file path from a connection
get_conn_name <- function(conn) {
  nm <- attr(conn, "name")
  if (is.null(nm)) nm <- summary(conn)$description
  nm
}

show_namespace_warning <- function(method){
  message <- str_interp("Deprecation warning: update `redivis::${method}()` to `redivis$${method}()`. Accessing methods directly on the Redivis namespace is deprecated and will be removed in a future version.")
  if (is.null(globals$printed_warnings[[message]])){
    globals$printed_warnings[message] <- TRUE
    warning(message)
  }
}

parallel_download_raw_files <- function(self, path = getwd(), overwrite = FALSE, max_results = NULL, file_id_variable = NULL, progress=TRUE, max_parallelization=100){
  if (endsWith(path, '/')) {
    path <- stringr::str_sub(path,1,nchar(path)-1) # remove trailing "/", as this screws up file.path()
  }

  if (is.null(file_id_variable)){
    file_id_variables <- make_paginated_request(path=str_interp("${self$uri}/variables"), max_results=2, query = list(isFileId = TRUE))

    if (length(file_id_variables) == 0){
      stop("No variable containing file ids was found on this table")
    } else if (length(file_id_variables) > 1){
      stop("This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter.")
    }
    file_id_variable = file_id_variables[[1]]$'name'
  }

  df <- make_rows_request(
    uri=self$uri,
    max_results=max_results,
    selected_variable_names = list(file_id_variable),
    type='data_table',
    variables=list(list(name=file_id_variable, type="string")),
    progress=FALSE
  )

  if (!dir.exists(path)) dir.create(path, recursive = TRUE)

  if (progress){
    progressr::with_progress(perform_parallel_raw_file_download(df[[file_id_variable]], path, overwrite, max_parallelization))
  } else {
    perform_parallel_raw_file_download(df[[file_id_variable]], path, overwrite, max_parallelization)
  }
}

perform_parallel_raw_file_download <- function(vec, path, overwrite, max_parallelization){
  pb <- progressr::progressor(steps = length(vec))
  download_paths <- list()
  get_download_path_from_headers <- function(headers){
    name <- get_filename_from_content_disposition(headers$'content-disposition')
    file_path <- base::file.path(path, name)
    download_paths <<- append(download_paths, file_path)
    return(file_path)
  }
  perform_parallel_download(
    purrr::map(vec, function(id){str_interp("/rawFiles/${id}")}),
    overwrite=overwrite,
    get_download_path_from_headers=get_download_path_from_headers,
    on_finish=function(){pb(1)},
    max_parallelization
  );
  return(download_paths)
}
