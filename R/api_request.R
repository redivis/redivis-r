#' @include auth.R util.R
make_request <- function(
  method = 'GET',
  query = NULL,
  payload = NULL,
  files = NULL, # TODO: remove once we remove multipart uploads
  parse_response = TRUE,
  path = "",
  download_path = NULL,
  download_overwrite = FALSE,
  headers = list(),
  headers_callback = NULL,
  stream_callback = NULL,
  as_connection = FALSE,
  start_byte = 0,
  end_byte = NULL,
  resumable_byte = 0,
  retry_count = 0
) {
  args <- list(
    method = method,
    query = query,
    payload = payload,
    files = files,
    parse_response = parse_response,
    path = path,
    download_path = download_path,
    download_overwrite = download_overwrite,
    headers = headers,
    headers_callback = headers_callback,
    stream_callback = stream_callback,
    as_connection = as_connection,
    start_byte = start_byte,
    end_byte = end_byte,
    resumable_byte = resumable_byte,
    retry_count = retry_count
  )

  if (start_byte || resumable_byte) {
    if (!is.null(end_byte)) {
      headers$Range = str_interp(
        "bytes=${start_byte+resumable_byte}-${end_byte}"
      )
    } else {
      headers$Range = str_interp("bytes=${start_byte+resumable_byte}-")
    }
  }

  auth_headers <- get_authorization_header(as_list = TRUE)
  all_headers <- c(auth_headers, headers)

  req <- httr2::request(generate_api_url(path)) |>
    httr2::req_method(method) |>
    httr2::req_headers(!!!all_headers) |>
    httr2::req_timeout(3600) |>
    httr2::req_error(is_error = function(resp) FALSE)

  if (length(query) > 0) {
    req <- req |> httr2::req_url_query(!!!query)
  }

  if (!is.null(files)) {
    # Multipart upload
    for (nm in names(files)) {
      file_val <- files[[nm]]
      if (inherits(file_val, "form_file")) {
        req <- req |>
          httr2::req_body_multipart(
            !!nm := curl::form_file(file_val$path, type = file_val$type)
          )
      } else {
        req <- req |> httr2::req_body_multipart(!!nm := file_val)
      }
    }
  } else if (!is.null(payload)) {
    body <- jsonlite::toJSON(
      payload,
      na = "null",
      null = "null",
      auto_unbox = TRUE
    )
    req <- req |>
      httr2::req_headers("Content-Type" = "application/json") |>
      httr2::req_body_raw(body, type = "application/json")
  }

  if (!is.null(stream_callback) || as_connection) {
    # Streaming response via connection
    res <- httr2::req_perform_connection(req)

    # If we're returning the connection,
    if (!as_connection) {
      on.exit(close(res), add = TRUE)
    }

    status <- httr2::resp_status(res)

    if (status == 503 && retry_count < 10) {
      close(res)
      on.exit(NULL, add = FALSE)
      Sys.sleep(retry_count)
      args$retry_count <- args$retry_count + 1
      return(do.call(make_request, args))
    }

    if (status >= 400) {
      resp_body <- httr2::resp_body_string(res)
      close(res)
      on.exit(NULL, add = FALSE)
      handle_error_response(res, resp_body, method, args)
      return(invisible(NULL))
    }

    resp_headers <- httr2::resp_headers(res)
    redivis_warning <- resp_headers[["x-redivis-warning"]]
    if (
      !is.null(redivis_warning) &&
        is.null(globals$printed_warnings[[redivis_warning]])
    ) {
      globals$printed_warnings[redivis_warning] <- TRUE
      warning(redivis_warning, call. = FALSE)
    }

    if (!is.null(headers_callback)) {
      headers_callback(resp_headers)
    }
    if (as_connection) {
      return(res)
    }

    while (length(buf <- httr2::resp_stream_raw(res, kb = 16)) > 0) {
      cb_res <- stream_callback(buf)
      if (!is.null(cb_res) && cb_res == FALSE) {
        break
      }
    }

    return(invisible(NULL))
  }

  if (!is.null(download_path)) {
    if (!download_overwrite && file.exists(download_path)) {
      abort_redivis_error(
        str_interp("File already exists: ${download_path}"),
      )
    }
    res <- httr2::req_perform(req, path = download_path)
  } else {
    res <- httr2::req_perform(req)
  }

  status <- httr2::resp_status(res)

  if (status == 503 && retry_count < 10) {
    Sys.sleep(retry_count)
    args$retry_count <- args$retry_count + 1
    return(do.call(make_request, args))
  }

  resp_headers <- httr2::resp_headers(res)

  redivis_warning <- resp_headers[["x-redivis-warning"]]
  if (
    !is.null(redivis_warning) &&
      is.null(globals$printed_warnings[[redivis_warning]])
  ) {
    globals$printed_warnings[redivis_warning] <- TRUE
    warning(redivis_warning, call. = FALSE)
  }

  if ((!parse_response || method == "HEAD") && status < 400) {
    return(res)
  }

  if (status >= 400) {
    resp_body <- if (method == "HEAD") NULL else httr2::resp_body_string(res)
    handle_error_response(res, resp_body, method, args)
    return(invisible(NULL))
  }

  # Parse successful response
  response_content <- httr2::resp_body_string(res)
  content_type <- resp_headers[["content-type"]]
  if (
    !is.null(content_type) &&
      stringr::str_starts(content_type, 'application/json')
  ) {
    response_content <- jsonlite::fromJSON(
      response_content,
      simplifyVector = FALSE
    )
  }

  response_content
}

#' Handle error responses for both streaming and non-streaming requests
#' @param res The httr2 response object
#' @param resp_body The response body as a string, or NULL for HEAD requests
#' @param method The HTTP method
#' @param args The original request arguments (for retrying after credential refresh)
#' @keywords internal
handle_error_response <- function(res, resp_body, method, args) {
  status <- httr2::resp_status(res)
  resp_headers <- httr2::resp_headers(res)

  is_json <- FALSE
  response_content <- NULL

  if (method == "HEAD") {
    error_payload <- resp_headers[["x-redivis-error-payload"]]
    if (!is.null(error_payload)) {
      is_json <- TRUE
      response_content <- jsonlite::fromJSON(
        URLdecode(error_payload),
        simplifyVector = FALSE
      )
    }
  } else if (!is.null(resp_body)) {
    response_content <- resp_body
    content_type <- resp_headers[["content-type"]]
    if (
      !is.null(content_type) &&
        stringr::str_starts(content_type, 'application/json')
    ) {
      is_json <- TRUE
      response_content <- jsonlite::fromJSON(
        response_content,
        simplifyVector = FALSE
      )
    }
  }

  if (
    (status == 401 ||
      (status == 403 &&
        is_json &&
        response_content$error == 'insufficient_scope')) &&
      is.na(Sys.getenv("REDIVIS_API_TOKEN", unset = NA)) &&
      is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset = NA))
  ) {
    refresh_credentials(
      scope = if (is.null(response_content$scope)) {
        NULL
      } else {
        strsplit(response_content$scope, " ")
      },
      amr_values = response_content$amr_values
    )
    return(do.call(make_request, args))
  }

  if (is_json) {
    raise_api_error(
      response_json = response_content,
      response = res
    )
  } else {
    raise_api_error(
      response_text = response_content,
      response = res
    )
  }
}

parse_curl_headers <- function(res_data) {
  vec <- curl::parse_headers(res_data$headers)

  header_names <- purrr::map(vec, function(header) {
    tolower(strsplit(header, ':')[[1]][[1]])
  })
  header_contents <- purrr::map(vec, function(header) {
    split = strsplit(header, ':\\s+')[[1]]
    paste0(tail(split, -1), collapse = ':')
  })
  headers <- purrr::set_names(header_contents, header_names)
}

make_paginated_request <- function(
  path,
  query = list(),
  page_size = 100,
  max_results = NULL
) {
  page <- 0
  results <- list()
  next_page_token <- NULL

  while (TRUE) {
    if (!is.null(max_results) && length(results) >= max_results) {
      break
    }

    response = make_request(
      method = "GET",
      path = path,
      parse_response = TRUE,
      query = append(
        query,
        list(
          "pageToken" = next_page_token,
          "maxResults" = if (
            is.null(max_results) || (page + 1) * page_size < max_results
          ) {
            page_size
          } else {
            max_results - page * page_size
          }
        )
      )
    )

    page <- page + 1
    results <- append(results, response$results)
    next_page_token <- response$nextPageToken
    if (is.null(next_page_token)) {
      break
    }
  }

  results
}


generate_api_url <- function(path) {
  str_interp(
    '${if (Sys.getenv("REDIVIS_API_ENDPOINT") == "") "https://redivis.com/api/v1" else Sys.getenv("REDIVIS_API_ENDPOINT")}${utils::URLencode(path)}'
  )
}

version_info <- R.Version()
redivis_version <- packageVersion("redivis")
get_authorization_header <- function(as_list = FALSE) {
  auth_token <- get_auth_token()
  if (as_list) {
    list(
      "Authorization" = str_interp("Bearer ${auth_token}"),
      "User-Agent" = str_interp(
        "redivis-r/${redivis_version} (${version_info$platform}; R/${version_info$major}.${version_info$minor})"
      )
    )
  } else {
    c(
      "Authorization" = str_interp("Bearer ${auth_token}"),
      "User-Agent" = str_interp(
        "redivis-r/${redivis_version} (${version_info$platform}; R/${version_info$major}.${version_info$minor})"
      )
    )
  }
}
