globals <- new.env(parent = emptyenv())
globals$printed_warnings <- list()

get_arrow_schema <- function(variables) {
  schema <- purrr::map(variables, function(variable) {
    if (variable$type == 'integer') {
      arrow::int64()
    } else if (variable$type == 'float') {
      arrow::float64()
    } else if (variable$type == 'boolean') {
      arrow::boolean()
    } else if (variable$type == 'date') {
      arrow::date32()
    } else if (variable$type == 'dateTime') {
      arrow::timestamp(unit = "us", timezone = "")
    } else if (variable$type == 'time') {
      arrow::time64(unit = "us")
    } else {
      arrow::string()
    }
  })

  names <- purrr::map(variables, function(variable) variable$name)

  arrow::schema(purrr::set_names(schema, names))
}

convert_data_to_parquet <- function(data) {
  folder <- str_interp('/${get_temp_dir()}/parquet')

  if (!dir.exists(folder)) {
    dir.create(folder, recursive = TRUE)
  }

  temp_file_path <- str_interp('${folder}/${uuid::UUIDgenerate()}')

  if (is(data, "sf")) {
    sf_column_name <- attr(data, "sf_column")
    wkt_geopoint <- sapply(sf::st_geometry(data), function(x) sf::st_as_text(x))
    sf::st_geometry(data) <- NULL
    data[sf_column_name] <- wkt_geopoint
  }

  if (is(data, "Dataset")) {
    dir.create(temp_file_path)
    arrow::write_dataset(
      data,
      temp_file_path,
      format = 'parquet',
      max_partitions = 1,
      basename_template = "part-{i}.parquet"
    )
    temp_file_path <- str_interp('${temp_file_path}/part-0.parquet')
  } else {
    arrow::write_parquet(data, sink = temp_file_path)
  }
  temp_file_path
}

get_temp_dir <- function() {
  if (Sys.getenv("REDIVIS_TMPDIR") == "") {
    return(tempdir())
  } else {
    user_suffix <- Sys.info()[["user"]]
    return(file.path(
      Sys.getenv("REDIVIS_TMPDIR"),
      str_interp("redivis_${user_suffix}")
    ))
  }
}

get_filename_from_content_disposition <- function(s) {
  fname <- stringr::str_match(s, "filename\\*=([^;]+)")[, 2]
  if (is.na(fname) || length(fname) == 0) {
    fname <- stringr::str_match(s, "filename=([^;]+)")[, 2]
  }
  if (grepl("utf-8''", fname, ignore.case = TRUE)) {
    fname <- URLdecode(sub("utf-8''", '', fname, ignore.case = TRUE))
  }
  fname <- stringr::str_trim(fname)
  fname <- stringr::str_replace_all(fname, '"', '')
  return(fname)
}


## Helper to extract a file path from a connection
get_conn_name <- function(conn) {
  nm <- attr(conn, "name")
  if (is.null(nm)) {
    nm <- summary(conn)$description
  }
  nm
}

show_namespace_warning <- function(method) {
  message <- str_interp(
    "Deprecation warning: update `redivis::${method}()` to `redivis$${method}()`. Accessing methods directly on the Redivis namespace is deprecated and will be removed in a future version."
  )
  if (is.null(globals$printed_warnings[[message]])) {
    globals$printed_warnings[message] <- TRUE
    warning(message, call. = FALSE)
  }
}
