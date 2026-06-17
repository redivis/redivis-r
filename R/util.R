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

get_parquet_rows_per_group <- function(data) {
  TARGET_GROUP_BYTES <- 128 * 1024^2 # ~128MB per row group
  MAX_RPG <- 1000000L
  MIN_RPG <- 1L
  SAMPLE_N <- 10000L # rows to slice for Arrow tables

  bytes_per_row <- NA_real_

  if (is(data, "Dataset")) {
    # Cheap, metadata-only: sum source file sizes / row count
    files <- data$files
    total_bytes <- sum(file.size(files), na.rm = TRUE)
    n_rows <- tryCatch(data$num_rows, error = function(e) NA_real_)
    if (!is.na(n_rows) && n_rows > 0 && total_bytes > 0) {
      bytes_per_row <- total_bytes / n_rows
    }
  } else if (inherits(data, "Table") || inherits(data, "RecordBatch")) {
    # Slice a prefix (zero-copy) and measure just that
    n_rows <- data$num_rows
    if (n_rows > 0) {
      s <- data$Slice(0, min(SAMPLE_N, n_rows))
      bytes_per_row <- s$nbytes() / s$num_rows
    }
  } else {
    # tibble / data.table / data.frame: object.size on the whole object
    n_rows <- nrow(data)
    if (!is.null(n_rows) && n_rows > 0) {
      bytes_per_row <- as.numeric(object.size(data)) / n_rows
    }
  }

  if (is.na(bytes_per_row) || bytes_per_row <= 0) {
    return(100000L)
  }

  rpg <- as.integer(TARGET_GROUP_BYTES / bytes_per_row)
  max(MIN_RPG, min(MAX_RPG, rpg))
}

convert_data_to_parquet <- function(data) {
  folder <- file.path(get_temp_dir(), "parquet")

  if (!dir.exists(folder)) {
    dir.create(folder, recursive = TRUE)
  }

  temp_file_path <- file.path(folder, uuid::UUIDgenerate())

  if (is(data, "sf")) {
    sf_column_name <- attr(data, "sf_column")
    wkt_geopoint <- sapply(sf::st_geometry(data), function(x) sf::st_as_text(x))
    sf::st_geometry(data) <- NULL
    data[sf_column_name] <- wkt_geopoint
  }

  rows_per_group = get_parquet_rows_per_group(data)

  if (is(data, "Dataset")) {
    dir.create(temp_file_path)
    arrow::write_dataset(
      data,
      temp_file_path,
      format = 'parquet',
      max_partitions = 1,
      basename_template = "part-{i}.parquet",
      write_statistics = FALSE,
      min_rows_per_group = rows_per_group,
      max_rows_per_group = rows_per_group
    )
    temp_file_path <- str_interp('${temp_file_path}/part-0.parquet')
  } else {
    arrow::write_parquet(
      data,
      sink = temp_file_path,
      write_statistics = FALSE,
      chunk_size = rows_per_group
    )
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
