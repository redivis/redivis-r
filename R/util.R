globals <- new.env(parent = emptyenv())
globals$printed_warnings <- list()

# Base R only has this since 4.4
`%||%` <- function(x, y) if (is.null(x)) y else x

# Sleep before a retry. A separate function so tests can skip the waits.
retry_sleep <- function(seconds) {
  Sys.sleep(seconds)
}

# Sleep between polls of a job's status. Also separate so tests can skip it.
poll_sleep <- function(seconds) {
  Sys.sleep(seconds)
}

# Splits a reference like `owner.dataset:v1_0.table` into its dot-separated
# parts. A version tag can also be written with a dot (`:v1.0`), so it's first
# normalized to the equivalent underscore form.
split_reference <- function(reference) {
  normalized <- gsub(":v(\\d+)\\.(\\d+)", ":v\\1_\\2", reference)
  strsplit(normalized, ".", fixed = TRUE)[[1]]
}

# Whether an environment variable is set to a non-empty value
has_env <- function(name) {
  nzchar(Sys.getenv(name))
}

# Waits for a transform or notebook run, given the response to starting it.
# Raises a job error if the run fails or is cancelled.
wait_for_run <- function(instance, run_properties) {
  job_id <- run_properties[["currentJob"]][["id"]]
  repeat {
    poll_sleep(2)
    instance$get()
    jobs <- list(
      instance$properties[["currentJob"]],
      instance$properties[["lastRunJob"]]
    )
    job <- if (!is.null(job_id)) {
      # Track the job this run started, rather than whichever is most recent
      Find(function(j) identical(j[["id"]], job_id), jobs)
    } else {
      jobs[[1]] %||% jobs[[2]]
    }
    if (
      !is.null(job) &&
        job[["status"]] %in% c("completed", "failed", "cancelled")
    ) {
      if (job[["status"]] != "completed") {
        abort_redivis_job_error(
          message = job$errorMessage,
          kind = job$kind,
          status = job$status
        )
      }
      return(invisible(NULL))
    }
  }
}

# Downloads are written to this file alongside the destination, which only
# replaces the destination once complete and verified, so that a failed
# download never leaves a partial file there
get_partial_download_path <- function(download_path) {
  paste0(download_path, ".redivis-partial")
}

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

# Attaches each variable's label and value labels to its column, following
# haven's conventions: columns with value labels become haven::labelled
# vectors, and any other labelled column gets a "label" attribute
apply_variable_labels <- function(df, variables) {
  if (!requireNamespace("haven", quietly = TRUE)) {
    abort_redivis_error(
      "The haven package must be installed to read data with labelled = TRUE."
    )
  }
  is_data_table <- data.table::is.data.table(df)
  lower_names <- tolower(names(df))
  for (variable in variables) {
    index <- match(tolower(variable$name), lower_names)
    if (is.na(index)) {
      next
    }
    labelled <- get_label_attributes(
      df[[index]],
      variable$label,
      variable$valueLabels
    )
    if (!length(labelled$attributes)) {
      next
    }
    if (is_data_table) {
      # data.table columns are already materialized, and setting attributes
      # on them from R would copy the whole column, so set them in place
      if (labelled$widened) {
        data.table::set(df, j = index, value = list(labelled$x))
      }
      for (name in names(labelled$attributes)) {
        data.table::setattr(df[[index]], name, labelled$attributes[[name]])
      }
    } else {
      # Unlike attr<-, structure() keeps arrow's lazily converted (ALTREP)
      # columns from being materialized
      df[[index]] <- do.call(
        structure,
        c(list(labelled$x), labelled$attributes)
      )
    }
  }
  df
}

# Returns the attributes to set on x, and x itself, which is widened to a
# double when an integer column has non-integral value labels
get_label_attributes <- function(x, label, value_labels) {
  label_names <- purrr::map_chr(value_labels, "label")
  label_values <- purrr::map_chr(value_labels, "value")
  widened <- FALSE

  can_have_value_labels <- is.character(x) ||
    (is.numeric(x) && !inherits(x, "integer64"))

  # Labels that don't parse as numbers can't apply to numeric columns
  if (length(label_values) && can_have_value_labels && is.numeric(x)) {
    label_values <- suppressWarnings(as.numeric(label_values))
    keep <- !is.na(label_values)
    label_values <- label_values[keep]
    label_names <- label_names[keep]
  }

  if (length(label_values) && can_have_value_labels) {
    if (is.integer(x)) {
      if (all(label_values == round(label_values))) {
        label_values <- as.integer(label_values)
      } else {
        x <- as.double(x)
        widened <- TRUE
      }
    }
    # The same attributes haven::labelled() sets
    attributes <- list(
      labels = purrr::set_names(label_values, label_names),
      label = label,
      class = c("haven_labelled", "vctrs_vctr", typeof(x))
    )
  } else {
    attributes <- list(label = label)
  }

  list(
    x = x,
    widened = widened,
    attributes = Filter(Negate(is.null), attributes)
  )
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

  rpg <- TARGET_GROUP_BYTES / bytes_per_row
  rpg <- max(MIN_RPG, min(MAX_RPG, rpg))
  as.integer(rpg)
}

convert_data_to_parquet <- function(data) {
  folder <- file.path(get_temp_dir(), "parquet")

  if (!dir.exists(folder)) {
    dir.create(folder, recursive = TRUE)
  }

  temp_file_path <- file.path(folder, uuid::UUIDgenerate())

  if (is(data, "sf")) {
    data <- sf_to_geoparquet_table(data)
  }

  rows_per_group <- get_parquet_rows_per_group(data)

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
    if (!is(data, "Table") && !is(data, "RecordBatch")) {
      # We need to do this, otherwise the metadata on large R objects completely bloat the parquet file and cause read errors
      data <- arrow::as_arrow_table(data)
      data$metadata$r <- NULL # drop serialized R attributes
    }
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

# An arrow table of an sf data frame, in the form of GeoParquet: its geometries
# as WKB, described by the table's "geo" metadata, which Redivis reads as
# geographies. Geographies are in WGS84, so other coordinates are transformed.
sf_to_geoparquet_table <- function(data) {
  is_geometry <- vapply(data, inherits, logical(1), "sfc")
  geometry_names <- names(data)[is_geometry]
  table <- arrow::as_arrow_table(as.data.frame(data)[!is_geometry])
  table$metadata$r <- NULL # drop serialized R attributes

  wgs84 <- sf::st_crs(4326)
  for (name in geometry_names) {
    geometry <- data[[name]]
    if (!is.na(sf::st_crs(geometry)) && sf::st_crs(geometry) != wgs84) {
      geometry <- sf::st_transform(geometry, wgs84)
    }
    table[[name]] <- arrow::Array$create(
      unclass(sf::st_as_binary(geometry)),
      type = arrow::binary()
    )
  }
  table <- table[, names(data)]

  # Without a "crs", GeoParquet coordinates are longitude/latitude in WGS84
  table$metadata$geo <- as.character(jsonlite::toJSON(
    list(
      version = "1.0.0",
      primary_column = attr(data, "sf_column"),
      columns = stats::setNames(
        lapply(geometry_names, function(name) {
          list(encoding = "WKB", geometry_types = list())
        }),
        geometry_names
      )
    ),
    auto_unbox = TRUE
  ))
  table
}
