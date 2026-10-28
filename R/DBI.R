#' @include Query.R Dataset.R Workflow.R Table.R Variable.R
#' @import DBI
#' @import methods
NULL

# ===========================================================================
# DBI / dbplyr connector for Redivis (read-only)
# ===========================================================================
#
# Usage:
#   library(dplyr)
#   library(dbplyr)
#   con <- DBI::dbConnect(redivis::RedivisDBI(), dataset = "owner.dataset:v1.0")
#   con <- DBI::dbConnect(redivis::RedivisDBI(), workflow = "owner.workflow")
#   con <- DBI::dbConnect(redivis::RedivisDBI())  # uses global redivis$table()/redivis$query()
#
#   tbl(con, "my_table") |> filter(year > 2020) |> collect()
#   DBI::dbDisconnect(con)
# ---------------------------------------------------------------------------

# --- Driver ----------------------------------------------------------------

#' Redivis DBI classes and methods
#'
#' S4 classes implementing the DBI interface for Redivis. Create a connection
#' with \code{DBI::dbConnect(RedivisDBI(), ...)}; see \code{\link{RedivisDBI}}.
#' Writing tables is not supported.
#'
#' @name RedivisDBI-classes
#' @keywords internal
#' @aliases RedivisDriver-class
#' @aliases RedivisConnection-class
#' @aliases RedivisResult-class
#' @aliases dbCanConnect,RedivisDriver-method
#' @aliases dbClearResult,RedivisResult-method
#' @aliases dbColumnInfo,RedivisResult-method
#' @aliases dbConnect,RedivisDriver-method
#' @aliases dbCreateTable,RedivisConnection,character,character-method
#' @aliases dbDisconnect,RedivisConnection-method
#' @aliases dbExistsTable,RedivisConnection,character-method
#' @aliases dbFetch,RedivisResult-method
#' @aliases dbGetInfo,RedivisConnection-method
#' @aliases dbGetInfo,RedivisDriver-method
#' @aliases dbGetQuery,RedivisConnection,character-method
#' @aliases dbGetQuery,RedivisConnection,SQL-method
#' @aliases dbGetRowCount,RedivisResult-method
#' @aliases dbGetRowsAffected,RedivisResult-method
#' @aliases dbGetStatement,RedivisResult-method
#' @aliases dbHasCompleted,RedivisResult-method
#' @aliases dbIsValid,RedivisConnection-method
#' @aliases dbIsValid,RedivisDriver-method
#' @aliases dbIsValid,RedivisResult-method
#' @aliases dbListFields,RedivisConnection,character-method
#' @aliases dbListTables,RedivisConnection-method
#' @aliases dbQuoteIdentifier,RedivisConnection,character-method
#' @aliases dbQuoteIdentifier,RedivisConnection,SQL-method
#' @aliases dbQuoteString,RedivisConnection,character-method
#' @aliases dbRemoveTable,RedivisConnection,character-method
#' @aliases dbSendQuery,RedivisConnection,character-method
#' @aliases dbSendQuery,RedivisConnection,SQL-method
#' @aliases dbUnloadDriver,RedivisDriver-method
#' @aliases dbWriteTable,RedivisConnection,character,data.frame-method
#' @aliases show,RedivisConnection-method
#' @aliases show,RedivisDriver-method
#' @aliases show,RedivisResult-method
NULL

#' @export
setClass("RedivisDriver", contains = "DBIDriver")

#' Create a Redivis DBI driver
#' @return A RedivisDriver object
#' @export
RedivisDBI <- function() {
  new("RedivisDriver")
}

#' @export
setMethod("show", "RedivisDriver", function(object) {
  cat("<RedivisDriver>\n")
})

#' @export
setMethod("dbGetInfo", "RedivisDriver", function(dbObj, ...) {
  list(
    driver.version = utils::packageVersion("redivis"),
    client.version = utils::packageVersion("redivis"),
    max.connections = Inf
  )
})

#' @export
setMethod("dbIsValid", "RedivisDriver", function(dbObj, ...) TRUE)

#' @export
setMethod("dbUnloadDriver", "RedivisDriver", function(drv, ...) invisible(TRUE))

#' @export
setMethod("dbCanConnect", "RedivisDriver", function(drv, ...) TRUE)

# --- Connection ------------------------------------------------------------

#' @export
setClass(
  "RedivisConnection",
  contains = "DBIConnection",
  slots = list(
    owner = "ANY",
    .state = "environment"
  )
)

#' @export
setMethod(
  "dbConnect",
  "RedivisDriver",
  function(drv, dataset = NULL, workflow = NULL, ...) {
    if (!is.null(dataset) && !is.null(workflow)) {
      stop("Specify either `dataset` or `workflow`, not both.")
    }

    if (!is.null(dataset)) {
      if (is.character(dataset)) {
        owner <- Dataset$new(name = dataset)
      } else {
        owner <- dataset
      }
    } else if (!is.null(workflow)) {
      if (is.character(workflow)) {
        owner <- Workflow$new(name = workflow)
      } else {
        owner <- workflow
      }
    } else {
      owner <- NULL
    }

    state <- new.env(parent = emptyenv())
    state$disconnected <- FALSE

    new(
      "RedivisConnection",
      owner = owner,
      .state = state
    )
  }
)

#' @export
setMethod("show", "RedivisConnection", function(object) {
  status <- if (object@.state$disconnected) " (disconnected)" else ""
  label <- if (is.null(object@owner)) {
    "<global>"
  } else {
    object@owner$qualified_reference
  }
  cat("<RedivisConnection> ", label, status, "\n", sep = "")
})

#' @export
setMethod("dbGetInfo", "RedivisConnection", function(dbObj, ...) {
  list(
    db.version = NA_character_,
    dbname = if (is.null(dbObj@owner)) {
      "<global>"
    } else {
      dbObj@owner$qualified_reference
    }
  )
})

#' @export
setMethod("dbIsValid", "RedivisConnection", function(dbObj, ...) {
  !dbObj@.state$disconnected
})

#' @export
setMethod("dbDisconnect", "RedivisConnection", function(conn, ...) {
  conn@.state$disconnected <- TRUE
  invisible(TRUE)
})

# --- Querying --------------------------------------------------------------

#' @export
setMethod(
  "dbSendQuery",
  signature("RedivisConnection", "character"),
  function(conn, statement, ...) {
    assert_connected(conn)

    q <- if (is.null(conn@owner)) {
      redivis$query(statement)
    } else {
      conn@owner$query(statement)
    }

    new("RedivisResult", connection = conn, query = q)
  }
)

#' @export
setMethod(
  "dbSendQuery",
  signature("RedivisConnection", "SQL"),
  function(conn, statement, ...) {
    dbSendQuery(conn, as.character(statement), ...)
  }
)

#' @export
setMethod(
  "dbGetQuery",
  signature("RedivisConnection", "character"),
  function(conn, statement, ...) {
    assert_connected(conn)

    # Intercept sentinel SQL from sql_query_fields — return an empty
    # data frame with the correct column names without hitting the API
    if (grepl("^--redivis-fields:", statement)) {
      cols <- strsplit(sub("^--redivis-fields:", "", statement), "\t")[[1]]
      df <- as.data.frame(
        matrix(nrow = 0, ncol = length(cols)),
        stringsAsFactors = FALSE
      )
      names(df) <- cols
      return(df)
    }

    res <- dbSendQuery(conn, statement, ...)
    on.exit(dbClearResult(res))
    dbFetch(res)
  }
)

#' @export
setMethod(
  "dbGetQuery",
  signature("RedivisConnection", "SQL"),
  function(conn, statement, ...) {
    dbGetQuery(conn, as.character(statement), ...)
  }
)

# --- Table introspection ---------------------------------------------------

#' @export
setMethod("dbListTables", "RedivisConnection", function(conn, ...) {
  assert_connected(conn)
  if (is.null(conn@owner)) {
    return(character(0))
  }
  tables <- if (inherits(conn@owner, "Workflow")) {
    conn@owner$list_tables(include_datasources = TRUE)
  } else {
    conn@owner$list_tables()
  }
  table_sql_names(tables)
})

#' @export
setMethod(
  "dbExistsTable",
  signature("RedivisConnection", "character"),
  function(conn, name, ...) {
    assert_connected(conn)

    tryCatch(
      {
        if (is.null(conn@owner)) {
          redivis$table(name)$exists()
        } else {
          conn@owner$table(name)$exists()
        }
      },
      error = function(e) FALSE
    )
  }
)

#' @export
setMethod(
  "dbListFields",
  signature("RedivisConnection", "character"),
  function(conn, name, ...) {
    assert_connected(conn)
    tbl <- if (is.null(conn@owner)) {
      redivis$table(name)
    } else {
      conn@owner$table(name)
    }
    vars <- tbl$list_variables()
    vapply(vars, function(v) v$name, character(1))
  }
)

# --- Read-only guards ------------------------------------------------------

#' @export
setMethod(
  "dbWriteTable",
  signature("RedivisConnection", "character", "data.frame"),
  function(conn, name, value, ...) {
    stop("RedivisConnection is read-only. Writing tables is not supported.")
  }
)

#' @export
setMethod(
  "dbRemoveTable",
  signature("RedivisConnection", "character"),
  function(conn, name, ...) {
    stop("RedivisConnection is read-only. Removing tables is not supported.")
  }
)

#' @export
setMethod(
  "dbCreateTable",
  signature("RedivisConnection", "character", "character"),
  function(conn, name, fields, ...) {
    stop("RedivisConnection is read-only. Creating tables is not supported.")
  }
)

# --- Result ----------------------------------------------------------------

#' @export
setClass(
  "RedivisResult",
  contains = "DBIResult",
  slots = list(
    connection = "RedivisConnection",
    query = "ANY"
  )
)

#' @export
setMethod("show", "RedivisResult", function(object) {
  cat("<RedivisResult>\n")
})

#' @export
setMethod("dbFetch", "RedivisResult", function(res, n = -1, ...) {
  if (n >= 0) {
    res@query$to_tibble(max_results = n)
  } else {
    res@query$to_tibble()
  }
})

#' @export
setMethod("dbHasCompleted", "RedivisResult", function(res, ...) TRUE)

#' @export
setMethod("dbClearResult", "RedivisResult", function(res, ...) {
  invisible(TRUE)
})

#' @export
setMethod("dbGetRowCount", "RedivisResult", function(res, ...) {
  if (!is.null(res@query$properties$numRows)) {
    as.numeric(res@query$properties$numRows)
  } else {
    NA_real_
  }
})

#' @export
setMethod("dbGetRowsAffected", "RedivisResult", function(res, ...) 0L)

#' @export
setMethod("dbGetStatement", "RedivisResult", function(res, ...) {
  res@query$query
})

#' @export
setMethod("dbIsValid", "RedivisResult", function(dbObj, ...) TRUE)

#' @export
setMethod("dbColumnInfo", "RedivisResult", function(res, ...) {
  vars <- res@query$list_variables()
  data.frame(
    name = vapply(vars, function(v) v$name, character(1)),
    type = vapply(
      vars,
      function(v) redivis_type_to_r(v$properties$type),
      character(1)
    ),
    stringsAsFactors = FALSE
  )
})

# --- Identifier quoting (GoogleSQL uses backticks) -------------------------

#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("RedivisConnection", "SQL"),
  function(conn, x, ...) {
    x
  }
)

#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("RedivisConnection", "character"),
  function(conn, x, ...) {
    DBI::SQL(paste0("`", gsub("`", "\\\\`", x), "`"))
  }
)

#' @export
setMethod(
  "dbQuoteString",
  signature("RedivisConnection", "character"),
  function(conn, x, ...) {
    escaped <- gsub("'", "\\\\'", gsub("\\", "\\\\", x, fixed = TRUE))
    out <- paste0("'", escaped, "'")
    out[is.na(x)] <- "NULL"
    DBI::SQL(out)
  }
)

# --- dbplyr backend --------------------------------------------------------

#' @exportS3Method dbplyr::dbplyr_edition
dbplyr_edition.RedivisConnection <- function(con) 2L

# dbplyr >= 2.6.0 no longer routes string literals through dbQuoteString(), and
# its default only doubles quotes; GoogleSQL also needs backslashes escaped
#' @exportS3Method dbplyr::sql_escape_string
sql_escape_string.RedivisConnection <- function(con, x) {
  if (inherits(x, "SQL")) {
    return(x)
  }
  DBI::dbQuoteString(con, x)
}

#' @exportS3Method dbplyr::sql_escape_date
sql_escape_date.RedivisConnection <- function(con, x) {
  out <- paste0("DATE '", format(x, "%Y-%m-%d"), "'")
  out[is.na(x)] <- "NULL"
  DBI::SQL(out)
}

# Redivis dateTime variables are DATETIMEs (no time zone), which can't be
# compared with dbplyr's default ISO 8601 "...Z" string literals
#' @exportS3Method dbplyr::sql_escape_datetime
sql_escape_datetime.RedivisConnection <- function(con, x) {
  out <- paste0(
    "DATETIME '",
    format(x, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"),
    "'"
  )
  out[is.na(x)] <- "NULL"
  DBI::SQL(out)
}

#' @exportS3Method dbplyr::db_connection_describe
db_connection_describe.RedivisConnection <- function(con, ...) {
  label <- if (is.null(con@owner)) "<global>" else con@owner$qualified_reference
  paste0("Redivis: ", label)
}

#' @exportS3Method dbplyr::sql_translation
sql_translation.RedivisConnection <- function(con) {
  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,

      # Operators
      `^` = dbplyr::sql_prefix("POW"),
      # Like R, rounds toward negative infinity, so that the remainder takes
      # the sign of the divisor (DIV and MOD truncate toward zero, and only
      # accept integers)
      `%/%` = function(x, y) {
        dbplyr::build_sql("FLOOR(", x, " / ", y, ")")
      },
      `%%` = function(x, y) {
        dbplyr::build_sql("(", x, " - ", y, " * FLOOR(", x, " / ", y, "))")
      },
      `%||%` = dbplyr::sql_prefix("IFNULL"),

      # Type coercion
      as.integer = function(x) {
        dbplyr::build_sql("CAST(", x, " AS INT64)")
      },
      as.numeric = function(x) {
        dbplyr::build_sql("CAST(", x, " AS FLOAT64)")
      },
      as.double = function(x) {
        dbplyr::build_sql("CAST(", x, " AS FLOAT64)")
      },
      as.character = function(x) {
        dbplyr::build_sql("CAST(", x, " AS STRING)")
      },
      as.logical = function(x) {
        dbplyr::build_sql("CAST(", x, " AS BOOLEAN)")
      },
      as.Date = function(x, format = NULL, ...) {
        sql_as_date(x, format)
      },
      as_date = function(x, format = NULL, ...) {
        sql_as_date(x, format)
      },
      # Redivis dateTime variables are DATETIMEs; there is no TIMESTAMP type
      as.POSIXct = function(x, ...) {
        dbplyr::build_sql("CAST(", x, " AS DATETIME)")
      },
      as_datetime = function(x, ...) {
        dbplyr::build_sql("CAST(", x, " AS DATETIME)")
      },

      # String functions
      nchar = function(x) {
        dbplyr::build_sql("LENGTH(", x, ")")
      },
      tolower = function(x) {
        dbplyr::build_sql("LOWER(", x, ")")
      },
      toupper = function(x) {
        dbplyr::build_sql("UPPER(", x, ")")
      },
      trimws = function(x) {
        dbplyr::build_sql("TRIM(", x, ")")
      },
      startsWith = dbplyr::sql_prefix("STARTS_WITH", 2),
      endsWith = dbplyr::sql_prefix("ENDS_WITH", 2),
      # GoogleSQL has no CONCAT_WS, and ARRAY_TO_STRING requires strings
      paste = function(..., sep = " ") {
        args <- lapply(list(...), function(x) {
          dbplyr::build_sql("CAST(", int_arg(x), " AS STRING)")
        })
        dbplyr::build_sql(
          "ARRAY_TO_STRING([",
          dbplyr::sql(paste(unlist(args), collapse = ", ")),
          "], ",
          sep,
          ")"
        )
      },
      paste0 = dbplyr::sql_prefix("CONCAT"),
      substr = function(x, start, stop) {
        start <- int_arg(start)
        stop <- int_arg(stop)
        dbplyr::build_sql(
          "SUBSTR(",
          x,
          ", ",
          start,
          ", ",
          stop,
          " - ",
          start,
          " + 1)"
        )
      },
      substring = function(x, first, last = NULL) {
        first <- int_arg(first)
        if (is.null(last)) {
          dbplyr::build_sql("SUBSTR(", x, ", ", first, ")")
        } else {
          last <- int_arg(last)
          dbplyr::build_sql(
            "SUBSTR(",
            x,
            ", ",
            first,
            ", ",
            last,
            " - ",
            first,
            " + 1)"
          )
        }
      },

      # stringr equivalents
      str_detect = function(string, pattern, negate = FALSE) {
        out <- dbplyr::build_sql("REGEXP_CONTAINS(", string, ", ", pattern, ")")
        if (isTRUE(negate)) dbplyr::build_sql("NOT ", out) else out
      },
      str_starts = function(string, pattern, negate = FALSE) {
        out <- dbplyr::build_sql(
          "REGEXP_CONTAINS(",
          string,
          ", ",
          anchored_pattern(pattern, "^(?:", ")"),
          ")"
        )
        if (isTRUE(negate)) dbplyr::build_sql("NOT ", out) else out
      },
      str_ends = function(string, pattern, negate = FALSE) {
        out <- dbplyr::build_sql(
          "REGEXP_CONTAINS(",
          string,
          ", ",
          anchored_pattern(pattern, "(?:", ")$"),
          ")"
        )
        if (isTRUE(negate)) dbplyr::build_sql("NOT ", out) else out
      },
      str_extract = dbplyr::sql_prefix("REGEXP_EXTRACT", 2),
      str_replace = function(string, pattern, replacement) {
        sql_regexp_replace_first(string, pattern, replacement)
      },
      str_replace_all = dbplyr::sql_prefix("REGEXP_REPLACE", 3),
      str_remove = function(string, pattern) {
        sql_regexp_replace_first(string, pattern, "")
      },
      str_remove_all = function(string, pattern) {
        dbplyr::build_sql("REGEXP_REPLACE(", string, ", ", pattern, ", '')")
      },
      str_trim = function(x, side = "both") {
        switch(
          side,
          "both" = dbplyr::build_sql("TRIM(", x, ")"),
          "left" = dbplyr::build_sql("LTRIM(", x, ")"),
          "right" = dbplyr::build_sql("RTRIM(", x, ")"),
          stop("Unknown side: ", side)
        )
      },
      str_squish = function(x) {
        dbplyr::build_sql("TRIM(REGEXP_REPLACE(", x, ", '\\\\s+', ' '))")
      },
      str_pad = function(string, width, side = "left", pad = " ") {
        fn <- switch(
          side,
          "left" = "LPAD(",
          "right" = "RPAD(",
          abort_redivis_value_error(
            "str_pad() on Redivis only supports side = \"left\" or \"right\""
          )
        )
        dbplyr::build_sql(dbplyr::sql(fn), string, ", ", int_arg(width), ", ", pad, ")")
      },
      str_to_lower = function(x) dbplyr::build_sql("LOWER(", x, ")"),
      str_to_upper = function(x) dbplyr::build_sql("UPPER(", x, ")"),
      str_to_title = function(x) dbplyr::build_sql("INITCAP(", x, ")"),
      str_c = dbplyr::sql_prefix("CONCAT"),
      str_length = function(x) dbplyr::build_sql("LENGTH(", x, ")"),
      str_sub = function(string, start = 1L, end = -1L) {
        start <- int_arg(start)
        end <- int_arg(end)
        if (identical(end, -1L)) {
          dbplyr::build_sql("SUBSTR(", string, ", ", start, ")")
        } else {
          dbplyr::build_sql(
            "SUBSTR(",
            string,
            ", ",
            start,
            ", ",
            end,
            " - ",
            start,
            " + 1)"
          )
        }
      },

      # Regex (base R)
      grepl = function(pattern, x) {
        dbplyr::build_sql("REGEXP_CONTAINS(", x, ", ", pattern, ")")
      },
      gsub = function(pattern, replacement, x) {
        dbplyr::build_sql(
          "REGEXP_REPLACE(",
          x,
          ", ",
          pattern,
          ", ",
          replacement,
          ")"
        )
      },
      sub = function(pattern, replacement, x) {
        sql_regexp_replace_first(x, pattern, replacement)
      },

      # Conditional
      ifelse = dbplyr::sql_prefix("IF"),
      coalesce = dbplyr::sql_prefix("COALESCE"),

      # Parallel min/max
      pmax = dbplyr::sql_prefix("GREATEST"),
      pmin = dbplyr::sql_prefix("LEAST"),

      # Current date/time
      Sys.Date = function() dbplyr::sql("CURRENT_DATE()"),
      Sys.time = function() dbplyr::sql("CURRENT_DATETIME()"),
      today = function(...) dbplyr::sql("CURRENT_DATE()"),
      now = function(...) dbplyr::sql("CURRENT_DATETIME()"),

      # Date/time components (lubridate)
      year = function(x) sql_extract("YEAR", x),
      isoyear = function(x) sql_extract("ISOYEAR", x),
      quarter = function(x) sql_extract("QUARTER", x),
      month = function(x, label = FALSE, ...) {
        if (!isFALSE(label)) {
          abort_redivis_value_error(
            "month(label = TRUE) is not supported on Redivis"
          )
        }
        sql_extract("MONTH", x)
      },
      # lubridate::week() counts complete 7 day periods since January 1st
      week = function(x) {
        dbplyr::build_sql("DIV(", sql_extract("DAYOFYEAR", x), " - 1, 7) + 1")
      },
      isoweek = function(x) sql_extract("ISOWEEK", x),
      day = function(x) sql_extract("DAY", x),
      mday = function(x) sql_extract("DAY", x),
      yday = function(x) sql_extract("DAYOFYEAR", x),
      wday = function(x, label = FALSE, abbr = TRUE, week_start = 7) {
        if (!isFALSE(label) || week_start != 7) {
          abort_redivis_value_error(
            "wday() on Redivis only supports label = FALSE and week_start = 7"
          )
        }
        sql_extract("DAYOFWEEK", x)
      },
      hour = function(x) sql_extract("HOUR", x),
      minute = function(x) sql_extract("MINUTE", x),
      second = function(x) sql_extract("SECOND", x),
      floor_date = function(x, unit = "seconds", ...) {
        dbplyr::build_sql("DATE_TRUNC(", x, ", ", sql_date_part(unit), ")")
      },

      # Date/time components and arithmetic (clock)
      get_year = function(x) sql_extract("YEAR", x),
      get_month = function(x) sql_extract("MONTH", x),
      get_day = function(x) sql_extract("DAY", x),
      get_hour = function(x) sql_extract("HOUR", x),
      get_minute = function(x) sql_extract("MINUTE", x),
      get_second = function(x) sql_extract("SECOND", x),
      add_days = function(x, n, ...) sql_date_add(x, n, "DAY"),
      add_weeks = function(x, n, ...) sql_date_add(x, n, "WEEK"),
      add_months = function(x, n, ...) sql_date_add(x, n, "MONTH"),
      add_quarters = function(x, n, ...) sql_date_add(x, n, "QUARTER"),
      add_years = function(x, n, ...) sql_date_add(x, n, "YEAR"),
      date_build = function(year, month = 1L, day = 1L, ...) {
        dbplyr::build_sql(
          "DATE(",
          int_arg(year),
          ", ",
          int_arg(month),
          ", ",
          int_arg(day),
          ")"
        )
      },
      # Counts complete units, like clock (DATE_DIFF counts unit boundaries)
      date_count_between = function(start, end, precision, ..., n = 1L) {
        if (!(is.numeric(n) && length(n) == 1 && isTRUE(n == 1))) {
          abort_redivis_value_error(
            "date_count_between() on Redivis only supports n = 1"
          )
        }
        switch(
          precision,
          "day" = dbplyr::build_sql("DATE_DIFF(", end, ", ", start, ", DAY)"),
          "week" = dbplyr::build_sql(
            "DIV(DATE_DIFF(",
            end,
            ", ",
            start,
            ", DAY), 7)"
          ),
          abort_redivis_value_error(
            "date_count_between() on Redivis only supports precision = \"day\" or \"week\""
          )
        )
      },
      difftime = function(time1, time2, tz, units = "days") {
        if (!missing(tz) || !identical(units, "days")) {
          abort_redivis_value_error(
            "difftime() on Redivis only supports units = \"days\" and no tz"
          )
        }
        dbplyr::build_sql("DATE_DIFF(", time1, ", ", time2, ", DAY)")
      },

      # Math
      log = function(x, base = exp(1)) {
        if (isTRUE(all.equal(base, exp(1)))) {
          dbplyr::build_sql("LN(", x, ")")
        } else if (isTRUE(all.equal(base, 10))) {
          dbplyr::build_sql("LOG10(", x, ")")
        } else {
          dbplyr::build_sql("LOG(", x, ") / LOG(", base, ")")
        }
      },
      log10 = function(x) dbplyr::build_sql("LOG10(", x, ")"),
      log2 = function(x) dbplyr::build_sql("LOG(", x, ") / LOG(2)"),

      # Rounding
      round = function(x, digits = 0L) {
        dbplyr::build_sql("ROUND(", x, ", ", int_arg(digits), ")")
      },
      floor = function(x) dbplyr::build_sql("FLOOR(", x, ")"),
      ceiling = function(x) dbplyr::build_sql("CEIL(", x, ")")
    ),
    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,

      n = function() dbplyr::sql("COUNT(*)"),

      all = dbplyr::sql_prefix("LOGICAL_AND", 1),
      any = dbplyr::sql_prefix("LOGICAL_OR", 1),

      sd = dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      cor = dbplyr::sql_aggregate_2("CORR"),
      cov = dbplyr::sql_aggregate_2("COVAR_SAMP"),

      n_distinct = function(x) {
        dbplyr::build_sql("COUNT(DISTINCT ", x, ")")
      },
      median = function(x, na.rm = TRUE) {
        dbplyr::build_sql("APPROX_QUANTILES(", x, ", 2)[SAFE_ORDINAL(2)]")
      },
      # Approximate, to the nearest 0.1 percentile
      quantile = function(x, probs, na.rm = TRUE) {
        if (!is.numeric(probs) || length(probs) != 1 || probs < 0 || probs > 1) {
          abort_redivis_value_error(
            "quantile() on Redivis requires a single `probs` value between 0 and 1"
          )
        }
        dbplyr::build_sql(
          "APPROX_QUANTILES(",
          x,
          ", 1000)[OFFSET(",
          as.integer(round(probs * 1000)),
          ")]"
        )
      },
      str_flatten = function(string, collapse = "", ...) {
        dbplyr::build_sql("STRING_AGG(", string, ", ", collapse, ")")
      }
    ),
    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,

      n = function() dbplyr::sql("COUNT(*)"),

      all = dbplyr::win_absent("LOGICAL_AND"),
      any = dbplyr::win_absent("LOGICAL_OR"),

      sd = dbplyr::win_recycled("STDDEV_SAMP"),
      var = dbplyr::win_recycled("VAR_SAMP"),
      cor = dbplyr::win_absent("CORR"),
      cov = dbplyr::win_absent("COVAR_SAMP"),

      n_distinct = dbplyr::win_absent("n_distinct"),
      median = dbplyr::win_absent("median"),
      quantile = dbplyr::win_absent("quantile"),
      str_flatten = dbplyr::win_absent("str_flatten")
    )
  )
}

#' @exportS3Method dbplyr::sql_query_fields
sql_query_fields.RedivisConnection <- function(con, sql, ...) {
  # In dbplyr edition 2, sql_query_fields returns SQL that
  # dbplyr_query_fields then executes via db_get_query to get column names.
  #
  # For simple table references, we intercept here and extract column
  # names via the Redivis API (table$list_variables()), returning a
  # trivial query that won't actually be executed — we throw a custom
  # condition that our dbGetQuery method catches.
  #
  # For complex SQL, we fall through to the default implementation.
  table_name <- NULL

  if (inherits(sql, "ident") || inherits(sql, "ident_q")) {
    table_name <- as.character(sql)
  } else {
    sql_str <- trimws(as.character(sql))
    m <- regmatches(sql_str, regexec("^`([^`]+)`$", sql_str))[[1]]
    if (length(m) == 2) {
      table_name <- m[2]
    }
  }

  if (!is.null(table_name)) {
    field_names <- dbListFields(con, table_name)
    # Return a sentinel SQL that our dbGetQuery will intercept
    sentinel <- paste0("--redivis-fields:", paste(field_names, collapse = "\t"))
    return(DBI::SQL(sentinel))
  }

  NextMethod()
}

# --- Helpers ---------------------------------------------------------------

assert_connected <- function(conn) {
  if (conn@.state$disconnected) {
    stop("Connection has been disconnected.")
  }
}

redivis_type_to_r <- function(type) {
  switch(
    type %||% "unknown",
    "integer" = "numeric",
    "float" = "double",
    "boolean" = "logical",
    "date" = "Date",
    "dateTime" = "POSIXct",
    "time" = "character",
    "geography" = "character",
    "string" = "character",
    "character"
  )
}

# The name a table is referenced by in SQL: its scoped
# reference without the trailing reference id, e.g. "concept_class:3jet" ->
# "concept_class"
sql_reference_name <- function(properties) {
  sub(":.*$", "", properties$scopedReference %||% properties$name)
}

table_sql_names <- function(tables) {
  vapply(tables, function(t) sql_reference_name(t$properties), character(1))
}

# dbplyr renders whole-number R doubles as e.g. `2.0`, but many GoogleSQL
# functions only accept INT64 arguments
int_arg <- function(x) {
  if (is.double(x) && all(x == trunc(x), na.rm = TRUE)) as.integer(x) else x
}

is_string_literal <- function(x) {
  is.character(x) && !inherits(x, c("sql", "ident"))
}

sql_extract <- function(part, x) {
  dbplyr::build_sql("EXTRACT(", dbplyr::sql(part), " FROM ", x, ")")
}

sql_as_date <- function(x, format) {
  if (is.null(format)) {
    dbplyr::build_sql("CAST(", x, " AS DATE)")
  } else {
    dbplyr::build_sql("PARSE_DATE(", format, ", ", x, ")")
  }
}

sql_date_part <- function(unit) {
  part <- sub("s$", "", tolower(unit))
  parts <- c("second", "minute", "hour", "day", "week", "month", "quarter", "year")
  if (!(part %in% parts)) {
    abort_redivis_value_error(str_interp("Unsupported date unit: ${unit}"))
  }
  dbplyr::sql(toupper(part))
}

sql_date_add <- function(x, n, part) {
  dbplyr::build_sql(
    "DATE_ADD(",
    x,
    ", INTERVAL ",
    int_arg(n),
    " ",
    dbplyr::sql(part),
    ")"
  )
}

anchored_pattern <- function(pattern, prefix, suffix) {
  if (is_string_literal(pattern)) {
    paste0(prefix, pattern, suffix)
  } else {
    dbplyr::build_sql("CONCAT(", prefix, ", ", pattern, ", ", suffix, ")")
  }
}

# GoogleSQL's REGEXP_REPLACE always replaces every match, so emulate replacing
# only the first match by capturing (and restoring) everything before it
sql_regexp_replace_first <- function(string, pattern, replacement) {
  if (
    !is_string_literal(pattern) ||
      !is_string_literal(replacement) ||
      grepl("\\", replacement, fixed = TRUE)
  ) {
    abort_redivis_value_error(
      "Replacing only the first match on Redivis requires a literal pattern and replacement (without backreferences). Use str_replace_all() / gsub() to replace every match."
    )
  }
  dbplyr::build_sql(
    "REGEXP_REPLACE(",
    string,
    ", ",
    paste0("(?s)^(.*?)(?:", pattern, ")"),
    ", ",
    paste0("\\1", replacement),
    ")"
  )
}
