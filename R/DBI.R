#' @include Query.R Dataset.R Workflow.R Table.R Variable.R
#' @import DBI
#' @import methods

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
  tables <- conn@owner$list_tables()
  vapply(tables, function(t) t$name, character(1))
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
    DBI::SQL(paste0("'", gsub("'", "\\\\'", x), "'"))
  }
)

# --- dbplyr backend --------------------------------------------------------

#' @importFrom dbplyr dbplyr_edition
#' @export
dbplyr_edition.RedivisConnection <- function(con) 2L

#' @importFrom dbplyr db_connection_describe
#' @export
db_connection_describe.RedivisConnection <- function(con, ...) {
  label <- if (is.null(con@owner)) "<global>" else con@owner$qualified_reference
  paste0("Redivis: ", label)
}

#' @importFrom dbplyr sql_translation
#' @export
sql_translation.RedivisConnection <- function(con) {
  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,

      # Operators
      `^` = dbplyr::sql_prefix("POW"),
      `%%` = dbplyr::sql_prefix("MOD"),
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
      as.Date = function(x) {
        dbplyr::build_sql("CAST(", x, " AS DATE)")
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
      paste = function(..., sep = " ") {
        dbplyr::build_sql("CONCAT_WS(", sep, ", ", ..., ")")
      },
      paste0 = dbplyr::sql_prefix("CONCAT"),
      substr = function(x, start, stop) {
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
        if (is.null(last)) {
          dbplyr::build_sql("SUBSTR(", x, ", ", first, ")")
        } else {
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
      str_detect = dbplyr::sql_prefix("REGEXP_CONTAINS", 2),
      str_extract = dbplyr::sql_prefix("REGEXP_EXTRACT", 2),
      str_replace = dbplyr::sql_prefix("REGEXP_REPLACE", 3),
      str_replace_all = dbplyr::sql_prefix("REGEXP_REPLACE", 3),
      str_trim = function(x, side = "both") {
        switch(
          side,
          "both" = dbplyr::build_sql("TRIM(", x, ")"),
          "left" = dbplyr::build_sql("LTRIM(", x, ")"),
          "right" = dbplyr::build_sql("RTRIM(", x, ")"),
          stop("Unknown side: ", side)
        )
      },
      str_to_lower = function(x) dbplyr::build_sql("LOWER(", x, ")"),
      str_to_upper = function(x) dbplyr::build_sql("UPPER(", x, ")"),
      str_to_title = function(x) dbplyr::build_sql("INITCAP(", x, ")"),
      str_c = dbplyr::sql_prefix("CONCAT"),
      str_length = function(x) dbplyr::build_sql("LENGTH(", x, ")"),
      str_sub = function(string, start = 1L, end = -1L) {
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
        # GoogleSQL REGEXP_REPLACE replaces all by default;
        # no single-replacement variant, so same translation
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

      # Conditional
      ifelse = dbplyr::sql_prefix("IF"),
      coalesce = dbplyr::sql_prefix("COALESCE"),

      # Parallel min/max
      pmax = dbplyr::sql_prefix("GREATEST"),
      pmin = dbplyr::sql_prefix("LEAST"),

      # Date/time
      Sys.date = dbplyr::sql_prefix("CURRENT_DATE"),
      Sys.time = dbplyr::sql_prefix("CURRENT_TIMESTAMP"),
      as.POSIXct = function(x) {
        dbplyr::build_sql("CAST(", x, " AS TIMESTAMP)")
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
      round = function(x, digits = 0) {
        dbplyr::build_sql("ROUND(", x, ", ", digits, ")")
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
      median = dbplyr::win_absent("median")
    )
  )
}

#' @importFrom dbplyr sql_query_fields
#' @export
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
