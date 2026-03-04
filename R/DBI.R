#' @include Query.R Dataset.R Workflow.R Table.R Variable.R

# ===========================================================================
# DBI / dbplyr connector for Redivis (read-only)
# ===========================================================================
#
# Usage:
#   library(dplyr)
#   library(dbplyr)
#   con <- DBI::dbConnect(redivis::RedivisDBI(), dataset = "owner.dataset:v1.0")
#   con <- DBI::dbConnect(redivis::RedivisDBI(), workflow = "owner.workflow")
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
setClass("RedivisConnection",
  contains = "DBIConnection",
  slots = list(
    owner = "ANY",
    owner_type = "character",
    disconnected = "logical"
  )
)

#' @export
setMethod("dbConnect", "RedivisDriver",
  function(drv, dataset = NULL, workflow = NULL, ...) {
    if (!is.null(dataset) && !is.null(workflow)) {
      stop("Specify either `dataset` or `workflow`, not both.")
    }
    if (is.null(dataset) && is.null(workflow)) {
      stop("Must specify `dataset` or `workflow`.")
    }

    if (!is.null(dataset)) {
      if (is.character(dataset)) {
        owner <- Dataset$new(name = dataset)
      } else {
        owner <- dataset
      }
      owner_type <- "dataset"
    } else {
      if (is.character(workflow)) {
        owner <- Workflow$new(name = workflow)
      } else {
        owner <- workflow
      }
      owner_type <- "workflow"
    }

    new("RedivisConnection",
      owner = owner,
      owner_type = owner_type,
      disconnected = FALSE
    )
  }
)

#' @export
setMethod("show", "RedivisConnection", function(object) {
  status <- if (object@disconnected) " (disconnected)" else ""
  cat(
    "<RedivisConnection> ",
    object@owner$qualified_reference,
    status, "\n",
    sep = ""
  )
})

#' @export
setMethod("dbGetInfo", "RedivisConnection", function(dbObj, ...) {
  list(
    db.version = NA_character_,
    dbname = dbObj@owner$qualified_reference,
    owner_type = dbObj@owner_type
  )
})

#' @export
setMethod("dbIsValid", "RedivisConnection", function(dbObj, ...) {
  !dbObj@disconnected
})

#' @export
setMethod("dbDisconnect", "RedivisConnection", function(conn, ...) {
  conn@disconnected <- TRUE
  invisible(TRUE)
})

# --- Querying --------------------------------------------------------------

#' @export
setMethod("dbSendQuery", signature("RedivisConnection", "character"),
  function(conn, statement, ...) {
    assert_connected(conn)

    q <- if (conn@owner_type == "dataset") {
      Query$new(
        query = statement,
        default_dataset = conn@owner$qualified_reference
      )
    } else {
      Query$new(
        query = statement,
        default_workflow = conn@owner$qualified_reference
      )
    }

    new("RedivisResult",
      connection = conn,
      query = q,
      fetched = FALSE
    )
  }
)

#' @export
setMethod("dbGetQuery", signature("RedivisConnection", "character"),
  function(conn, statement, ...) {
    assert_connected(conn)
    res <- dbSendQuery(conn, statement, ...)
    on.exit(dbClearResult(res))
    dbFetch(res)
  }
)

# --- Table introspection ---------------------------------------------------

#' @export
setMethod("dbListTables", "RedivisConnection", function(conn, ...) {
  assert_connected(conn)
  tables <- conn@owner$list_tables()
  vapply(tables, function(t) t$name, character(1))
})

#' @export
setMethod("dbExistsTable", signature("RedivisConnection", "character"),
  function(conn, name, ...) {
    assert_connected(conn)
    tryCatch(
      conn@owner$table(name)$exists(),
      error = function(e) FALSE
    )
  }
)

#' @export
setMethod("dbListFields", signature("RedivisConnection", "character"),
  function(conn, name, ...) {
    assert_connected(conn)
    tbl <- conn@owner$table(name)
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
  signature("RedivisConnection", "character", "data.frame"),
  function(conn, name, fields, ...) {
    stop("RedivisConnection is read-only. Creating tables is not supported.")
  }
)

# --- Result ----------------------------------------------------------------

#' @export
setClass("RedivisResult",
  contains = "DBIResult",
  slots = list(
    connection = "RedivisConnection",
    query = "ANY",
    fetched = "logical"
  )
)

#' @export
setMethod("show", "RedivisResult", function(object) {
  status <- if (object@fetched) "fetched" else "pending"
  cat("<RedivisResult> [", status, "]\n", sep = "")
})

#' @export
setMethod("dbFetch", "RedivisResult", function(res, n = -1, ...) {
  if (n > 0) {
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
  query_wait_for_finish(res@query)
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

#' @importFrom dbplyr sql_translation
#' @export
sql_translation.RedivisConnection <- function(con) {
  dbplyr::sql_variant(
    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      # GoogleSQL-specific scalar overrides
      paste = function(..., sep = " ") {
        dbplyr::build_sql("CONCAT_WS(", sep, ", ", ..., ")")
      },
      paste0 = function(...) {
        dbplyr::build_sql("CONCAT(", ..., ")")
      },
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
        dbplyr::build_sql("CAST(", x, " AS BOOL)")
      },
      as.Date = function(x) {
        dbplyr::build_sql("CAST(", x, " AS DATE)")
      },
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
      ifelse = function(test, yes, no) {
        dbplyr::build_sql("IF(", test, ", ", yes, ", ", no, ")")
      }
    ),
    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("COUNT(*)"),
      var = function(x, na.rm = FALSE) {
        dbplyr::build_sql("VAR_SAMP(", x, ")")
      },
      sd = function(x, na.rm = FALSE) {
        dbplyr::build_sql("STDDEV_SAMP(", x, ")")
      },
      cor = function(x, y) {
        dbplyr::build_sql("CORR(", x, ", ", y, ")")
      },
      all = function(x, na.rm = FALSE) {
        dbplyr::build_sql("LOGICAL_AND(", x, ")")
      },
      any = function(x, na.rm = FALSE) {
        dbplyr::build_sql("LOGICAL_OR(", x, ")")
      }
    ),
    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      n = function() dbplyr::sql("COUNT(*)")
    )
  )
}

#' @importFrom dbplyr sql_query_fields
#' @export
sql_query_fields.RedivisConnection <- function(con, table, ...) {
  dbplyr::build_sql(
    "SELECT * FROM ", table, " WHERE FALSE LIMIT 0",
    con = con
  )
}

# --- Helpers ---------------------------------------------------------------

assert_connected <- function(conn) {
  if (conn@disconnected) {
    stop("Connection has been disconnected.")
  }
}

redivis_type_to_r <- function(type) {
  switch(type %||% "unknown",
    "integer"   = "numeric",
    "float"     = "double",
    "boolean"   = "logical",
    "date"      = "Date",
    "dateTime"  = "POSIXct",
    "time"      = "character",
    "geography" = "character",
    "string"    = "character",
    "character"
  )
}
