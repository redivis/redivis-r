# The data that the live tests upload, and what they expect to read back.

tiny_csv_path <- function() {
  testthat::test_path("fixtures", "tiny.csv")
}

# The contents of tiny.csv, as Redivis should read them
tiny_expected <- function() {
  read_csv_file(tiny_csv_path(), col_classes = tiny_col_classes)
}

tiny_col_classes <- c(
  id = "integer",
  Numeric = "integer",
  Key = "character",
  Timestamp = "character",
  Year = "integer",
  Boolean = "logical",
  Text = "character"
)

# The columns of tiny.csv, to select in SQL. An unreleased table also has
# columns identifying the upload each row came from.
tiny_columns_sql <- function() {
  paste(names(tiny_col_classes), collapse = ", ")
}

# Reads a CSV the way Redivis writes one, keeping empty strings as such
read_csv_file <- function(path, col_classes = tiny_col_classes) {
  utils::read.csv(
    path,
    colClasses = col_classes,
    na.strings = character(),
    encoding = "UTF-8"
  )
}

# The Redivis type of each column of tiny.csv
tiny_variable_types <- c(
  id = "integer",
  Numeric = "integer",
  Key = "string",
  Timestamp = "string",
  Year = "integer",
  Boolean = "boolean",
  Text = "string"
)

# A column of each of the scalar types that Redivis supports, including nulls
# and the sorts of strings that trip up parsers
typed_data <- function() {
  data.frame(
    id = 1:5,
    float_col = c(1.5, -2.25, NA, 0, 1e10),
    string_col = c("plain", "with, comma", 'with "quotes"', "multi\nline", NA),
    unicode_col = c(
      "héllo",
      "日本語",
      "emoji \U0001F389",
      "tab\there",
      NA
    ),
    bool_col = c(TRUE, FALSE, NA, TRUE, FALSE),
    date_col = as.Date(c(
      "2020-01-01",
      "1999-12-31",
      NA,
      "2024-02-29",
      "1970-01-01"
    )),
    datetime_col = as.POSIXct(
      c(
        "2020-01-01 12:30:15.123456",
        "1999-12-31 23:59:59",
        "2024-02-29 00:00:00",
        NA,
        "1970-01-01 00:00:00"
      ),
      tz = "UTC"
    ),
    stringsAsFactors = FALSE
  )
}

# The Redivis type of each column of typed_data()
typed_variable_types <- c(
  id = "integer",
  float_col = "float",
  string_col = "string",
  unicode_col = "string",
  bool_col = "boolean",
  date_col = "date",
  datetime_col = "dateTime"
)

geo_features <- list(
  list(name = "point", value = 1L, type = "Point", coordinates = c(-122.17, 37.43)),
  list(
    name = "line",
    value = 2L,
    type = "LineString",
    coordinates = list(c(-122, 37), c(-121, 38))
  ),
  list(
    name = "polygon",
    value = 3L,
    type = "Polygon",
    coordinates = list(list(c(-122, 37), c(-121, 37), c(-121, 38), c(-122, 37)))
  )
)

geojson_raw <- function() {
  features <- lapply(geo_features, function(feature) {
    list(
      type = "Feature",
      geometry = list(type = feature$type, coordinates = feature$coordinates),
      properties = list(name = feature$name, value = feature$value)
    )
  })
  charToRaw(as.character(jsonlite::toJSON(
    list(type = "FeatureCollection", features = features),
    auto_unbox = TRUE,
    digits = NA
  )))
}

# The files uploaded to the file index table, by path
raw_files <- function() {
  list(
    "readme.txt" = charToRaw(enc2utf8(
      "A file for testing.\nIt has a few lines,\nincluding ünïcödé.\n"
    )),
    "data/numbers.csv" = charToRaw(paste0(0:999, ",", (0:999)^2, "\n", collapse = "")),
    # Large enough to need several reads when streamed
    "data/nested/random.bin" = withr::with_seed(
      0,
      as.raw(sample.int(256, 3 * 2^20, replace = TRUE) - 1L)
    ),
    "with space & symbols.txt" = charToRaw("named awkwardly")
  )
}

write_files <- function(directory, files) {
  for (path in names(files)) {
    file_path <- file.path(directory, path)
    dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
    writeBin(files[[path]], file_path)
  }
  invisible(directory)
}

# The files under a directory, by their paths relative to it
read_files <- function(directory) {
  paths <- list.files(directory, recursive = TRUE, all.files = TRUE)
  stats::setNames(
    lapply(paths, function(path) {
      readBin(file.path(directory, path), "raw", file.size(file.path(directory, path)))
    }),
    paths
  )
}

variable_types <- function(variables) {
  stats::setNames(
    vapply(variables, function(v) v$properties$type, character(1)),
    vapply(variables, function(v) v$name, character(1))
  )
}

small_csv <- charToRaw("id,name,value\n1,a,1.5\n2,b,2.5\n3,c,\n")
jagged_csv <- charToRaw("a,b\n1,2\n3\n4,5\n")

# Rows come back in no particular order, so compare them sorted, as plain data
# frames
sorted_by <- function(df, column = "id") {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  df <- df[order(df[[column]]), , drop = FALSE]
  rownames(df) <- NULL
  df
}

# Compares a table's rows to the expected ones, regardless of order or the kind
# of data frame. Datetimes are compared as instants, whatever their time zone.
expect_rows_equal <- function(object, expected, by = "id") {
  act <- testthat::quasi_label(rlang::enquo(object), arg = "object")
  normalize <- function(df) {
    df <- sorted_by(df, by)
    df[] <- lapply(df, function(column) {
      if (inherits(column, "POSIXct")) attr(column, "tzone") <- "UTC"
      column
    })
    df
  }
  testthat::expect_equal(
    normalize(act$val),
    normalize(expected),
    label = act$lab,
    expected.label = "expected"
  )
}
