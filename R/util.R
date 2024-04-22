
#' @importFrom arrow schema
#' @importFrom purrr map set_names
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
