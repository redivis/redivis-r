#' @import tibble
#' @importFrom hms as_hms
rows_to_tibble <- function(rows, variables){
  df <- as_tibble(matrix(unlist(rows), nrow=length(rows), byrow=TRUE), .name_repair="minimal")
  colnames(df) <- Map(function(variable) variable$name, variables)

  for (variable in variables){
    if (variable$type == 'integer'){
      df[[variable$name]] <- as.integer64(df[[variable$name]])
      max_val <- max(df[[variable$name]])
      min_val <- min(df[[variable$name]])
      if ((is.na(max_val) || max_val <= 2147483647) && (is.na(min_val) || min_val >= -2147483648)){
        df[[variable$name]] <- as.integer(df[[variable$name]])
      }
    } else if (variable$type == 'float'){
      df[[variable$name]] <- as.double(df[[variable$name]])
    } else if (variable$type == 'boolean'){
      df[[variable$name]] <- as.logical(df[[variable$name]])
    } else if (variable$type == 'date'){
      df[[variable$name]] <- as.Date(df[[variable$name]])
    } else if (variable$type == 'dateTime'){
      df[[variable$name]] <- as.POSIXlt(df[[variable$name]])
    } else if (variable$type == 'time'){
      df[[variable$name]] <- as_hms(df[[variable$name]])
    }
  }

  df
}
