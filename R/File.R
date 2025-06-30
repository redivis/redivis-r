
#' @include api_request.R util.R
File <- setRefClass(
  "File",
  fields = list(
    id = "character",
    properties="list",
    table="ANY"
  ),
  methods = list(
    initialize = function(..., id="", properties=list(), table=NULL){
      populated_properties = append(properties, list(kind="rawFile", id=id, uri=str_interp("/rawFiles/${id}")))
      callSuper(...,
        id=id,
        properties=populated_properties,
        table=table
      )
    },

    show = function(){
      if (!is.null(.self$properties$name)){
        print(str_interp("<File name=${.self$properties$name}, id=${.self$id})>"))
      } else {
        print(str_interp("<File id=${.self$id}>"))
      }

    },

    get = function(){
      res <- make_request(method="HEAD", path=str_interp("/rawFiles/${.self$id}"))
      parse_file_headers(.self, httr::headers(res))
    },

    download = function(path = NULL, overwrite = FALSE) {
      is_dir = FALSE

      if (is.null(path)) {
        path <- getwd()
        is_dir <- TRUE
      } else if (endsWith(path, '/')) {
        is_dir <- TRUE
        path <- stringr::str_sub(path,1,nchar(path)-1) # remove trailing "/", as this screws up file.path()
      } else if (dir.exists(path)) {
        is_dir <- TRUE
      }

      file_name <- path

      target_dir <- dirname(file_name)

      # Make sure output directory exists
      if (!dir.exists(target_dir)) {
        dir.create(target_dir, recursive = TRUE)
      }

      get_download_path_callback <- NULL
      stream_callback <- NULL
      if (is_dir){
        get_download_path_callback <- function(headers){
          name <- get_filename_from_content_disposition(headers$'content-disposition')
          file_name <- file.path(path, name)
          if (!overwrite && base::file.exists(file_name)){
            stop(str_interp("File already exists at '${file_name}'. Set parameter overwrite=TRUE to overwrite existing files."))
          }
          return(file_name)
        }
      } else {
        if (!overwrite && base::file.exists(file_name)){
          stop(str_interp("File already exists at '${file_name}'. Set parameter overwrite=TRUE to overwrite existing files."))
        }
        con <- base::file(base::file.path(path, name), "w+b")
        stream_callback = function(){
          writeBin(chunk, con)
        }
        on.exit(close(con), add=TRUE)
      }

      res <- make_request(method="GET", path=str_interp("/rawFiles/${.self$id}"), parse_response=FALSE, get_download_path_callback=get_download_path_callback, stream_callback=stream_callback)
      if (is_dir){
        return(res)
      } else {
        return(file_name)
      }
    },

    read = function(as_text = FALSE, start_byte=0, end_byte=NULL) {
      res <- make_request(
        method="GET",
        path=str_interp("/rawFiles/${.self$id}"),
        parse_response = FALSE,
        start_byte=start_byte,
        end_byte=end_byte
      )
      httr::content(res, as = if(as_text) 'text' else 'raw')
    },

    stream = function(callback, start_byte=0, end_byte=NULL) {
      make_request(
        method="GET",
        path=str_interp("/rawFiles/${.self$id}"),
        parse_response = FALSE,
        stream_callback = callback,
        start_byte=start_byte,
        end_byte=end_byte
      )
    }
  )
)

parse_file_headers <- function(file, headers) {
  file$properties$name <- get_filename_from_content_disposition(headers[["content-disposition"]])
  file$properties$contentType <- headers[["content-type"]]

  digest = if (!is.null(headers[["Digest"]])) headers[["Digest"]] else headers[["x-goog-hash"]]

  if (!is.null(digest)) {
    file$properties$md5 <- sub("md5=", "", digest)
  }

  file$properties$size <- as.integer(dplyr::coalesce(headers[["x-redivis-size"]], headers[["x-goog-stored-content-length"]], headers[["content-length"]]))
}
