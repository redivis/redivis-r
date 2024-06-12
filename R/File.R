
#' @include api_request.R util.R
File <- setRefClass(
  "File",
  fields = list(id = "character"),

  methods = list(
    show = function(){
      print(str_interp("<File ${.self$id}>"))
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
        on.exit(close(con))
      }

      res <- make_request(method="GET", path=str_interp("/rawFiles/${id}"), query=list(allowRedirect="true"), parse_response=FALSE, get_download_path_callback=get_download_path_callback, stream_callback=stream_callback)
      if (is_dir){
        return(res)
      } else {
        return(file_name)
      }
    },

    read = function(as_text = FALSE) {
      res <- make_request(method="GET", path=str_interp("/rawFiles/${id}"), parse_response = FALSE)
      httr::content(res, as = if(as_text) 'text' else 'raw')
    },

    stream = function(callback) {
      make_request(method="GET", path=str_interp("/rawFiles/${id}"), parse_response = FALSE, stream_callback = callback)
    }
  )
)
