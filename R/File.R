
#' @importFrom stringr str_interp str_sub
#' @importFrom httr headers content
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
        path <- str_sub(path,1,nchar(path)-1) # remove trailing "/", as this screws up file.path()
      } else if (dir.exists(path)) {
        is_dir <- TRUE
      }

      file_name <- path

      if (is_dir){
        header_res <- make_request(method="HEAD", path=str_interp("/rawFiles/${id}"), parse_response = FALSE)
        name <- headers(header_res)$'x-redivis-filename'
        file_name <- file.path(path, name)
      }

      if (!overwrite && file.exists(file_name)){
        stop(str_interp("File already exists at '${file_name}'. Set parameter overwrite=TRUE to overwrite existing files."))
      }

      target_dir <- dirname(file_name)

      # Make sure output directory exists
      if (!dir.exists(target_dir)) dir.create(target_dir, recursive = TRUE)

      res <- make_request(method="GET", path=str_interp("/rawFiles/${id}"), parse_response = FALSE, download_path = file_name, download_overwrite = overwrite)

      file_name
    },

    read = function(as_text = FALSE) {
      res <- make_request(method="GET", path=str_interp("/rawFiles/${id}"), parse_response = FALSE)
      content(res, as = if(as_text) 'text' else 'raw')
    },

    stream = function(callback) {
      make_request(method="GET", path=str_interp("/rawFiles/${id}"), parse_response = FALSE, stream_callback = callback)
    }
  )
)
