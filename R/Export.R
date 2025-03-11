#' @include Table.R api_request.R util.R
Export <- setRefClass("Export",
  fields = list(table="ANY", properties="list", uri="character"),
  methods = list(
    initialize = function(..., properties=list()){
      callSuper(...,
                uri=properties$uri,
                properties=properties
      )
    },
    show = function(){
      print(str_interp("<Export `${.self$uri}` on ${.self$table}>"))
    },

    get = function(wait_for_statistics=FALSE) {
      .self$properties = make_request(path=.self$uri)
      .self$uri = .self$properties$uri
      .self
    },

    download_files = function(path = NULL, overwrite = FALSE, progress=TRUE) {
      if (progress){
        progressr::with_progress(.self$wait_for_finish())
      } else {
        .self$wait_for_finish()
      }


      file_count <- .self$properties$fileCount
      is_dir <- FALSE
      if (is.null(path) || (file.exists(path) && file.info(path)$isdir)) {
        is_dir <- TRUE
        if (is.null(path)) {
          path <- getwd()
        }
        if (file_count > 1) {
          if (is.null(.self$table$properties$name)) {
            .self$table$get()
          }
          escaped_table_name <- tolower(stringr::str_replace_all(.self$table$properties$name, "\\W+", "_"))
          path <- file.path(path, escaped_table_name)
        }
      } else if (grepl("[/\\]$", path) || (!file.exists(path) && !grepl("\\.", path))) {
        is_dir <- TRUE
      } else if (file_count > 1) {
        stop(sprintf("Path '%s' is a file, but the export consists of multiple files. Please specify the path to a directory", path))
      }

      if (!overwrite && file.exists(path) && (!is_dir || file_count > 1)) {
        stop(sprintf("File already exists at '%s'. Set parameter overwrite=TRUE to overwrite existing files.", path))
      }

      if (is_dir) {
        dir.create(path, showWarnings = FALSE, recursive = TRUE)
      } else {
        dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
      }

      # IMPORTANT: progress isn't currently working here
      if (progress && FALSE){
        progressr::with_progress(perform_parallel_export_download(uri=.self$uri, download_path=path, file_count=file_count, is_dir=is_dir, overwrite=overwrite, total_size=.self$properties$size))
      } else {
        perform_parallel_export_download(uri=.self$uri, download_path=path, file_count=file_count, is_dir=is_dir, overwrite=overwrite, total_size=.self$properties$size)
      }

    },

    wait_for_finish = function() {
      iter_count <- 0
      pb <- progressr::progressor(steps = 100)
      pb(message="Preparing download...")
      previous_progress=0
      while (TRUE) {
        if (.self$properties$status == "completed") {
          if (previous_progress != 100){
            pb(amount=100 - previous_progress, message="Preparing download...")
          }
          break
        } else if (.self$properties$status == "failed") {
          stop(sprintf("Export job failed with message: %s", .self$properties$errorMessage))
        } else if (.self$properties$status == "cancelled") {
          stop("Export job was cancelled")
        } else {
          iter_count <- iter_count + 1
          if (round(.self$properties$percentCompleted) > previous_progress){
            pb(amount=round(.self$properties$percentCompleted) - previous_progress, message="Preparing download...")
            previous_progress <- round(.self$properties$percentCompleted)
          }
          Sys.sleep(min(iter_count * 0.5, 2))
          .self$get()
        }
      }
    }
  )
)


perform_parallel_export_download <- function(uri, file_count, download_path, is_dir, overwrite, total_size){
  pb <- progressr::progressor(steps=total_size)

  output_file_paths <- list()
  worker_count <- min(8, parallelly::availableCores(), file_count)
  if (parallelly::supportsMulticore()){
    oplan <- future::plan(future::multicore, workers = worker_count)
  } else {
    oplan <- future::plan(future::multisession, workers = worker_count)
    # Helpful for testing in dev
    # oplan <- future::plan(future::sequential)
  }
  # This avoids overwriting any future strategy that may have been set by the user, resetting on exit
  on.exit(future::plan(oplan), add = TRUE)

  output_file_paths <- furrr::future_map(1:file_count, function(file_number){
    headers_callback <- NULL
    final_path <- download_path
    con <- NULL

    if (is_dir){
      headers_callback <- function(headers){
        name <- get_filename_from_content_disposition(headers$'content-disposition')
        final_path <<- file.path(download_path, name)
        if (!overwrite && base::file.exists(final_path)){
          stop(str_interp("File already exists at '${final_path}'. Set parameter overwrite=TRUE to overwrite existing files."))
        }
        con <<- base::file(final_path, "w+b")
      }
    } else {
      con <- base::file(final_path, "w+b")
    }


    current_progress_bytes <- 0
    last_measured_progress = Sys.time()
    stream_callback = function(chunk){
      current_progress_bytes <<- current_progress_bytes + length(chunk)
      if(Sys.time() - last_measured_progress > 0.2){
        pb(amount=current_progress_bytes)
        current_progress_bytes <<- 0
        last_measured_progress <<- Sys.time()
      }
      writeBin(chunk, con)
    }
    on.exit({
      if (!is.null(con) && base::isOpen(con)) {
        close(con)
      }
    }, add=TRUE)

    query_params = list()
    if (!is.null(file_number)){
      query_params <- append(query_params, list(filePart=file_number - 1))
    }
    res <- make_request(method="GET", path=sprintf("%s/download", uri), query=query_params, parse_response=FALSE, headers_callback=headers_callback, stream_callback=stream_callback)

    return(final_path)
  })

  return(output_file_paths)
}
