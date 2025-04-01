#' @include Table.R User.R api_request.R util.R
Notebook <- setRefClass("Notebook",
   fields = list(current_notebook_job_id="character"),
   methods = list(
     create_output_table = function(data, name = NULL, geography_variables = NULL, append = FALSE){
       payload = list()
       if (!is.null(name)){
         payload=base::append(
           payload,
           list("name"= name)
         )
       }
       if (!is.null(geography_variables)){
         payload=base::append(
           payload,
           list("geographyVariables" = geography_variables)
         )
       }
       if (append){
         payload=base::append(
           payload,
           list("append" = "TRUE")
         )
       }

       should_remove_tempfile <- TRUE
       temp_file_path <- NULL

       if (is.character(data)){
         if (endsWith(data, ".parquet")) {
           should_remove_tempfile <- FALSE
           temp_file_path <- data
         } else {
           stop("Only paths to parquet files (ending in .parquet) are supported when a string argument is provided")
         }
       } else {
          if (is(data,"sf")){
            sf_column_name <- attr(data, "sf_column")
            if (is.null(geography_variables)){
              payload=base::append(
              payload,
              list("geographyVariables" = list(sf_column_name))
              )
            }
          }
         temp_file_path <- convert_data_to_parquet(data)
       }

       file_size <- base::file.info(temp_file_path)$size

       res <- make_request(
         method="POST",
         path=str_interp("/notebookJobs/${current_notebook_job_id}/tempUploads"),
         payload=list(tempUploads=list(list(size=file_size, resumable=file_size>5e7)))
       )

       temp_upload = res$results[[1]]

       con <- base::file(temp_file_path, "rb")
       on.exit(close(con), add=TRUE)

       if (temp_upload$resumable) {
         perform_resumable_upload(
           data=con,
           temp_upload_url=temp_upload$url,
           proxy_url=str_interp("${Sys.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/${current_notebook_job_id}/tempUploadProxy")
         )
       } else {
         perform_standard_upload(
           data=con,
           temp_upload_url=temp_upload$url,
           proxy_url=str_interp("${Sys.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/${current_notebook_job_id}/tempUploadProxy")
           )
       }

       payload <- base::append(payload, list(tempUploadId=temp_upload$id))

       res <- make_request(method='PUT', path=str_interp("/notebookJobs/${current_notebook_job_id}/outputTable"), payload = payload)

       if (should_remove_tempfile){
         base::file.remove(temp_file_path)
       }

       user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[1]
       workflow_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[2]
       table <- User$new(name=user_name)$workflow(name=workflow_name)$table(name=res$name)

       table$properties <- res

       table
     }
   )
)
