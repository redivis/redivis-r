#' @importFrom uuid UUIDgenerate
#' @importFrom arrow write_parquet write_dataset
#' @importFrom sf st_geometry st_as_text
#' @include Table.R User.R api_request.R
Notebook <- setRefClass("Notebook",
   fields = list(current_notebook_job_id="character"),
   methods = list(
     create_output_table = function(df, name = NULL, geography_variables = NULL, append = FALSE){
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

       folder <- str_interp('/${tempdir()}/redivis/out')

       if (!dir.exists(folder)) dir.create(folder, recursive = TRUE)

       temp_file_path <- str_interp('${folder}/${uuid::UUIDgenerate()}')

       if (is(df,"sf")){
         sf_column_name <- attr(df, "sf_column")
         if (is.null(geography_variables)){
           payload=base::append(
             payload,
             list("geographyVariables" = list(sf_column_name))
           )
         }
         wkt_geopoint <- sapply(sf::st_geometry(df), function(x) sf::st_as_text(x))
         sf::st_geometry(df) <- NULL
         df[sf_column_name] <- wkt_geopoint
       }

       if (is(df, "Dataset")){
         dir.create(temp_file_path)
         arrow::write_dataset(df, temp_file_path, format='parquet', max_partitions=1, basename_template="part-{i}.parquet")
         temp_file_path <- str_interp('${temp_file_path}/part-0.parquet')
         file_path = temp_file_path
       } else {
          write_parquet(df, sink=temp_file_path)
          file_path = temp_file_path
       }

       file_size <- base::file.info(temp_file_path)$size

       res <- make_request(
         method="POST",
         path=str_interp("/notebookJobs/${current_notebook_job_id}/tempUploads"),
         payload=list(tempUploads=list(list(size=file_size, resumable=TRUE))) # file_size>5e7
       )

       temp_upload = res$results[[1]]

       if (temp_upload$resumable) {
         perform_resumable_upload(file_path=temp_file_path, temp_upload_url=temp_upload$url)
       } else {
         perform_standard_upload(
           file_path=temp_file_path,
           temp_upload_url=temp_upload$url,
           proxy_url=str_interp("${Sys.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/${current_notebook_job_id}/tempUploadProxy")
           )
       }

       payload <- base::append(payload, list(tempUploadId=temp_upload$id))

       res <- make_request(method='PUT', path=str_interp("/notebookJobs/${current_notebook_job_id}/outputTable"), payload = payload)

       base::file.remove(temp_file_path)

       user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_PROJECT"), "[.]"))[1]
       project_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_PROJECT"), "[.]"))[2]
       table <- User$new(name=user_name)$project(name=project_name)$table(name=res$name)

       table$properties <- res

       table
     }
   )
)
