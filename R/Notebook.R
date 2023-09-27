#' @importFrom uuid UUIDgenerate
#' @importFrom arrow write_parquet
#' @importFrom httr upload_file
Notebook <- setRefClass("Notebook",
   fields = list(current_notebook_job_id="character"),
   methods = list(
     create_output_table = function(df, name = NULL, geography_variables = NULL, append = FALSE){
       query = list()
       if (!is.null(name)){
         query=base::append(
           query,
           list("name"= name)
         )
       }
       if (!is.null(geography_variables)){
         query=base::append(
           query,
           list("geographyVariables" = geography_variables)
         )
       }
       if (append){
         query=base::append(
           query,
           list("append" = "TRUE")
         )
       }

       folder <- '/tmp/redivis/out'

       if (!dir.exists(folder)) dir.create(folder, recursive = TRUE)

       temp_file_path <- str_interp('${folder}/${uuid::UUIDgenerate()}')

       if(is.character(df)){
         file_path = df
         temp_file_path = NULL
       } else {
          write_parquet(df, sink=temp_file_path)
          file_path = temp_file_path
       }

       make_request(method='PUT', path=str_interp("/notebookJobs/${current_notebook_job_id}/outputTable"), query = query, payload = upload_file(upload_file_path) )

       if (!is.null(temp_file_path)){
         file.remove(temp_file_path)
       }
     }
   )
)
