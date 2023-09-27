#' @importFrom uuid UUIDgenerate
#' @importFrom arrow write_parquet write_dataset
#' @importFrom httr upload_file
#' @importFrom sf st_geometry st_as_text
#' @include Table.R
#' @include User.R
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

       if (is(df,"sf")){
         sf_column_name <- attr(df, "sf_column")
         if (is.null(geography_variables)){
           query=base::append(
             query,
             list("geographyVariables" = list(sf_column_name))
           )
         }
         wkt_geopoint <- sapply(sf::st_geometry(df), function(x) sf::st_as_text(x))
         sf::st_geometry(df) <- NULL
         df[sf_column_name] <- wkt_geopoint
       }

       if(is.character(df)){
         file_path = df
         temp_file_path = NULL
       } else if (is(df, "Dataset")){
         dir.create(temp_file_path)
         arrow::write_dataset(df, temp_file_path, format='parquet', max_partitions=1, basename_template="part-{i}.parquet")
         temp_file_path <- str_interp('${temp_file_path}/part-0.parquet')
         file_path = temp_file_path
       } else {
          write_parquet(df, sink=temp_file_path)
          file_path = temp_file_path
       }

       res <- make_request(method='PUT', path=str_interp("/notebookJobs/${current_notebook_job_id}/outputTable"), query = query, payload = upload_file(file_path) )

       if (!is.null(temp_file_path)){
         file.remove(temp_file_path)
       }

       user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_PROJECT"), "[.]"))[1]
       project_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_PROJECT"), "[.]"))[2]
       table <- User$new(name=user_name)$project(name=project_name)$table(name=res$name)
       table$properties <- res

       table
     }
   )
)
