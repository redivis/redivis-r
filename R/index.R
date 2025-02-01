#' @importFrom stringr str_interp


#' @title query
#'
#' @description Create a Redivis SQL query
#'
#' @param query The query string to execute
#'
#' @return class<Query>
#' @examples
#' output_table <- redivis::query(query = 'SELECT 1 + 1 AS two')$to_tibble()
#' @export
query <- function(query="", default_workflow=NULL, default_dataset=NULL) {
  if (is.null(default_workflow) && is.null(default_dataset)){
    default_workflow <- Sys.getenv("REDIVIS_DEFAULT_WORKFLOW")
    default_dataset <- Sys.getenv("REDIVIS_DEFAULT_DATASET")
  }
  Query$new(query=query, default_workflow=default_workflow, default_dataset=default_dataset)
}


#' @title user
#'
#' @description Reference user-owned resources on Redivis
#'
#' @param name The user's username
#'
#' @return class<user>
#' @examples
#' redivis::user('my_username')$workflow('my_workflow')$table('my_table')$to_tibble(max_results=100)
#'
#' We can also construct a query scoped to a particular workflow, removing the need to fully qualify table names
#' redivis::user('my_username')$workflow('my_workflow')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")$to_tibble()
#' @export
user <- function(name){
  User$new(name=name)
}


#' @title organization
#'
#' @description Reference organization-owned resources on Redivis
#'
#' @param name The organization's username
#'
#' @return class<user>
#' @examples
#' redivis::organization('demo_organization')$dataset('some_dataset')$table('a_table')$to_tibble(max_results=100)
#'
#' We can also construct a query scoped to a particular dataset, removing the need to fully qualify table names
#' redivis::user('my_username')$workflow('my_workflow')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")$to_tibble()
#' @export
organization <- function(name){
  Organization$new(name=name)
}

#' @title table
#'
#' @description Reference a specific table when the REDIVIS_DEFAULT_WORKFLOW or REDIVIS_DEFAULT_DATASET env variable is set
#'
#' @param name The table's username
#'
#' @return class<table>
#' @examples
#' redivis::table('a_table')$to_tibble(max_results=100)
#'

#' @export
table <- function(name){
  if (Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID") != ""){
    Table$new(name=name)
  } else if (Sys.getenv("REDIVIS_DEFAULT_WORKFLOW") != ""){
    user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[1]
    workflow_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_WORKFLOW"), "[.]"))[2]
    User$new(name=user_name)$workflow(name=workflow_name)$table(name=name)

  }else if (Sys.getenv("REDIVIS_DEFAULT_DATASET") != ""){
    user_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_DATASET"), "[.]"))[1]
    dataset_name <- unlist(strsplit(Sys.getenv("REDIVIS_DEFAULT_DATASET"), "[.]"))[2]

    User$new(name=user_name)$dataset(name=dataset_name)$table(name=name)
  }
  else{
    stop("Cannot reference an unqualified table if the neither the REDIVIS_DEFAULT_WORKFLOW or REDIVIS_DEFAULT_DATASET environment variables are set.")
  }
}

#' @title file
#'
#' @description Reference a file stored on Redivis
#'
#' @param id The id of the file
#'
#' @return class<File>
#' @examples
#' file <- redivis::file("s335-8ey8zt7bx.qKmzpdttY2ZcaLB0wbRB7A")$download()
#' @export
file <- function(id) {
  File$new(id=id)
}


#' @title current_notebook
#'
#' @description A reference to the current Redivis notebook. Will be NULL if not running in a Redivis notebook environment.
#'
#' @return class<Notebook>
#' @examples
#' redivis::current_notebook()$create_output_table(df)
#' @export
current_notebook <- function() {
  if(Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID") != "") {
    Notebook$new(current_notebook_job_id=Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID"))
  }else {
    NULL
  }
}

#' @title authenticate
#'
#' @description Manually authenticate the current session with your Redivis credentials. Authentication normally happens automatically, and this method does not need to be called directly in most use cases.
#'
#' @return void
#' @examples
#' redivis::authenticate(force_reauthentication=FALSE)
authenticate <- function(scope=NULL, force_reauthentication=FALSE) {
  if (force_reauthentication){
    clear_cached_credentials()
  }
  if (is.character(scope)){
    scope <- list(scope)
  }
  get_auth_token(scope=scope)
  invisible(NULL)
}


#' @title redivis
#'
#' @description Redivis wrapper, all primary methods accessible via $
#'
#' @return list
#' @examples
#' dataset <- redivis$user("username")$dataset("dataset_name")
#' @export
redivis <- list(
  "query"=query,
  "user"=user,
  "organization"=organization,
  "file"=file,
  "table"=table,
  "current_notebook"=current_notebook,
  "authenticate"=authenticate
)


