
#' @title user
#'
#' @description Reference user-owned resources on Redivis
#'
#' @param name The user's username
#'
#' @return class<user>
#' @examples
#' redivis::user('my_username')$project('my_project')$table('my_table')$to_dataframe(limit=100)
#'
#' We can also construct a query scoped to a particular project, removing the need to fully qualify table names
#' redivis::user('my_username')$project('my_project')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")
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
#' redivis::organization('demo_organization')$dataset('some_dataset')$table('a_table')$to_dataframe(limit=100)
#'
#' We can also construct a query scoped to a particular dataset, removing the need to fully qualify table names
#' redivis::user('my_username')$project('my_project')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")
#' @export
organization <- function(name){
  Organization$new(name=name)
}


User <- setRefClass("User",
  fields = list(name="character"),
  methods = list(
    dataset = function(name, version="current") {
      Dataset$new(name=name, version=version, user=.self)
    },
    project = function(name) {
      Project$new(name=name, user=.self)
    }
  )
)

Organization <- setRefClass("Organization",
  fields = list(name="character"),
  methods = list(
    dataset = function(name, version="current") {
      Dataset$new(name=name, version=version, organization=.self)
    }
  )
)

Dataset <- setRefClass("Dataset",
  fields = list(name="character", version="character", user="User", organization="Organization"),
  methods = list(
    get_identifier = function(){
      owner <- if(is.null(user)) organization else user
      str_interp("${owner$name}.${name}")
    },
    table = function(name) {
      Table$new(name=name, dataset=.self)
    },
    query = function(query){
      redivis::query(query, default_dataset = .self)
    }
  )
)

Project <- setRefClass("Project",
  fields = list(name="character", user="User"),
  methods = list(
    get_identifier = function(){
      str_interp("${user$name}.${name}")
    },
    table = function(name) {
      Table(name=name, project=.self)
    },
    query = function(query){
      redivis::query(query, default_project = .self)
    }
  )
)

Table <- setRefClass("Table",
   fields = list(name="character", dataset="Dataset", project="Project"),
   methods = list(
     to_dataframe = function(limit=NULL) {
       container <- if (is.null(dataset)) project else dataset
       owner <- if(is.null(container$user)) container$organization else container$user
       uri <- str_interp("/tables/${owner$name}.${container$name}.${name}")
       variables <- make_paginated_request(path=str_interp("${uri}/variables"))

       table_metadata <- make_request(method="GET", path=uri)

       max_results <- if(!is.null(limit)) min(limit, table_metadata$numRows) else table_metadata$numRows

       rows <- make_rows_request(
         uri=uri,
         max_results=max_results,
         query=list(
           "selectedVariables" = paste(Map(function(variable) variable$name, variables), sep='', collapse=',')
         )
       )

       rows_to_dataframe(rows, variables)
     }
   )
)
