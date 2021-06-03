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
query <- function(query="", default_project=NULL, default_dataset=NULL) {
  Query$new(query=query, default_project=default_project, default_dataset=default_dataset)
}


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
