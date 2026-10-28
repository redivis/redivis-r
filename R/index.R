#' @include config.R
#' @importFrom later later
#' @importFrom rlang :=
#' @importFrom stats setNames
#' @importFrom utils browseURL flush.console object.size tail URLdecode
NULL

#' @title query
#'
#' @description DEPRECATED: please use redivis$query
#'
#' @param query The query string to execute
#' @param default_workflow Optional workflow reference used to resolve unqualified table references. Defaults to the configured default workflow (see redivis$config()).
#' @param default_dataset Optional dataset reference used to resolve unqualified table references. Defaults to the configured default dataset (see redivis$config()).
#'
#' @return class<Query>
#' @examples
#' \dontrun{
#' output_table <- redivis::query(query = 'SELECT 1 + 1 AS two')$to_tibble()
#' }
#' @export
query <- function(query = "", default_workflow = NULL, default_dataset = NULL) {
  show_namespace_warning("query")
  if (is.null(default_workflow) && is.null(default_dataset)) {
    default_workflow <- get_config("default_workflow")
    default_dataset <- get_config("default_dataset")
  }
  Query$new(
    query = query,
    default_workflow = default_workflow,
    default_dataset = default_dataset
  )
}


#' @title user
#'
#' @description DEPRECATED: please use redivis$user
#'
#' @param name The user's username
#'
#' @return class<user>
#' @examples
#' \dontrun{
#' redivis::user('my_username')$workflow('my_workflow')$table('my_table')$to_tibble(max_results=100)
#'
#' # We can also construct a query scoped to a particular workflow, removing the need to fully qualify table names
#' redivis::user('my_username')$workflow('my_workflow')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")$to_tibble()
#' }
#' @export
user <- function(name) {
  show_namespace_warning("user")
  User$new(name = name)
}


#' @title organization
#'
#' @description DEPRECATED: please use redivis$organization
#'
#' @param name The organization's username
#'
#' @return class<user>
#' @examples
#' \dontrun{
#' redivis::organization('demo_organization')$dataset('some_dataset')$table('a_table')$to_tibble(max_results=100)
#'
#' # We can also construct a query scoped to a particular dataset, removing the need to fully qualify table names
#' redivis::organization('demo_organization')$dataset('some_dataset')$query("SELECT * FROM table_1 INNER JOIN table_2 ON id")$to_tibble()
#' }
#' @export
organization <- function(name) {
  show_namespace_warning("organization")
  Organization$new(name = name)
}


#' @title current_notebook
#'
#' @description DEPRECATED: please use redivis$current_notebook
#'
#' @return class<Notebook>
#' @examples
#' \dontrun{
#' redivis::current_notebook()$create_output_table(df)
#' }
#' @export
current_notebook <- function() {
  show_namespace_warning("current_notebook")
  if (Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK") != "") {
    Notebook$new(name = Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK"))
  } else {
    NULL
  }
}


#' @title redivis
#'
#' @description Redivis wrapper, all primary constructor accessible via redivis$<constructor>()
#'
#' Full documentation and examples available at: https://apidocs.redivis.com/client-libraries/redivis-r
#'
#' @usage redivis
#'
#' @section Overview:
#' \preformatted{
#' library(redivis)
#'
#' # Reference various resources
#' redivis$current_user()
#' redivis$dataset(name)
#' redivis$datasource(source)
#' redivis$notebook(name)
#' redivis$organization(name)
#' redivis$parameter(name)
#' redivis$table(name)
#' redivis$transform(name)
#' redivis$user(name)
#' redivis$workflow(name)
#'
#' # Create a SQL query
#' redivis$query(query)
#'
#' # Only available within Redivis notebooks
#' redivis$current_notebook()
#' redivis$current_workflow()
#' user$secret("MY_SECRET")$get_value()
#' organization$secret("OUR_SECRET")$get_value()
#'
#' # Read data
#' query$to_tibble()
#' table$to_tibble()
#' # also:
#' #   to_data_table()
#' #   to_arrow_table()
#' #   to_arrow_batch_iterator()
#' #   to_sf_tibble()
#' #   ...
#' table$download(path=NULL, format="csv", ...)
#'
#' # DBI interface
#' con <- redivis$connect_dbi()
#'
#' # Read files (file tables only):
#' table$file("path/to/file")
#' open(file)
#' file$read(...)
#' file$download(path=NULL, ...)
#' dir <- table$to_directory()
#' dir$get("/path/to/file/or/dir")
#' dir$download(path=NULL, ...)
#'
#' # Upload data
#' table$upload()$create("/path/to/file.csv", ...)
#' # Upload raw files
#' table$add_files(directory="/path/to/dir", ...)
#'
#' # List resources
#' organization$list_datasets(max_results)
#' user$list_datasets(max_results)
#' user$list_workflows(max_results)
#' dataset$list_tables(max_results)
#' workflow$list_tables(max_results, include_datasources=FALSE)
#' workflow$list_transforms(max_results)
#' table$list_variables(max_results)
#' #...etc, see documentation
#'
#' # Configure library-wide defaults. Unset values fall back to their
#' # environment variables (REDIVIS_DEFAULT_WORKFLOW, REDIVIS_API_ENDPOINT, etc.),
#' # and NULL clears a value. Returns the current configuration.
#' redivis$config(
#'   default_workflow,     # Workflow that unqualified table, query, parameter,
#'                         #   transform, and notebook references resolve within
#'   default_dataset,      # Dataset that unqualified table and query references
#'                         #   resolve within, when there's no default workflow
#'   default_user,         # Owner of unqualified dataset and workflow references
#'   default_organization, # ...when there's no default user
#'   api_endpoint,         # Defaults to "https://redivis.com/api/v1"
#'   config_dir,           # Where credentials are cached. Defaults to "~/.redivis"
#'   tmp_dir,              # For temporary files. Defaults to tempdir()
#'   max_parallelization,  # Default for methods' max_parallelization argument
#'   progress              # Default for methods' progress argument (TRUE)
#' )
#'
#' # Miscellaneous methods, for advanced use cases
#' redivis$authenticate(scope=NULL, force_reauthentication=FALSE)
#' redivis$make_api_request(method="GET", path, ...)
#' }
#'
#' @return list(authenticate, config, current_notebook, current_user, current_workflow, dataset, datasource, file, make_api_request, notebook, organization, parameter, query, table, transform, user, workflow)
#' @examples
#' \dontrun{
#'  df <- redivis$table("demo.ghcn_daily_weather_data.stations")$to_tibble()
#'  df <- redivis$query("SELECT 1+1 as two")$to_tibble()
#'
#'  redivis$dataset("demo.ghcn_daily_weather_data")$exists() # TRUE
#'  datasets <- redivis$organization("demo")$list_datasets(max_results=10)
#'  workflows <- redivis$current_user()$list_workflows(10)
#'
#'  # Only callable from within a Redivis notebook
#'  notebook <- redivis$current_notebook();
#'  workflow <- redivis$current_workflow();
#' }
#' @export
redivis <- local({
  e <- new.env(parent = emptyenv())

  e$authenticate <- function(scope = NULL, force_reauthentication = FALSE) {
    if (force_reauthentication) {
      clear_cached_credentials()
    }
    if (is.character(scope)) {
      scope <- list(scope)
    }
    get_auth_token(scope = scope)
    invisible(NULL)
  }

  e$config <- set_config

  e$current_notebook <- function() {
    if (Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK") != "") {
      Notebook$new(name = Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK"))
    } else {
      NULL
    }
  }

  e$current_user <- function() {
    res <- make_request(method = "GET", path = "/users/me")
    User$new(name = res$name)
  }

  e$current_workflow <- function() {
    if (!is.null(get_config("default_workflow"))) {
      Workflow$new(name = get_config("default_workflow"))
    } else {
      NULL
    }
  }

  e$dataset <- function(name, version = NULL) {
    Dataset$new(name = name, version = version)
  }

  e$datasource <- function(source) {
    Datasource$new(source = source)
  }

  e$file <- function(id) {
    abort_redivis_deprecation_error(
      'Calling redivis$file() is no longer supported. Please use redivis$table("table_reference")$file("filename") instead.'
    )
  }

  e$make_api_request <- function(
    method = 'GET',
    path = "",
    query = NULL,
    payload = NULL,
    headers = NULL,
    parse_response = TRUE,
    stream_callback = NULL
  ) {
    make_request(
      method = method,
      path = path,
      query = query,
      payload = payload,
      headers = headers,
      parse_response = parse_response,
      stream_callback = stream_callback
    )
  }

  e$notebook <- function(name) {
    Notebook$new(name = name)
  }

  e$organization <- function(name) {
    Organization$new(name = name)
  }

  e$parameter <- function(name) {
    Parameter$new(name = name)
  }

  e$query <- function(
    query = "",
    default_workflow = NULL,
    default_dataset = NULL
  ) {
    if (is.null(default_workflow) && is.null(default_dataset)) {
      default_workflow <- get_config("default_workflow")
      default_dataset <- get_config("default_dataset")
    }
    Query$new(
      query = query,
      default_workflow = default_workflow,
      default_dataset = default_dataset
    )
  }

  e$table <- function(name) {
    Table$new(name = name)
  }

  e$transform <- function(name) {
    Transform$new(name = name)
  }

  e$user <- function(name) {
    User$new(name = name)
  }

  e$workflow <- function(name) {
    Workflow$new(name = name)
  }

  e$connect_dbi = function() {
    DBI::dbConnect(RedivisDBI())
  }

  lockEnvironment(e, bindings = TRUE)
  e
})
