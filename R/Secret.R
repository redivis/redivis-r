#' @include api_request.R
Secret <- R6::R6Class(
  "Secret",
  public = list(
    name = NULL,
    user = NULL,
    organization = NULL,

    initialize = function(name = "", user = NULL, organization = NULL) {
      self$name <- name
      self$user <- user
      self$organization <- organization
    },

    print = function(...) {
      cat(str_interp("<Secret ${self$name}>\n"))
      invisible(self)
    },

    get_value = function() {
      base_path <- if (!is.null(self$user)) {
        str_interp("/users/${self$user$name}")
      } else {
        str_interp("/organizations/${self$organization$name}")
      }

      secret <- make_request(
        path = str_interp("${base_path}/secrets/${self$name}")
      )
      secret$value
    }
  )
)
