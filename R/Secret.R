#' @include api_request.R
Secret <- setRefClass(
  "Secret",
  fields = list(name="character", user="ANY", organization="ANY"),
  methods = list(
    show = function(){
      print(str_interp("<Secret ${.self$name}>"))
    },
    get_value = function() {
      base_path <- if (!is.null(.self$user)) str_interp("/users/${.self$user$name}") else str_interp("/organizations/${.self$organization$name}")

      secret <- make_request(path=str_interp("${base_path}/secrets/${.self$name}"))
      secret$value
    }
  )
)
