.normalize_tag <- function(tag) {
  if (!identical(tag, "current") &&
      !identical(tag, "next") &&
      !grepl("^v", tag, ignore.case = TRUE)) {
    sprintf("v%s", tag)
  } else {
    tag
  }
}

#' @include Dataset.R api_request.R
Version <- setRefClass("Version",
  fields = list(
    tag = "character",
    ds = "ANY",
    uri = "character",
    properties = "list"
  ),

  methods = list(
    initialize = function(tag, dataset, properties = list(), ...) {
      tag <<- .normalize_tag(tag)
      ds <<- dataset
      properties <<- properties

      if (!is.null(properties$uri)) {
        uri <<- properties$uri
      } else {
        uri <<- sprintf("%s/versions/%s", dataset$uri, tag)
      }

      callSuper(...)
    },

    show = function(){
      if (grepl(tag, str_interp(":${.self$ds$qualified_reference}"))){
        print(str_interp("<Version ${.self$ds$qualified_reference}>"))
      } else {
        print(str_interp("<Version ${.self$ds$qualified_reference}:${.self$tag}>"))
      }

    },

    get = function() {
      properties <<- make_request(method = "GET", path = uri)
      uri <<- properties$uri
      .self
    },

    exists = function() {
      res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
      if (length(res$error)){
        if (res$status == 404){
          return(FALSE)
        } else {
          stop(str_interp("${res$error}: ${res$error_description}"))
        }
      } else {
        return(TRUE)
      }
    },

    update = function(label = NULL, release_notes = NULL) {
      payload <- list()

      if (!is.null(label)) payload$label <- label
      if (!is.null(release_notes)) payload$releaseNotes <- release_notes

      properties <<- make_request(method = "PATCH", path = uri, payload = payload)
      uri <<- properties$uri
      .self
    },

    delete = function() {
      properties <<- make_request(method = "DELETE", path = uri)
      uri <<- properties$uri
      .self
    },

    undelete = function() {
      properties <<- make_request(method = "POST", path = sprintf("%s/undelete", uri))
      uri <<- properties$uri
      .self
    },

    previous_version = function() {
      if (! ("previousVersion" %in% .self$properties)) {
        .self$get()
      }
      pv <- .self$properties$previousVersion
      if (is.null(pv)) return(NULL)
      Version$new(tag = pv$tag, dataset = ds)
    },

    next_version = function() {
      if (! ("nextVersion" %in% .self$properties)) {
        .self$get()
      }
      nv <- .self$properties$nextVersion
      if (is.null(nv)) return(NULL)
      Version$new(tag = nv$tag, dataset = ds)
    },

    dataset = function() {
      Dataset$new(
        ds$scopedReference,
        organization = ds$organization,
        user = ds$user,
        version = tag
      )
    }
  )
)
