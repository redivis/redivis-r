.normalize_tag <- function(tag) {
  if (
    !identical(tag, "current") &&
      !identical(tag, "next") &&
      !grepl("^v", tag, ignore.case = TRUE)
  ) {
    sprintf("v%s", tag)
  } else {
    tag
  }
}

#' @include Dataset.R api_request.R
Version <- R6::R6Class(
  "Version",
  public = list(
    tag = NULL,
    ds = NULL,
    uri = NULL,
    properties = NULL,

    initialize = function(tag, dataset, properties = list()) {
      self$tag <- .normalize_tag(tag)
      self$ds <- dataset
      self$properties <- properties

      if (!is.null(properties$uri)) {
        self$uri <- properties$uri
      } else {
        self$uri <- sprintf("%s/versions/%s", dataset$uri, self$tag)
      }
    },

    print = function(...) {
      if (grepl(self$tag, str_interp(":${self$ds$qualified_reference}"))) {
        cat(str_interp("<Version ${self$ds$qualified_reference}>\n"))
      } else {
        cat(str_interp(
          "<Version ${self$ds$qualified_reference}:${self$tag}>\n"
        ))
      }
      invisible(self)
    },

    get = function() {
      self$properties <- make_request(method = "GET", path = self$uri)
      self$uri <- self$properties$uri
      self
    },

    exists = function() {
      tryCatch(
        {
          make_request(
            method = "HEAD",
            path = self$uri
          )
          TRUE
        },
        redivis_not_found_error = function(e) {
          FALSE
        }
      )
    },

    update = function(label = NULL, release_notes = NULL) {
      payload <- list()

      if (!is.null(label)) {
        payload$label <- label
      }
      if (!is.null(release_notes)) {
        payload$releaseNotes <- release_notes
      }

      self$properties <- make_request(
        method = "PATCH",
        path = self$uri,
        payload = payload
      )
      self$uri <- self$properties$uri
      self
    },

    delete = function() {
      self$properties <- make_request(method = "DELETE", path = self$uri)
      self$uri <- self$properties$uri
      self
    },

    undelete = function() {
      self$properties <- make_request(
        method = "POST",
        path = sprintf("%s/undelete", self$uri)
      )
      self$uri <- self$properties$uri
      self
    },

    previous_version = function() {
      if (!("previousVersion" %in% names(self$properties))) {
        self$get()
      }
      pv <- self$properties$previousVersion
      if (is.null(pv)) {
        return(NULL)
      }
      Version$new(tag = pv$tag, dataset = self$ds)
    },

    next_version = function() {
      if (!("nextVersion" %in% names(self$properties))) {
        self$get()
      }
      nv <- self$properties$nextVersion
      if (is.null(nv)) {
        return(NULL)
      }
      Version$new(tag = nv$tag, dataset = self$ds)
    },

    dataset = function() {
      Dataset$new(
        self$ds$scoped_reference,
        organization = self$ds$organization,
        user = self$ds$user,
        version = self$tag
      )
    }
  )
)
