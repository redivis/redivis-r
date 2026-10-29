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
      # e.g. v1.0, even if given in the form used in references (v1_0)
      self$tag <- normalize_version_tag(.normalize_tag(tag))
      self$ds <- dataset
      self$properties <- properties

      if (!is.null(properties$uri)) {
        self$uri <- properties$uri
      } else {
        self$uri <- sprintf(
          "%s/versions/%s",
          dataset$uri,
          normalize_version_tag(self$tag, for_reference = TRUE)
        )
      }
    },

    print = function(...) {
      reference_tag <- normalize_version_tag(self$tag, for_reference = TRUE)
      if (
        grepl(
          str_interp(":${reference_tag}"),
          self$ds$qualified_reference,
          fixed = TRUE
        )
      ) {
        cat(str_interp("<Version ${self$ds$qualified_reference}>\n"))
      } else {
        cat(str_interp(
          "<Version ${self$ds$qualified_reference}:${reference_tag}>\n"
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

    delete = function(wait_for_finalization = FALSE) {
      self$properties <- make_request(method = "DELETE", path = self$uri)
      self$uri <- self$properties$uri
      if (wait_for_finalization) {
        wait_for_version_finalization(self)
      }
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
