# Internal package state. Tracks whether the native (C) component loaded
# successfully. The native code powers a few optional features (file
# streaming connections and FUSE mounting); it must never prevent the rest
# of the package from loading if it fails to load (e.g. an ABI mismatch such
# as `undefined symbol: R_getVar` when the compiled .so is newer than the
# running R). See require_native().
.redivis_state <- new.env(parent = emptyenv())
.redivis_state$native_available <- FALSE
.redivis_state$native_load_error <- NULL

#' @keywords internal
native_available <- function() isTRUE(.redivis_state$native_available)

#' Abort a native-only feature with a clear message when the C component
#' could not be loaded, rather than surfacing a cryptic .Call() error.
#' @keywords internal
require_native <- function(feature) {
  if (!native_available()) {
    detail <- if (!is.null(.redivis_state$native_load_error)) {
      str_interp(" (${.redivis_state$native_load_error})")
    } else {
      ""
    }
    abort_redivis_error(
      str_interp(paste0(
        "${feature} requires redivis's native component, which failed to ",
        "load in this R session${detail}. All other Redivis functionality ",
        "is available. This usually means the installed binary was built ",
        "against a different version of R; reinstalling from source ",
        "(install.packages('redivis', type = 'source')) typically resolves it."
      )),
      class = "redivis_native_unavailable"
    )
  }
}

.onLoad <- function(libname, pkgname) {
  # Load the compiled code manually (instead of via NAMESPACE's useDynLib) so
  # that a load failure degrades only the native features rather than aborting
  # the whole package namespace.
  tryCatch(
    {
      library.dynam("redivis", pkgname, libname)
      .redivis_state$native_available <- TRUE
    },
    error = function(e) {
      .redivis_state$native_available <- FALSE
      .redivis_state$native_load_error <- conditionMessage(e)
    }
  )

  if (is_jupyter() && requireNamespace("IRdisplay", quietly = TRUE)) {
    handler_jupyter <- function(
      intrusiveness = getOption("progressr.intrusiveness.gui", 1),
      target = "gui",
      ...
    ) {
      reporter <- local({
        last_time <- proc.time()[3]

        list(
          reset = function(...) {},

          initiate = function(config, state, progression, ...) {
            if (!state$enabled || config$times == 1L) {
              return()
            }
            last_time <<- proc.time()[3]
            IRdisplay::display_html(
              '<progress value="0" max="100" style="width:100%">0%</progress> 0%'
            )
          },

          update = function(config, state, progression, ...) {
            if (!state$enabled || config$times == 1L) {
              return()
            }
            now <- proc.time()[3]
            if (now - last_time < 0.2) {
              return()
            }
            last_time <<- now
            pct <- as.integer(state$step / config$max_steps * 100)
            IRdisplay::clear_output(wait = TRUE)
            IRdisplay::display_html(sprintf(
              '<progress value="%d" max="100" style="width:100%%">%d%%</progress> %d%%',
              pct,
              pct,
              pct
            ))
          },

          finish = function(config, state, progression, ...) {
            if (is.null(state$step)) {
              return()
            }
            if (!state$enabled) {
              return()
            }
            IRdisplay::clear_output(wait = FALSE)
          }
        )
      })

      progressr::make_progression_handler(
        "jupyter",
        reporter = reporter,
        intrusiveness = intrusiveness,
        target = target,
        enable = TRUE,
        ...
      )
    }
    options(progressr.enable = TRUE)
    progressr::handlers(handler_jupyter)
  }
}

.onAttach <- function(libname, pkgname) {
  # Give a one-time heads-up when the optional native component failed to load,
  # so users learn about it up front rather than only when a native feature is
  # first used. Only shown on attach (library(redivis)), never on bare loading.
  if (!native_available()) {
    packageStartupMessage(
      "redivis: the optional native component could not be loaded; file ",
      "streaming and directory mounting are unavailable. All other ",
      "functionality works normally. This usually means the installed binary ",
      "was built against a different version of R — reinstalling from ",
      "source (install.packages('redivis', type = 'source')) typically ",
      "resolves it."
    )
  }
}

.onUnload <- function(libpath) {
  if (native_available()) {
    library.dynam.unload("redivis", libpath)
  }
}

is_jupyter <- function() {
  nzchar(Sys.getenv("JPY_PARENT_PID")) || isTRUE(getOption("jupyter.in_kernel"))
}
