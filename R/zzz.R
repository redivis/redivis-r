.onLoad <- function(libname, pkgname) {
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

is_jupyter <- function() {
  nzchar(Sys.getenv("JPY_PARENT_PID")) || isTRUE(getOption("jupyter.in_kernel"))
}
