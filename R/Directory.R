#' @include File.R
Directory <- setRefClass(
  "Directory",
  fields = list(
    path = "ANY",
    name = "character",
    table = "ANY",
    query = "ANY",
    parent = "ANY",
    children = "list",
    .mount_ptr = "ANY",
    .mount_path = "ANY"
  ),

  methods = list(
    initialize = function(
      path,
      table = NULL,
      query = NULL,
      parent = NULL
    ) {
      if (is.null(table) && is.null(query)) {
        abort_redivis_value_error(
          "All directories must either belong to a table or query."
        )
      }
      path <<- path
      table <<- table
      query <<- query
      parent <<- parent
      children <<- list()
      name <<- basename(path)
      .mount_ptr <<- NULL
      .mount_path <<- NULL
    },

    show = function() {
      print(str_interp("<Dir ${.self$path}>"))
    },

    list = function(
      max_results = NULL,
      mode = c("all", "files", "directories"),
      recursive = FALSE
    ) {
      mode <- match.arg(mode)
      if (is.null(max_results)) {
        max_results <- Inf
      }
      result <- base::list()

      for (child in .self$children) {
        if (length(result) >= max_results) {
          break
        }

        if (recursive && inherits(child, "Directory")) {
          if (mode == "all" || mode == "directories") {
            result[[length(result) + 1]] <- child
          }
          sub_children <- child$list(
            mode = mode,
            recursive = TRUE,
            max_results = max_results - length(result)
          )
          result <- c(result, sub_children)
        } else {
          if (
            mode == "all" ||
              (mode == "files" && inherits(child, "File")) ||
              (mode == "directories" && inherits(child, "Directory"))
          ) {
            result[[length(result) + 1]] <- child
          }
        }
      }

      result
    },

    mount = function(mount_path, cache_dir = NULL) {
      if (.Platform$OS.type == "windows") {
        abort_redivis_error("FUSE mounting is not supported on Windows.")
      }

      mount_path <- normalizePath(mount_path, mustWork = FALSE)

      if (file.exists(mount_path)) {
        abort_redivis_value_error(str_interp(
          "Mount path '${mount_path}' already exists. Please provide a path that does not exist."
        ))
      }

      if (is.null(cache_dir)) {
        cache_dir <- file.path(
          tempdir(),
          "redivis_fuse_cache",
          digest::digest(mount_path, algo = "md5")
        )
      }
      cache_dir <- normalizePath(cache_dir, mustWork = FALSE)
      dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
      # Create parent directories, then the mount point itself.
      # FUSE requires the mount point directory to exist.
      parent_dir <- dirname(mount_path)
      dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
      dir.create(mount_path, showWarnings = FALSE)

      # Collect all files recursively
      files <- .self$list(mode = "files", recursive = TRUE)
      if (length(files) == 0) {
        abort_redivis_value_error("Directory contains no files to mount.")
      }

      # Build the file manifest
      self_path <- gsub("^/+|/+$", "", as.character(.self$path))

      rel_paths <- vapply(
        files,
        function(f) {
          fp <- gsub("^/+|/+$", "", as.character(f$path))
          if (nchar(self_path) > 0) {
            sub(paste0("^", self_path, "/?"), "", fp)
          } else {
            fp
          }
        },
        character(1)
      )

      sizes <- vapply(
        files,
        function(f) {
          if (is.null(f$size)) 0 else as.double(f$size)
        },
        double(1)
      )

      file_ids <- vapply(
        files,
        function(f) {
          as.character(f$id)
        },
        character(1)
      )

      # Get auth info
      api_base_url <- generate_api_url("")
      # Strip the trailing path portion to get just the base
      # generate_api_url("") gives e.g. "https://redivis.com/api/v1"
      auth_token <- get_auth_token()

      .mount_ptr <<- .Call(
        "C_fuse_mount",
        as.character(mount_path),
        as.character(cache_dir),
        as.character(rel_paths),
        as.double(sizes),
        as.character(file_ids),
        as.character(api_base_url),
        as.character(auth_token)
      )

      .mount_path <<- mount_path
      message(str_interp("Mounted at ${mount_path}"))
      invisible(mount_path)
    },

    unmount = function() {
      if (is.null(.mount_ptr)) {
        warning("Not currently mounted.", call. = FALSE)
        return(invisible(NULL))
      }
      mount_path <- .self$.mount_path
      .Call("C_fuse_unmount", .mount_ptr)
      .mount_ptr <<- NULL
      .mount_path <<- NULL

      message("Unmounted.")
      invisible(NULL)
    },

    get = function(path) {
      path_str <- as.character(path)

      # Legacy support for passing file ids
      split <- strsplit(path_str, "\\.")[[1]]
      client_id <- split[[1]]
      id_parts <- strsplit(client_id, "-")[[1]]
      if (
        length(split) == 2 &&
          nchar(client_id) == 14 &&
          length(id_parts) == 2 &&
          nchar(id_parts[[1]]) == 4
      ) {
        warning(
          "Passing file ids is deprecated, please use file names instead. E.g.: table.file('filename.png')",
          call. = FALSE
        )
        files <- .self$list(mode = "files", recursive = TRUE)
        for (f in files) {
          if (f$id == path_str) return(f)
        }
        return(NULL)
      }

      is_explicit_dir <- endsWith(path_str, "/")

      # For absolute paths, walk up to the root first
      if (startsWith(path_str, "/")) {
        node <- .self
        while (!is.null(node$parent)) {
          node <- node$parent
        }
        # Delegate to root with the same path — it will be treated as relative
        # since we've already resolved the root
        stripped <- sub("^/+", "", path_str)
        if (nchar(stripped) == 0) {
          return(node)
        }
        return(node$get(stripped))
      }

      # Normalize to character parts
      parts <- strsplit(
        gsub("^/|/$", "", path_str),
        "/"
      )[[1]]
      parts <- parts[nchar(parts) > 0]

      if (length(parts) == 0) {
        return(.self)
      }

      node <- .self
      for (idx in seq_along(parts)) {
        part <- parts[[idx]]

        if (part == ".") {
          next
        }

        if (part == "..") {
          if (is.null(node$parent)) {
            abort_redivis_value_error("Path traverses above the root directory")
          }
          node <- node$parent
          if (idx == length(parts)) {
            return(node)
          }
          next
        }

        child <- node$children[[part]]
        if (is.null(child)) {
          return(NULL)
        }

        if (inherits(child, "Directory")) {
          node <- child
        } else {
          # Must be the last part to match a file
          if (idx == length(parts)) {
            if (is_explicit_dir) {
              return(NULL)
            }
            return(child)
          }
          return(NULL)
        }
      }

      node
    },

    download = function(
      path = getwd(),
      overwrite = FALSE,
      max_results = NULL,
      file_id_variable = NULL,
      progress = TRUE,
      max_parallelization = NULL
    ) {
      files <- .self$list(
        mode = "files",
        recursive = TRUE,
        max_results = max_results
      )

      if (file.exists(path)) {
        if (!dir.exists(path)) {
          # is a file, not a directory
          if (overwrite) {
            file.remove(path)
          } else {
            abort_redivis_value_error(paste0(
              "Destination path '",
              path,
              "' exists and is a file; set overwrite=TRUE to replace it."
            ))
          }
        }
      }

      dir.create(path, recursive = TRUE, showWarnings = FALSE)

      total_bytes <- sum(vapply(
        files,
        function(f) {
          if (is.null(f$size)) 0L else as.integer(f$size)
        },
        integer(1L)
      ))

      self_path <- gsub("^/+|/+$", "", as.character(.self$path))
      args = base::list(
        uris = purrr::map(files, function(f) {
          str_interp("/rawFiles/${f$id}")
        }),
        sizes = purrr::map(files, function(f) {
          f$size
        }),
        md5_hashes = purrr::map(files, function(f) {
          f$hash
        }),
        total_bytes = total_bytes,
        download_paths = purrr::map_chr(files, function(f) {
          relative_path <- gsub(
            paste0("^", self_path, "/?"),
            "",
            gsub("^/+|/+$", "", as.character(f$path))
          )
          # Normalize path to avoid double slashes from trailing slash in `path`
          normalizePath(
            file.path(gsub("/+$", "", path), relative_path),
            mustWork = FALSE
          )
        }),
        overwrite = overwrite,
        max_parallelization = max_parallelization
      )

      if (progress) {
        progressr::with_progress(do.call(perform_parallel_download, args))
      } else {
        do.call(perform_parallel_download, args)
      }
    }
  )
)

add_directory_file = function(dir, file) {
  # Normalize: strip all leading/trailing slashes, then re-add single leading /
  normalize_path <- function(p) {
    p <- gsub("^/+|/+$", "", as.character(p))
    p
  }

  self_path <- normalize_path(dir$path)
  file_path <- normalize_path(file$path)

  # Compute relative path by stripping self_path prefix
  if (nchar(self_path) > 0) {
    relative <- sub(paste0("^", self_path, "/?"), "", file_path, fixed = FALSE)
  } else {
    relative <- file_path
  }

  parts <- strsplit(relative, "/")[[1]]
  parts <- parts[nchar(parts) > 0]

  if (length(parts) == 0) {
    # file_path resolves to this directory itself — skip
    return(invisible(NULL))
  } else if (length(parts) == 1) {
    dir$children[[parts[[1]]]] <- file
    file$directory <- dir
  } else {
    subdir_name <- parts[[1]]
    subdir <- dir$children[[subdir_name]]
    if (is.null(subdir)) {
      # Build subdir path cleanly without file.path()
      subdir_path <- if (nchar(self_path) > 0) {
        paste0("/", self_path, "/", subdir_name)
      } else {
        paste0("/", subdir_name)
      }
      subdir <- Directory$new(
        parent = dir,
        path = subdir_path,
        table = dir$table,
        query = dir$query
      )
      dir$children[[subdir_name]] <- subdir
    }
    add_directory_file(subdir, file)
  }
}
