#' @include File.R
Directory <- R6::R6Class(
  "Directory",
  public = list(
    path = NULL,
    name = NULL,
    table = NULL,
    query = NULL,
    parent = NULL,
    children = NULL,
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
      self$path <- path
      self$table <- table
      self$query <- query
      self$parent <- parent
      self$children <- new.env(parent = emptyenv())
      self$name <- basename(path)
    },

    print = function(...) {
      if (!is.null(self$table)) {
        cat(str_interp(
          "<Dir ${self$table$qualified_reference}${self$path}>\n"
        ))
      } else {
        cat(str_interp("<Dir ${self$path}>\n"))
      }
      invisible(self)
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

      for (child_name in ls(self$children)) {
        child <- self$children[[child_name]]
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
      parent_dir <- dirname(mount_path)
      dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
      dir.create(mount_path, showWarnings = FALSE)

      # Collect all files recursively
      files <- self$list(mode = "files", recursive = TRUE)
      if (length(files) == 0) {
        abort_redivis_value_error("Directory contains no files to mount.")
      }

      # Build the file manifest
      self_path <- gsub("^/+|/+$", "", as.character(self$path))

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

      added_ats <- vapply(
        files,
        function(f) {
          if (
            is.null(f$added_at) || length(f$added_at) == 0 || is.na(f$added_at)
          ) {
            0
          } else {
            as.double(f$added_at)
          }
        },
        double(1)
      )

      # Build directory tree from the R Directory tree (already computed).
      # Walk the tree and collect each dir's immediate children.
      # Use an environment to accumulate results, avoiding <<- which
      # triggers spurious warnings from R5 class field checking.
      tree_env <- new.env(parent = emptyenv())
      tree_env$dir_paths <- character(0)
      tree_env$dir_child_names <- base::list()
      tree_env$dir_child_is_dir <- base::list()

      walk_dir <- function(node, rel_prefix, env) {
        child_names_vec <- character(0)
        child_is_dir_vec <- logical(0)

        for (child_name in ls(node$children)) {
          child <- node$children[[child_name]]
          child_names_vec <- c(child_names_vec, child_name)
          child_is_dir_vec <- c(child_is_dir_vec, inherits(child, "Directory"))
        }

        idx <- length(env$dir_paths) + 1L
        env$dir_paths[[idx]] <- rel_prefix
        env$dir_child_names[[idx]] <- child_names_vec
        env$dir_child_is_dir[[idx]] <- child_is_dir_vec

        # Recurse into subdirectories
        for (child_name in ls(node$children)) {
          child <- node$children[[child_name]]
          if (inherits(child, "Directory")) {
            child_rel <- if (nchar(rel_prefix) == 0) {
              child_name
            } else {
              paste0(rel_prefix, "/", child_name)
            }
            walk_dir(child, child_rel, env)
          }
        }
      }

      walk_dir(self, "", tree_env)

      dir_paths <- tree_env$dir_paths
      dir_child_names <- tree_env$dir_child_names
      dir_child_is_dir <- tree_env$dir_child_is_dir

      # Get auth info
      api_base_url <- generate_api_url("")
      auth_token <- get_auth_token()

      private$.mount_ptr <- .Call(
        "C_fuse_mount",
        as.character(mount_path),
        as.character(cache_dir),
        as.character(rel_paths),
        as.double(sizes),
        as.character(file_ids),
        as.double(added_ats),
        as.character(dir_paths),
        dir_child_names,
        dir_child_is_dir,
        as.character(api_base_url),
        as.character(auth_token)
      )

      private$.mount_path <- mount_path
      message(str_interp("Mounted at ${mount_path}"))
      invisible(mount_path)
    },

    unmount = function() {
      if (is.null(private$.mount_ptr)) {
        warning("Not currently mounted.", call. = FALSE)
        return(invisible(NULL))
      }
      .Call("C_fuse_unmount", private$.mount_ptr)
      private$.mount_ptr <- NULL
      private$.mount_path <- NULL
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
        files <- self$list(mode = "files", recursive = TRUE)
        for (f in files) {
          if (f$id == path_str) return(f)
        }
        return(NULL)
      }

      is_explicit_dir <- endsWith(path_str, "/")

      # For absolute paths, walk up to the root first
      if (startsWith(path_str, "/")) {
        node <- self
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
        return(self)
      }

      node <- self
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

        child <- get0(part, envir = node$children, inherits = FALSE)
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
      files <- self$list(
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

      self_path <- gsub("^/+|/+$", "", as.character(self$path))
      args <- base::list(
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
  ),

  private = list(
    .mount_ptr = NULL,
    .mount_path = NULL
  )
)

add_directory_file <- function(dir, file) {
  # Normalize: strip all leading/trailing slashes, then re-add single leading /
  self_path <- gsub("^/+|/+$", "", dir$path)
  file_path <- gsub("^/+|/+$", "", file$path)

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
