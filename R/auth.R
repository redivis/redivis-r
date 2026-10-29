# Only truly static, environment-independent state lives in this env. Values
# that depend on the runtime environment (HOME, REDIVIS_API_ENDPOINT) must NOT
# be computed here: top-level package code runs at build/install time, so a
# pre-built binary (e.g. from r-universe) would bake in the *build machine's*
# environment. Those values are computed on demand via the accessors below.
auth_vars <- new.env(parent = emptyenv())

auth_vars$cached_credentials <- NULL
# The file that cached_credentials were loaded from, which changes when
# config_dir does
auth_vars$cached_credentials_file <- NULL
auth_vars$default_scope <- list('data.edit', 'workflow.write')
auth_vars$client_id <- 'Ah850nGnQg5mFWd25nkyk9Y3'

get_redivis_dir <- function() {
  # Overridable for environments where the home directory isn't writable, or is
  # shared between users
  config_dir <- get_config("config_dir")
  if (!is.null(config_dir)) {
    path.expand(config_dir)
  } else {
    file.path(Sys.getenv("HOME"), ".redivis")
  }
}

# Credentials are only valid for the environment that issued them. Those for
# redivis.com are kept directly in the config directory, and any other API
# endpoint's host (e.g. "localhost_8080") has its own subdirectory. The JS and
# Python clients name these directories the same way.
get_environment_host <- function() {
  url <- httr2::url_parse(get_config("api_endpoint"))
  host <- tolower(url$hostname)
  default_port <- c(http = "80", https = "443")[[url$scheme]]
  if (!is.null(url$port) && !identical(as.character(url$port), default_port)) {
    host <- paste0(host, ":", url$port)
  }
  gsub("[^A-Za-z0-9_.-]", "_", host)
}

get_credentials_dir <- function() {
  host <- get_environment_host()
  if (host == "redivis.com") {
    get_redivis_dir()
  } else {
    file.path(get_redivis_dir(), "environments", host)
  }
}

get_credentials_file <- function() {
  file.path(get_credentials_dir(), "r_credentials")
}

get_base_url <- function() {
  sub(
    "(https?://.*?)(/|$).*",
    "\\1",
    get_config("api_endpoint")
  )
}

get_verify_ssl <- function() {
  !grepl(
    "https://localhost",
    get_config("api_endpoint"),
    fixed = TRUE
  )
}

# The credentials saved in a file, or NULL if there are none
read_credentials_file <- function(credentials_file) {
  if (!file.exists(credentials_file)) {
    return(NULL)
  }
  tryCatch(
    jsonlite::fromJSON(
      readLines(
        credentials_file,
        warn = FALSE # Otherwise will print a warning since there's not a line break at the end
      )
    ),
    error = function(e) NULL
  )
}

load_cached_credentials <- function() {
  credentials_file <- get_credentials_file()
  if (
    !is.null(auth_vars$cached_credentials) &&
      !is.null(auth_vars$cached_credentials_file) &&
      auth_vars$cached_credentials_file != credentials_file
  ) {
    auth_vars$cached_credentials <- NULL
  }
  if (is.null(auth_vars$cached_credentials)) {
    credentials <- read_credentials_file(credentials_file)
    if (!is.null(credentials)) {
      auth_vars$cached_credentials <- credentials
      auth_vars$cached_credentials_file <- credentials_file
    }
  }
}

# Whether an auth token can be obtained without an interactive login. When it
# can't, make_request() sends requests anonymously, since the resource may be
# publicly accessible, and only prompts for a login if the server rejects it.
has_credentials <- function() {
  if (!is.na(Sys.getenv("REDIVIS_API_TOKEN", unset = NA))) {
    return(TRUE)
  }
  load_cached_credentials()
  !is.null(auth_vars$cached_credentials)
}

write_cached_credentials <- function() {
  auth_vars$cached_credentials_file <- get_credentials_file()
  # The file holds a refresh token, so keep it private to this user
  credentials_dir <- get_credentials_dir()
  dir.create(credentials_dir, showWarnings = FALSE, recursive = TRUE, mode = "0700")
  # Write a temporary file and swap it into place, so that another process
  # (e.g. a second R session) never reads a partially written file. The umask
  # makes it private from the moment it's created, so the token is never
  # exposed to others, even briefly, and replacing the file also replaces the
  # permissions of one written by an earlier version of the client.
  old_umask <- Sys.umask("077")
  on.exit(Sys.umask(old_umask), add = TRUE)
  temp_file <- tempfile(".r_credentials.", tmpdir = credentials_dir)
  # A no-op once the file has been moved into place
  on.exit(unlink(temp_file), add = TRUE)
  write(
    jsonlite::toJSON(
      auth_vars$cached_credentials,
      pretty = TRUE,
      auto_unbox = TRUE
    ),
    temp_file
  )
  if (!file.rename(temp_file, get_credentials_file())) {
    abort_redivis_error(str_interp(
      "Failed to save credentials to ${get_credentials_file()}"
    ))
  }
}

# With allow_login = FALSE, returns NULL rather than logging in when no token
# can be obtained without one
get_auth_token <- function(scope = NULL, allow_login = TRUE) {
  if (is.null(scope)) {
    scope <- auth_vars$default_scope
  }

  if (!is.na(Sys.getenv("REDIVIS_API_TOKEN", unset = NA))) {
    if (
      is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset = NA)) && interactive()
    ) {
      warning(
        "Setting the REDIVIS_API_TOKEN for interactive sessions is deprecated and highly discouraged.\nPlease delete the token on Redivis and remove it from your code, and follow the authentication prompts here instead.\n\nThis environment variable should only ever be set in a non-interactive environment, such as in an automated script or service.",
        call. = FALSE
      )
    }
    return(Sys.getenv("REDIVIS_API_TOKEN"))
  }
  load_cached_credentials()

  missing_scope <- setdiff(scope, get_current_credential_scope())

  if (
    !is.null(auth_vars$cached_credentials) &&
      "expires_at" %in% names(auth_vars$cached_credentials) &&
      "access_token" %in% names(auth_vars$cached_credentials) &&
      length(missing_scope) == 0
  ) {
    # Refresh a token that expires in the next 5 minutes, rather than sending
    # one the server is about to reject
    if (
      auth_vars$cached_credentials$expires_at <
        (as.numeric(Sys.time()) + 5 * 60)
    ) {
      refresh_cached_credentials()
    }
    if (!is.null(auth_vars$cached_credentials)) {
      return(auth_vars$cached_credentials$access_token)
    }
  }

  if (!allow_login) {
    return(NULL)
  }
  perform_oauth_login(
    scope = if (length(missing_scope)) missing_scope else scope,
    upgrade_credentials = length(missing_scope) > 0
  )
  return(auth_vars$cached_credentials$access_token)
}

clear_cached_credentials <- function() {
  auth_vars$cached_credentials <- NULL
  if (file.exists(get_credentials_file())) {
    file.remove(get_credentials_file())
  }
}

perform_oauth_login <- function(
  scope,
  amr_values = NULL,
  upgrade_credentials = FALSE
) {
  # Always request the default scope alongside whatever else was asked for. A
  # token without it (e.g. from a login prompted by one resource's required
  # scope) would make get_auth_token() prompt again on the very next request.
  scope <- unique(c(unlist(auth_vars$default_scope), unlist(scope)))

  pkce <- get_pkce()
  challenge <- pkce$challenge
  verifier <- pkce$verifier

  # Sent along so that the new token is merged with the scopes and logins of the
  # current one, and so that the user can be asked to sign in with a login it
  # needs (amr_values), which only the user it belongs to can look up
  headers <- list()
  if (upgrade_credentials && !is.null(auth_vars$cached_credentials)) {
    headers <- list(
      'Authorization' = str_interp(
        "Bearer ${auth_vars$cached_credentials$access_token}"
      )
    )
  }

  req <- httr2::request(paste0(
    get_base_url(),
    "/oauth/device_authorization"
  )) |>
    httr2::req_method("POST") |>
    httr2::req_headers(!!!headers, "Content-Type" = "application/json") |>
    httr2::req_body_json(list(
      client_id = auth_vars$client_id,
      scope = paste(scope, collapse = " "),
      amr_values = amr_values,
      code_challenge = challenge,
      code_challenge_method = 'S256',
      access_type = 'offline'
    ))

  if (!get_verify_ssl()) {
    req <- req |> httr2::req_options(ssl_verifypeer = 0L)
  }

  res <- httr2::req_perform(req)
  parsed_response <- httr2::resp_body_json(res)

  browse_url_response <- browseURL(parsed_response$verification_uri_complete)

  # If successful, will be NULL. Otherwise will return 0.
  if (is.null(browse_url_response)) {
    cat('Please authenticate with your Redivis account. Opening browser to:\n')
    cat(parsed_response$verification_uri_complete, '\n')
  } else {
    cat(
      'Please visit the URL below to authenticate with your Redivis account:\n'
    )
    cat(parsed_response$verification_uri_complete, '\n')
  }
  flush.console()


  started_polling_at <- Sys.time()
  while (TRUE) {
    if (difftime(Sys.time(), started_polling_at, units = "secs") > 60 * 10) {
      abort_redivis_error(
        "Timed out after 10 minutes while waiting for device authorization"
      )
    }

    Sys.sleep(ifelse(
      is.null(parsed_response$interval),
      5,
      parsed_response$interval
    ))

    poll_req <- httr2::request(paste0(get_base_url(), "/oauth/token")) |>
      httr2::req_method("POST") |>
      httr2::req_headers(!!!headers) |>
      httr2::req_body_form(
        client_id = auth_vars$client_id,
        grant_type = 'urn:ietf:params:oauth:grant-type:device_code',
        device_code = parsed_response$device_code,
        code_verifier = verifier
      ) |>
      httr2::req_error(is_error = function(resp) FALSE)

    if (!get_verify_ssl()) {
      poll_req <- poll_req |> httr2::req_options(ssl_verifypeer = 0L)
    }

    res <- httr2::req_perform(poll_req)

    if (httr2::resp_status(res) == 200) {
      cat("Authentication was successful!")
      break
    } else if (httr2::resp_status(res) == 400) {
      error_response <- httr2::resp_body_json(res)
      if (error_response$error == 'authorization_pending') {
        # authorization pending
      } else {
        raise_api_error(response_json = error_response, response = res)
      }
    } else {
      raise_api_error(
        response_json = httr2::resp_body_json(res),
        response = res
      )
    }
  }

  auth_vars$cached_credentials <- httr2::resp_body_json(res)
  write_cached_credentials()

  return(auth_vars$cached_credentials)
}

refresh_credentials <- function(scope = NULL, amr_values = NULL) {
  if (!is.null(scope) || !is.null(amr_values)) {
    perform_oauth_login(
      scope = if (is.null(scope)) get_current_credential_scope() else scope,
      amr_values = amr_values,
      upgrade_credentials = TRUE
    )
  } else {
    refresh_cached_credentials()
  }

  return(get_auth_token())
}

# Exchanges the refresh token for a new access token, clearing the credentials
# if it's been rejected
refresh_cached_credentials <- function() {
  if (is.null(auth_vars$cached_credentials$refresh_token)) {
    clear_cached_credentials()
    return(invisible(NULL))
  }

  # Each refresh replaces the refresh token, and the server revokes the login if
  # a replaced one is used again. So if another process sharing these
  # credentials (e.g. a second R session) has refreshed them since they were
  # read, use what it saved instead.
  saved_credentials <- read_credentials_file(get_credentials_file())
  if (
    is.list(saved_credentials) &&
      !is.null(saved_credentials$refresh_token) &&
      !identical(saved_credentials$refresh_token, auth_vars$cached_credentials$refresh_token)
  ) {
    auth_vars$cached_credentials <- saved_credentials
    if (
      !is.null(saved_credentials$expires_at) &&
        saved_credentials$expires_at > as.numeric(Sys.time()) + 5 * 60
    ) {
      return(invisible(NULL))
    }
  }

  refresh_req <- httr2::request(paste0(get_base_url(), "/oauth/token")) |>
    httr2::req_method("POST") |>
    httr2::req_body_form(
      client_id = auth_vars$client_id,
      grant_type = 'refresh_token',
      refresh_token = auth_vars$cached_credentials$refresh_token
    ) |>
    httr2::req_error(is_error = function(resp) FALSE)

  if (!get_verify_ssl()) {
    refresh_req <- refresh_req |> httr2::req_options(ssl_verifypeer = 0L)
  }

  res <- httr2::req_perform(refresh_req)

  status <- httr2::resp_status(res)
  if (status %in% c(400, 401)) {
    # The refresh token was rejected (e.g. revoked or expired), so the user
    # needs to log in again
    clear_cached_credentials()
  } else if (status >= 400) {
    # Anything else (e.g. a 503 during a deploy) says nothing about the
    # refresh token, so keep it for a later call to retry with
    resp_body <- tryCatch(httr2::resp_body_string(res), error = function(e) "")
    response_json <- tryCatch(
      jsonlite::fromJSON(resp_body, simplifyVector = FALSE),
      error = function(e) NULL
    )
    raise_api_error(
      response_json = if (is.list(response_json)) response_json,
      response_text = resp_body,
      response = res
    )
  } else {
    refresh_response <- httr2::resp_body_json(res)
    auth_vars$cached_credentials$access_token <- refresh_response$access_token
    auth_vars$cached_credentials$expires_at <- refresh_response$expires_at
    auth_vars$cached_credentials$expires_in <- refresh_response$expires_in
    # The server rotates refresh tokens, returning a new one to replace the one
    # that was just used
    if (!is.null(refresh_response$refresh_token)) {
      auth_vars$cached_credentials$refresh_token <- refresh_response$refresh_token
    }
    write_cached_credentials()
  }
}

get_current_credential_scope <- function() {
  tryCatch(
    {
      if (!is.null(auth_vars$cached_credentials)) {
        token_payload <- strsplit(
          auth_vars$cached_credentials$access_token,
          ".",
          fixed = TRUE
        )[[1]][2]
        # JWT segments are unpadded base64url
        decoded_token <- jsonlite::fromJSON(rawToChar(jsonlite::base64url_dec(
          token_payload
        )))
        return(strsplit(decoded_token$scope, " ")[[1]])
      }
    },
    error = function(e) {
      # Ignore errors
    }
  )

  return(auth_vars$default_scope)
}

get_pkce <- function() {
  # Must come from a cryptographic source: sample() draws from R's RNG, which
  # is reproducible after any set.seed() call
  verifier <- safe_encode_base64_url(openssl::rand_bytes(48))
  challenge <- safe_encode_base64_url(openssl::sha256(charToRaw(verifier)))
  list(challenge = challenge, verifier = verifier)
}

# Helper function to handle base64 url encoding without padding
safe_encode_base64_url <- function(x) {
  sub("\\=", "", jsonlite::base64url_enc(x))
}
