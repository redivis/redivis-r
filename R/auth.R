library(httr)

auth_vars <- new.env(parent = emptyenv())

auth_vars$redivis_dir = file.path(Sys.getenv("HOME"), ".redivis")
auth_vars$cached_credentials = NULL
auth_vars$verify_ssl = !grepl("https://localhost", Sys.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com"), fixed = TRUE)
auth_vars$credentials_file = file.path(file.path(Sys.getenv("HOME"), ".redivis"), "r_credentials")
auth_vars$default_scope = list('data.edit')
auth_vars$client_id = 'Ah850nGnQg5mFWd25nkyk9Y3'
auth_vars$base_url = sub("(https?://.*?)(/|$).*", "\\1", Sys.getenv('REDIVIS_API_ENDPOINT', 'https://redivis.com'))

get_auth_token <- function(scope=NULL) {
  if (is.null(scope)){
    scope <- auth_vars$default_scope
  }

  if (!is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA))) {
    if (is.na(Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset=NA)) && interactive()) {
      warning("Setting the REDIVIS_API_TOKEN for interactive sessions is deprecated and highly discouraged.
Please delete the token on Redivis and remove it from your code, and follow the authentication prompts here instead.

This environment variable should only ever be set in a non-interactive environment, such as in an automated script or service.")
    }
    return(Sys.getenv("REDIVIS_API_TOKEN"))
  } else if (is.null(auth_vars$cached_credentials) && file.exists(auth_vars$credentials_file)) {
    tryCatch({
      auth_vars$cached_credentials <- jsonlite::fromJSON(
        readLines(
          auth_vars$credentials_file,
          warn=FALSE # Otherwise will print a warning since there's not a line break at the end
        )
      )
    }, error = function(e) {
      # ignore
    })
  }

  missing_scope = setdiff(scope, get_current_credential_scope())

  if (
    !is.null(auth_vars$cached_credentials)
    && "expires_at" %in% names(auth_vars$cached_credentials)
    && "access_token" %in% names(auth_vars$cached_credentials)
    && length(missing_scope) == 0
  ) {
    if (auth_vars$cached_credentials$expires_at < (as.numeric(Sys.time()) - 5 * 60)) {
      refresh_credentials()
    }

    return(auth_vars$cached_credentials$access_token)
  } else {
    if (!dir.exists(auth_vars$redivis_dir)) {
      dir.create(auth_vars$redivis_dir)
    }

    perform_oauth_login(scope=if(length(missing_scope)) missing_scope else scope, upgrade_credentials=length(missing_scope) > 0)
    return(auth_vars$cached_credentials$access_token)
  }
}

clear_cached_credentials <- function() {
  auth_vars$cached_credentials <- NULL
  if (file.exists(auth_vars$credentials_file)) {
    file.remove(auth_vars$credentials_file)
  }
}

perform_oauth_login <- function(scope, amr_values=NULL, upgrade_credentials=FALSE) {
  pkce <- get_pkce()
  challenge <- pkce$challenge
  verifier <- pkce$verifier

  res <- httr::POST(
    url = paste0(auth_vars$base_url, "/oauth/device_authorization"),
    httr::add_headers(`Content-Type` = "application/json"),
    body = list(
      client_id = auth_vars$client_id,
      scope = paste(scope, collapse=" "),
      amr_values = amr_values,
      code_challenge = challenge,
      code_challenge_method = 'S256',
      access_type = 'offline'
    ),
    encode = "json",
    config = if (auth_vars$verify_ssl) httr::config() else httr::config(ssl_verifypeer = FALSE)
  )

  httr::stop_for_status(res)
  parsed_response <- httr::content(res, "parsed")

  browse_url_response <- browseURL(parsed_response$verification_uri_complete)

  # If successful, will be NULL. Otherwise will return 0.
  if (is.null(browse_url_response)) {
    cat('Please authenticate with your Redivis account. Opening browser to:\n')
    cat(parsed_response$verification_uri_complete, '\n')
  } else {
    cat('Please visit the URL below to authenticate with your Redivis account:\n')
    cat(parsed_response$verification_uri_complete, '\n')
  }
  flush.console()

  headers <- c()
  if (upgrade_credentials && !is.null(auth_vars$cached_credentials)){
    headers <- c('Authorization' = str_interp("Bearer ${auth_vars$cached_credentials$access_token}"))
  }

  started_polling_at <- Sys.time()
  while (TRUE) {
    if (difftime(Sys.time(), started_polling_at, units = "secs") > 60 * 10) {
      stop('Timed out waiting for device authorization')
    }

    Sys.sleep(ifelse(is.null(parsed_response$interval), 5, parsed_response$interval))

    res <- httr::POST(
      url = paste0(auth_vars$base_url, "/oauth/token"),
      httr::add_headers(headers),
      body = list(
        client_id = auth_vars$client_id,
        grant_type = 'urn:ietf:params:oauth:grant-type:device_code',
        device_code = parsed_response$device_code,
        code_verifier = verifier
      ),
      encode = "form",
      config = if (auth_vars$verify_ssl) httr::config() else httr::config(ssl_verifypeer = FALSE)
    )

    if (httr::status_code(res) == 200) {
      cat("Authentication was successful!")
      break
    } else if (httr::status_code(res) == 400) {
      error_response <- httr::content(res, "parsed")
      if (error_response$error == 'authorization_pending') {
        # authorization pending
      } else {
        stop(error_response)
      }
    } else {
      stop(httr::content(res, "parsed"))
    }
  }

  auth_vars$cached_credentials <- httr::content(res, "parsed")
  write(jsonlite::toJSON(auth_vars$cached_credentials, pretty = TRUE, auto_unbox=TRUE), auth_vars$credentials_file)

  return(auth_vars$cached_credentials)
}

refresh_credentials <- function(scope=NULL, amr_values=NULL) {
  # Recreate, in case REDIVIS_API_ENDPOINT has changed
  auth_vars$base_url <- sub("(https?://.*?)(/|$).*", "\\1", Sys.getenv('REDIVIS_API_ENDPOINT', 'https://redivis.com'))
  if (!is.null(scope) || !is.null(amr_values)){
    perform_oauth_login(
      scope=if (is.null(scope)) get_current_credential_scope() else scope,
      amr_values=amr_values,
      upgrade_credentials=TRUE
    )
  }else if (!is.null(auth_vars$cached_credentials$refresh_token)) {
    res <- httr::POST(
      url = paste0(auth_vars$base_url, "/oauth/token"),
      body = list(
        client_id = auth_vars$client_id,
        grant_type = 'refresh_token',
        refresh_token = auth_vars$cached_credentials$refresh_token
      ),
      encode = "form",
      config = if (auth_vars$verify_ssl) httr::config() else httr::config(ssl_verifypeer = FALSE)
    )

    if (httr::status_code(res) >= 400) {
      clear_cached_credentials()
    } else {
      refresh_response <- httr::content(res, "parsed")
      auth_vars$cached_credentials$access_token <- refresh_response$access_token
      auth_vars$cached_credentials$expires_at <- refresh_response$expires_at
      auth_vars$cached_credentials$expires_in <- refresh_response$expires_in
      write(jsonlite::toJSON(auth_vars$cached_credentials, pretty = TRUE, auto_unbox=TRUE), auth_vars$credentials_file)
    }
  } else {
    clear_cached_credentials()
  }

  return(get_auth_token())
}

get_current_credential_scope <- function() {
  tryCatch({
    if (!is.null(auth_vars$cached_credentials)) {
      token_payload = strsplit(auth_vars$cached_credentials$access_token, ".", fixed=TRUE)[[1]][2]
      decoded_token <- jsonlite::fromJSON(rawToChar(base64enc::base64decode(token_payload)))
      return(strsplit(decoded_token$scope, " ")[[1]])
    }
  }, error = function(e) {
    # Ignore errors
  })

  return(auth_vars$default_scope)
}

get_pkce <- function() {
  verifier <- safe_encode_base64_url(charToRaw(paste(sample(c(0:9, letters, LETTERS), 64, replace = TRUE), collapse = "")))
  challenge <- safe_encode_base64_url(openssl::sha256(charToRaw(verifier)))
  list(challenge = challenge, verifier = verifier)
}

# Helper function to handle base64 url encoding without padding
safe_encode_base64_url <- function(x) {
  sub("\\=", "", jsonlite::base64url_enc(x))
}
