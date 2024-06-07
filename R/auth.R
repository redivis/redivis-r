library(jsonlite)
library(httr)

redivis_dir <- file.path(Sys.getenv("HOME"), ".redivis")
cached_credentials <- NULL
verify_ssl <- !grepl("https://localhost", Sys.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com"), fixed = TRUE)
credentials_file <- file.path(redivis_dir, "credentials")
scope <- 'data.edit'
client_id <- 'Ah850nGnQg5mFWd25nkyk9Y3'
base_url <- sub("(https?://.*?)(/|$).*", "\\1", Sys.getenv('REDIVIS_API_ENDPOINT', 'https://redivis.com'))

#' @importFrom jsonlite fromJSON toJSON
get_auth_token <- function() {
  if (!is.na(Sys.getenv("REDIVIS_API_TOKEN", unset=NA))) {
    if (is.na(Sys.getenv("REDIVIS_NOTEBOOK_JOB_ID", unset=NA)) && interactive()) {
      warning("Setting the REDIVIS_API_TOKEN for interactive sessions is deprecated and highly discouraged.
Please delete the token on Redivis and remove it from your code, and follow the authentication prompts here instead.

This environment variable should only ever be set in a non-interactive environment, such as in an automated script or service.")
    }
    return(Sys.getenv("REDIVIS_API_TOKEN"))
  } else if (is.null(cached_credentials) && file.exists(credentials_file)) {
    tryCatch({
      cached_credentials <<- jsonlite::fromJSON(readLines(credentials_file))
    }, error = function(e) {
      # ignore
    })
  }

  if (!is.null(cached_credentials) && "expires_at" %in% names(cached_credentials) && "access_token" %in% names(cached_credentials)) {
    if (cached_credentials$expires_at < (as.numeric(Sys.time()) - 5 * 60)) {
      return(refresh_credentials())
    } else {
      return(cached_credentials$access_token)
    }
  } else {
    if (!dir.exists(redivis_dir)) {
      dir.create(redivis_dir)
    }

    cached_credentials <<- perform_oauth_login()
    write(jsonlite::toJSON(cached_credentials, pretty = TRUE), credentials_file)
    return(cached_credentials$access_token)
  }
}

clear_cached_credentials <- function() {
  cached_credentials <<- NULL
  if (file.exists(credentials_file)) {
    file.remove(credentials_file)
  }
}

#' @importFrom httr POST config add_headers stop_for_status content
perform_oauth_login <- function() {
  pkce <- get_pkce()
  challenge <- pkce$challenge
  verifier <- pkce$verifier

  res <- httr::POST(
    url = paste0(base_url, "/oauth/device_authorization"),
    httr::add_headers(`Content-Type` = "application/json"),
    body = list(
      client_id = client_id,
      scope = scope,
      code_challenge = challenge,
      code_challenge_method = 'S256',
      access_type = 'offline'
    ),
    encode = "json",
    config = if (verify_ssl) httr::config() else httr::config(ssl_verifypeer = FALSE)
  )

  httr::stop_for_status(res)
  parsed_response <- httr::content(res, "parsed")

  browse_url_response <- browseURL(parsed_response$verification_uri_complete)

  print('testing')
  # If successful, will be NULL. Otherwise will return 0.
  if (is.null(browse_url_response)) {
    print('Please authenticate with your Redivis account. Opening browser to:\n')
    flush.console()

    print(parsed_response$verification_uri_complete, '\n')
    flush.console()

  } else {
    print('Please visit the URL below to authenticate with your Redivis account:\n')
    flush.console()

    print(parsed_response$verification_uri_complete, '\n')
    flush.console()

  }
  flush.console()

  started_polling_at <- Sys.time()
  while (TRUE) {
    if (difftime(Sys.time(), started_polling_at, units = "secs") > 60 * 10) {
      stop('Timed out waiting for device authorization')
    }


    Sys.sleep(ifelse(is.null(parsed_response$interval), 5, parsed_response$interval))

    res <- POST(
      url = paste0(base_url, "/oauth/token"),
      body = list(
        client_id = client_id,
        grant_type = 'urn:ietf:params:oauth:grant-type:device_code',
        device_code = parsed_response$device_code,
        code_verifier = verifier
      ),
      encode = "form",
      config = if (verify_ssl) config() else config(ssl_verifypeer = FALSE)
    )

    if (status_code(res) == 200) {
      break
    } else if (status_code(res) == 400) {
      error_response <- content(res, "parsed")
      if (error_response$error == 'authorization_pending') {
        # authorization pending
      } else {
        stop(error_response)
      }
    } else {
      stop(content(res, "parsed"))
    }
  }

  return(content(res, "parsed"))
}

#' @importFrom httr POST config content
#' @importFrom jsonlite toJSON
refresh_credentials <- function() {
  if (!is.null(cached_credentials$refresh_token)) {
    res <- POST(
      url = paste0(base_url, "/oauth/token"),
      body = list(
        client_id = client_id,
        grant_type = 'refresh_token',
        refresh_token = cached_credentials$refresh_token
      ),
      encode = "form",
      config = if (verify_ssl) config() else config(ssl_verifypeer = FALSE)
    )

    if (status_code(res) >= 400) {
      clear_cached_credentials()
    } else {
      refresh_response <- httr::content(res, "parsed")
      cached_credentials$access_token <- refresh_response$access_token
      cached_credentials$expires_at <- refresh_response$expires_at
      cached_credentials$expires_in <- refresh_response$expires_in
      write(jsonlite::toJSON(cached_credentials, pretty = TRUE), credentials_file)
    }
  } else {
    clear_cached_credentials()
  }

  return(get_auth_token())
}

#' @importFrom openssl sha256
get_pkce <- function() {
  verifier <- safe_encode_base64_url(charToRaw(paste(sample(c(0:9, letters, LETTERS), 64, replace = TRUE), collapse = "")))
  challenge <- safe_encode_base64_url(openssl::sha256(charToRaw(verifier)))
  list(challenge = challenge, verifier = verifier)
}

# Helper function to handle base64 url encoding without padding
#' @importFrom jsonlite base64url_enc
safe_encode_base64_url <- function(x) {
  sub("\\=", "", jsonlite::base64url_enc(x))
}
