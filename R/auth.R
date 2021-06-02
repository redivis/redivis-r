get_auth_token <- function(){
  if (Sys.getenv("REDIVIS_API_TOKEN") == ""){
    stop("The environment variable REDIVIS_API_TOKEN must be set.")
  }

  Sys.getenv("REDIVIS_API_TOKEN")
}
