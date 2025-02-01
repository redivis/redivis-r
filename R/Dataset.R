
#' @include Organization.R User.R Table.R api_request.R
Dataset <- setRefClass("Dataset",
   fields = list(
    name="character",
    version="ANY",
    user="ANY",
    organization="ANY",
    uri="character",
    qualified_reference="character",
    scoped_reference="character",
    properties="list"
  ),
   methods = list(

    initialize = function(..., name="", version=NULL, user=NULL, organization=NULL, properties=list()){
      parsed_name <- strsplit(name, ":")[[1]][1]
      version_arg <- version
      if (!is.null(version) && version != "current" && version != "next" && !startsWith(tolower(version), "v")){
        version_arg <- str_interp("v${version}")
      } else if (is.null(version) && !is.na(stringr::str_match(name, ":(v\\d+[._]\\d+|current|next)")[2])){
        version_arg <- stringr::str_match(name, ":(v\\d+[._]\\d+|current|next)")[2]
      }

      version_string <- ""
      if (!is.null(version_arg)){
        version_string <- str_interp(":${version_arg}")
      }

      reference_id <- ""
      reference_id_split <- stringr::str_split_fixed(stringr::str_replace_all(name, ":(v\\d+[._]\\d+|current|next|sample)", ""), ":", Inf)

      if (length(reference_id_split) > 1) {
        reference_id <- reference_id_split[2]
      }

      if (reference_id != "") {
        reference_id <- paste0(":", reference_id)
      }

      owner_name <- if (is.null(user)) organization$name else user$name
      scoped_reference_val <- if (length(properties$scopedReference)) properties$scopedReference else str_interp("${parsed_name}${reference_id}${version_string}")
      qualified_reference_val <- if (length(properties$qualifiedReference)) properties$qualifiedReference else str_interp("${owner_name}.${scoped_reference_val}")

      callSuper(...,
                name=parsed_name,
                version=version_arg,
                user=user,
                organization=organization,
                qualified_reference=qualified_reference_val,
                scoped_reference=scoped_reference_val,
                uri=str_interp("/datasets/${URLencode(qualified_reference_val)}"),
                properties=properties
              )
    },

    show = function(){
      print(str_interp("<Dataset ${.self$qualified_reference}>"))
    },

    create = function(public_access_level="none", description=NULL){
      if (is.null(.self$organization)){
        path <- str_interp("/users/${.self$user$name}/datasets")
      } else {
        path <- str_interp("/organizations/${.self$organization$name}/datasets")
      }

      payload = list(name=.self$name, publicAccessLevel=public_access_level)

      if (!is.null(description)){
        payload$description = description
      }

      res <- make_request(
        method="POST",
        path=path,
        payload=payload
      )
      update_dataset_properties(.self, res)
      .self
    },

  create_next_version = function(if_not_exists=FALSE){
    if (is.null(.self$properties) || is.null(attr(.self$properties, "nextVersion"))){
      .self$get()
    }
    if (is.null(.self$properties$nextVersion)){
      make_request(method="POST", path=str_interp("/${.self$uri}/versions"))
    } else if (!if_not_exists){
      stop(str_interp("Next version already exists at ${.self$properties$nextVersion$datasetUri}. To avoid this error, set argument if_not_exists to TRUE"))
    }

    Dataset$new(name=.self$name, user=.self$user, organization=.self$organization, version="next")$get()
  },

  delete = function(){
    make_request(method="DELETE", path=.self$uri)
  },

  get = function(){
    res <- make_request(path=.self$uri)
    update_dataset_properties(.self, res)
    .self
  },

  exists = function(){
    res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
    if (length(res$error)){
      if (res$status == 404){
        return(FALSE)
      } else {
        stop(res$message)
      }
    } else {
      return(TRUE)
    }
  },

  list_tables = function(max_results=NULL) {
    tables <- make_paginated_request(
      path=str_interp("/${.self$uri}/tables"),
      page_size=100,
      max_results=max_results
    )
    purrr::map(tables, function(table_properties) {
      Table$new(name=table_properties$name, dataset=.self, properties=table_properties)
    })
  },

  query = function(query){
    redivis::query(query, default_dataset=.self$qualified_reference)
  },

  release = function(){
    version_res <- make_request(method="POST", path=str_interp("${.self$uri}/versions/next/release"))
    .self$uri = version_res$datasetUri
    .self$get()
    .self
  },

  table = function(name) {
    Table$new(name=name, dataset=.self)
  },

  update = function(name=NULL, public_access_level=NULL, description=NULL){
    payload <- list()
    if (!is.null(name)){
      payload <- append(payload, list("name"=name))
    }
    if (!is.null(public_access_level)){
      payload <- append(payload, list("public_access_level"=public_access_level))
    }
    if (!is.null(description)){
      payload <- append(payload, list("description"=description))
    }
    res <- make_request(
      method="PATCH",
      path=.self$uri,
      payload=payload,
    )
    update_dataset_properties(.self, res)
    .self
  }
  )
)

update_dataset_properties <- function(instance, properties){
  instance$properties = properties
  instance$qualified_reference = properties$qualifiedReference
  instance$scoped_reference = properties$scopedReference
  instance$name = properties$name
  instance$uri = properties$uri
  instance$version = properties$version$tag
}


