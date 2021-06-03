#' @include User.R
#' @include Organization.R
Dataset <- setRefClass("Dataset",
   fields = list(name="character", version="character", user="User", organization="Organization"),
   methods = list(
     get_identifier = function(){
       owner <- if(length(user$name) == 0) organization else user
       str_interp("${owner$name}.${name}")
     },
     table = function(name) {
       Table$new(name=name, dataset=.self)
     },
     query = function(query){
       redivis::query(query, default_dataset = .self)
     }
   )
)
