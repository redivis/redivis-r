#' @include User.R
Project <- setRefClass("Project",
   fields = list(name="character", user="User"),
   methods = list(
     get_identifier = function(){
       str_interp("${user$name}.${name}")
     },
     table = function(name) {
       Table(name=name, project=.self)
     },
     query = function(query){
       redivis::query(query, default_project = .self)
     }
   )
)
