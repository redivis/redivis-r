User <- setRefClass("User",
  fields = list(name="character"),
  methods = list(
    dataset = function(name, version="current") {
      Dataset$new(name=name, version=version, user=.self)
    },
    project = function(name) {
      Project$new(name=name, user=.self)
    }
  )
)
