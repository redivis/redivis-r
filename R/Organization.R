Organization <- setRefClass("Organization",
  fields = list(name="character"),
  methods = list(
    dataset = function(name, version="current") {
      Dataset$new(name=name, version=version, organization=.self)
    }
  )
)

