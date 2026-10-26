# Geographic data, uploaded as GeoJSON

test_that("GeoJSON is uploaded with a geography variable", {
  expect_equal(
    variable_types(live_geo_table()$list_variables()),
    c(geometry = "geography", name = "string", value = "integer")
  )
})

test_that("to_sf_tibble() reads geographies as sf geometries", {
  skip_if_not_installed("sf")

  df <- live_geo_table()$to_sf_tibble(progress = FALSE)

  expect_s3_class(df, "sf")
  expect_equal(sf::st_crs(df)$epsg, 4326L)
  expect_equal(attr(df, "sf_column"), "geometry")
  df <- df[order(df$value), ]
  for (i in seq_along(geo_features)) {
    feature <- geo_features[[i]]
    expect_equal(df$name[[i]], feature$name)
    expect_equal(df$value[[i]], feature$value)
    geometry <- sf::st_geometry(df)[[i]]
    expect_equal(as.character(sf::st_geometry_type(geometry)), toupper(feature$type))
    # Edges come back densified, as geodesics, so compare their extents
    coordinates <- matrix(unlist(feature$coordinates), ncol = 2, byrow = TRUE)
    expect_equal(
      unname(as.vector(sf::st_bbox(geometry))),
      c(min(coordinates[, 1]), min(coordinates[, 2]), max(coordinates[, 1]), max(coordinates[, 2])),
      tolerance = 1e-6
    )
  }
})

test_that("the geography variable can be chosen", {
  skip_if_not_installed("sf")

  df <- live_geo_table()$to_sf_tibble(
    geography_variable = "geometry",
    progress = FALSE
  )
  expect_equal(attr(df, "sf_column"), "geometry")

  expect_error(
    live_geo_table()$to_sf_tibble(geography_variable = "name_typo", progress = FALSE),
    class = "redivis_not_found_error"
  )
})

test_that("geographies are read as WKT by the other methods", {
  df <- live_geo_table()$to_tibble(progress = FALSE)

  expect_type(df$geometry, "character")
  expect_match(df$geometry[df$name == "point"], "^POINT ?\\(")
})

test_that("a table without a geography variable can't be read as sf", {
  skip_if_not_installed("sf")

  expect_error(
    live_tiny_table()$to_sf_tibble(progress = FALSE),
    "geography",
    class = "redivis_not_found_error"
  )
})
