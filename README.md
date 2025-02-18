![Redivis Logo](https://github.com/redivis/redivis-python/raw/main/assets/logo_small.png)
# redivis-r
[Redivis](https://redivis.com) client library for R! 

Connect data on Redivis to the R scientific stack, upload new data to Redivis, and script your data management
tasks!

## Getting started

The easiest way to get started is to [create a new R notebook on Redivis](https://docs.redivis.com/reference/workflows/notebooks/r-notebooks).
This package, alongside other common data science R packages, are all preinstalled.

You can also install the latest version of this package in any other R environment:

```r
devtools::install_github("redivis/redivis-r", ref="main")
```

And then you're ready to go!

```r
library("redivis") 

organization <- redivis$organization("demo")
dataset <- organization$dataset("cms_2014_medicare_data")
table <- dataset$table("home_health_agencies")

df <- table$to_tibble()
```

## Documentation

This package contains a number of methods for processing data on Redivis and interfacing with the API through R. Comprehensive
documentation and examples are available at https://apidocs.redivis.com/client-libraries/redivis-r

## Issue reporting

Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-r/issues).

## Contributing

Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-r/issues)
— your feedback is much appreciated!
