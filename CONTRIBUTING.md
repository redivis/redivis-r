## Contributing

### Development setup

Install the development tools, and the package's dependencies (including those
the tests use):

```r
install.packages("devtools")
devtools::install_deps(dependencies = TRUE)
```

Load your working copy with `devtools::load_all()` (Cmd/Ctrl + Shift + L in
RStudio) to experiment in the console, or install it with `devtools::install()`.

To try the client against the local development environment:

```r
Sys.setenv(REDIVIS_API_ENDPOINT = "https://local.host:8443/api/v1")
devtools::load_all()
```

Note that the client keeps each environment's credentials apart, in
`~/.redivis/environments/<host>/`, so logging in to another environment never
replaces your credentials for redivis.com.

### Running the tests

The tests use [testthat](https://testthat.r-lib.org/) (3rd edition), and live in
two groups:

| Files                               | What they cover                                                                                                         | Need                                    |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `tests/testthat/test-offline-*.R`   | The client's internals (retries, auth, reading, uploads, downloads, mounting, SQL translation…), against a mock of the API | Nothing; they run offline in seconds      |
| `tests/testthat/test-live-*.R`      | End-to-end behavior against a live Redivis API, by uploading data and verifying what's read back                         | A running environment, and a login for it |

```r
devtools::test()                          # Everything, with the live tests run against dev
devtools::test(filter = "offline")        # Just the offline tests
devtools::test(filter = "live")           # Just the live tests
devtools::test(filter = "live-uploads")   # A single file
devtools::test_active_file()              # The file open in RStudio
```

Or, from a shell:

```sh
Rscript -e 'devtools::test(filter = "live")'
```

The live tests are skipped on CRAN, and whenever the environment they run
against can't be reached (and listed as such in the summary). `R CMD check`
skips them too, unless `NOT_CRAN=true` is set.

The live tests spend most of their time waiting for uploads to be processed,
so they take a while one at a time. To run test files in parallel, in separate
R processes:

```r
Sys.setenv(TESTTHAT_PARALLEL = "true", TESTTHAT_CPUS = 8)
devtools::test(filter = "live")
```

The slowest files are started first (see `Config/testthat/start-first` in
`DESCRIPTION`). Log in to the environment with a serial run first (any single
live test will do), since a worker process can't prompt you to.

### Live tests

#### Choosing an environment

By default, the live tests run against the dev environment, at
`https://local.host:8443`. Choose another with the `REDIVIS_TEST_ENV`
environment variable, which takes either the name of an environment or the URL
of an API:

```r
Sys.setenv(REDIVIS_TEST_ENV = "prod")
Sys.setenv(REDIVIS_TEST_ENV = "https://staging.example.com/api/v1")
devtools::test(filter = "live")
```

The named environments, and their defaults for the options below, are defined
by `live_environments` in
[`tests/testthat/helper-live.R`](tests/testthat/helper-live.R); add an entry
there to name a new one. The tests point the client at the environment
themselves (in [`setup-live.R`](tests/testthat/setup-live.R)), so there's no
need to set `REDIVIS_API_ENDPOINT`.

#### The code under test

Parts of the client run in separate R processes (parallel reads and file
uploads, via `future`), and those load `redivis` from your R library, not the
working copy that `devtools::test()` loads. So when run with `devtools`, the
live tests first install your working copy to a temporary library, which those
processes load instead (this takes a few seconds, once per run). Otherwise they
would test whatever version you last installed.

#### Authenticating

The first time you run the live tests against an environment, the client opens
a browser to log in to it, as it would for any user. The client keeps each
environment's credentials apart, in `~/.redivis/environments/<host>/`, so the
test environment's never touch your credentials for redivis.com. Set
`REDIVIS_TEST_CONFIG_DIR` to use another config directory.

For a non-interactive run (e.g., in CI, where `CI` is set and the tests fail
rather than wait for a login), provide an API token for the environment
instead:

```sh
REDIVIS_API_TOKEN=... REDIVIS_TEST_ENV=prod Rscript -e 'devtools::test(filter = "live")'
```

#### Test data

A fresh environment (like dev) has no data in it, so the tests upload their
own. The first test that needs it creates a sample dataset of tables with known
contents, which the reading, query, file, and DBI tests verify their results
against:

| Fixture              | Contents                                                              | Uploaded as      |
|----------------------|-----------------------------------------------------------------------|------------------|
| `live_tiny_table()`  | [`fixtures/tiny.csv`](tests/testthat/fixtures/tiny.csv)               | A CSV file       |
| `live_typed_table()` | `typed_data()`: a column of each scalar type, with nulls and awkward strings | A data frame (as parquet) |
| `live_geo_table()`   | `geo_features`: a point, a line, and a polygon                        | GeoJSON          |
| `live_files_table()` | `raw_files()`: a few files, including nested directories and a 3MB binary file | Raw files |

Their expected contents are defined in
[`tests/testthat/helper-test-data.R`](tests/testthat/helper-test-data.R), and
the fixtures in [`helper-live.R`](tests/testthat/helper-live.R). Each table's
upload starts as soon as the sample dataset is created, so that they're
processed concurrently, and each fixture waits for its table's. A fixture is
created once per run; if it fails, so does every test that uses it.

Tests that modify data create their own datasets, tables, and workflows, with
`local_live_dataset()`, `local_live_table()`, and `local_live_workflow()`,
rather than touching the sample data.

#### Cleanup

Everything the tests create lives in datasets and workflows named
`rtest_<timestamp>_<process id>_…`, owned by the test owner (below). Those made
by the `local_live_*()` fixtures are deleted as the test that made them
finishes, and the rest at the end of the run. Set
`REDIVIS_TEST_KEEP_RESOURCES=true` to leave them in place to inspect. Workflows
left behind by an interrupted run are deleted by a later run once they're 12
hours old, as are datasets, though only once they've been released: the API
doesn't list unreleased datasets, so an interrupted run's unreleased ones need
deleting by hand.

#### Options

All of the options are environment variables:

| Variable                       | Purpose                                                                                                                                                         |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `REDIVIS_TEST_ENV`             | The environment to test: a name from `live_environments`, or an API URL. Defaults to `dev`.                                                                       |
| `REDIVIS_TEST_OWNER`           | The user or organization that owns the datasets and workflows the tests create. Defaults to the environment's default (`demo` for dev, whose user accounts are limited to 50 datasets), then the authenticated user. |
| `REDIVIS_TEST_ORGANIZATION`    | An organization the test user belongs to, for the organization tests. Defaults to the environment's default (`demo` for dev); the tests are skipped without one. |
| `REDIVIS_TEST_KEEP_RESOURCES`  | `true` to keep the datasets the tests create.                                                                                                                   |
| `REDIVIS_TEST_SLOW`            | `true` to also run slow tests, such as a resumable upload of over 32MB.                                                                                          |
| `REDIVIS_TEST_CONFIG_DIR`      | The config directory to use, which holds the credentials for the environment under test. Defaults to `~/.redivis`.                                               |

The tests also unset any `REDIVIS_DEFAULT_*` variables in your environment, so
that they run the same everywhere.

### Writing tests

- Prefer an offline test, against the mock API in
  [`helper-mock-api.R`](tests/testthat/helper-mock-api.R) (`local_mock_api()`)
  or with `make_request()` stubbed out (`local_mocked_bindings()`), for
  anything about the client's own logic: how it builds requests, retries,
  authenticates, or handles a particular response. They're fast and
  deterministic, and can simulate failures that a live API can't.
- Use a live test to verify that the client and the API work together: that
  data survives a round trip, that a workflow behaves end to end, and so on.
  Assert on the data that comes back, compared with what was uploaded, rather
  than just that a call didn't raise.
- Live tests may run against any environment, so they can't depend on
  existing data or on each other. Start each with `live_api()` or one of the
  fixtures (which skip the test when the environment is unavailable), create
  what you need with the fixtures in
  [`helper-live.R`](tests/testthat/helper-live.R), and name anything else you
  create with `live_unique_name()`.
- Rows come back in no particular order, so compare them with
  `expect_rows_equal()`, which sorts them (or sort them with `sorted_by()`).
  Pass `progress = FALSE` to keep progress bars out of the output.
- Check for errors by their class (e.g., `class = "redivis_not_found_error"`;
  see [`R/exceptions.R`](R/exceptions.R)), rather than only their message.
- Call `skip_unless_slow()` in a test that takes more than a minute or so.

### Building

- Run `devtools::document()` to regenerate the documentation with roxygen.
- Run `devtools::check()` before a release.

See also https://r-pkgs.org/testing-basics.html.
