# redivis 0.14.1

## Improvements and fixes

- `version$tag` is now always the version's tag as the API returns it, e.g.
  `v1.0`, rather than sometimes the form used in references (`v1_0`), e.g. for
  `dataset$version()` on a dataset referenced as `owner.dataset:v1_0`. Either
  form is still accepted wherever a tag is passed.

# redivis 0.14.0

## What's new

- New `redivis$config()` sets library-wide defaults: `default_workflow`,
  `default_dataset`, `default_user`, `default_organization`, `api_endpoint`,
  `config_dir`, `tmp_dir`, `max_parallelization`, and `progress`. Each falls
  back to its `REDIVIS_*` environment variable, and methods' `progress` and
  `max_parallelization` arguments now default to the configured values.
  ```r
  redivis$config(default_workflow = "username.workflow_name", progress = FALSE)
  ```
- Credentials for environments other than redivis.com are now kept apart, in
  `<config_dir>/environments/<host>/`, so logging in to one never replaces your
  credentials for another. Credentials for redivis.com stay in
  `~/.redivis/r_credentials`, and the directory can be changed with
  `config_dir` (or `REDIVIS_CONFIG_DIR`).
- `dataset$release()`, `dataset$unrelease()`, and `version$delete()` take a
  `wait_for_finalization` argument, to wait until the version's data has been
  moved, so that the dataset can be released, unreleased, or deleted again
  right away.
- New `workflow$create()`, and `workflow$delete()`, `transform$delete()`, and
  `datasource$delete()` methods.

## Improvements and fixes

- `dataset$unrelease()` now unreleases the current version, rather than
  sending a release request.
- `dataset$release(release_notes = )`, `dataset$update(public_access_level = )`,
  and `variable$update(value_labels = )` now take effect; they were previously
  sent to the API under the wrong names, and ignored.
- Uploading an sf data frame now uploads its geometries as GeoParquet (WKB),
  transformed to WGS84 when needed, so that they're read as geographies.
- Downloads are more robust: they're written to a partial file that's only
  moved into place once it's complete and verified against the file's size and
  MD5 hash, resume where they left off when a connection drops, and are retried
  from scratch once if the data was corrupted in transit.
- Uploads resume from the data that's already been received after an
  interruption, and start over if their upload session has expired. Their
  progress bars no longer overcount.
- An upload's name is checked for conflicts before its data is sent, and
  `remove_on_fail` now removes a failed upload even when `raise_on_fail` is
  `FALSE`.
- Reading rows that can't be returned in a single response (e.g., those of an
  unreleased version whose uploads have different schemas) now reads them
  through a read session, rather than raising an error.
- Reading the rows of a query that hasn't run yet (e.g.,
  `redivis$query("...")$to_tibble()`) now gets small results back in the same
  request that runs the query, when it finishes quickly, rather than through a
  read session.
- Columns returned with different capitalization are matched to the table's
  variables.
- GET requests are retried after network failures.
- A failed request to refresh your credentials (e.g., during an outage) now
  raises an error, rather than logging you out; only a rejected refresh token
  does.
- `REDIVIS_DEFAULT_USER` and `REDIVIS_DEFAULT_ORGANIZATION` no longer cause an
  error when set.
- Version tags written with a dot in references (e.g.,
  `username.dataset:v1.0`) now work, like `:v1_0`.
- Names containing special characters are now encoded in request URLs (users,
  organizations, secrets, variables, and parameters).
- `dataset$previous_version()` and `dataset$next_version()` return `NULL` for
  the first and latest versions, rather than raising an error.
- `file$read()` returns the right bytes for a range when the server sends the
  whole file.
- `table$file()` and `table$to_directory()` keep a separate directory for each
  `file_id_variable` and `file_name_variable`.
- Read streams now raise a clear error when passed `variables` or
  `max_results`, which are set when they're created.
- `notebook$run(wait_for_finish = TRUE)` and
  `transform$run(wait_for_finish = TRUE)` wait for the run they started, and
  `notebook$run()` now raises an error if the run is cancelled.

## Dependencies

- redivis now requires R 4.1.0 or later.
- dbplyr and data.table are now optional. Install dbplyr to use dplyr verbs
  with a `RedivisDBI()` connection, and data.table to use `to_data_table()`.
- redivis no longer depends on stringr, purrr, furrr, digest, or uuid, so it
  installs fewer packages.
