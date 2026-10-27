# AGENTS.md

Guidance for AI coding agents working in this repository. For how to set up,
run, and write tests, read [CONTRIBUTING.md](CONTRIBUTING.md); this file covers
the conventions and pitfalls that aren't written down elsewhere.

## What this is

`redivis` is the R client for the [Redivis](https://redivis.com) API. Users
reach everything through the `redivis` object (e.g.
`redivis$table("owner.dataset.table")$to_tibble()`), defined in
[`R/index.R`](R/index.R). Its reference documentation lives outside this repo,
at https://apidocs.redivis.com/client-libraries/redivis-r. It has Python and
JavaScript siblings (redivis-python, redivis-js), and behavior should stay
consistent with them where it overlaps (e.g., `redivis$config()`, credential
locations, cache keys).

## Layout

- `R/<Class>.R`: one R6 class per resource (`Dataset`, `Table`, `Workflow`,
  `Upload`, …). `TabularReader.R` is the shared base of `Table`, `Query`,
  `Upload`, and `ReadStream`, with the `to_*()` reading methods.
- `R/api_request.R`: `make_request()` and `make_paginated_request()`, which
  every API call goes through (auth, retries, error handling).
- `R/read_rows.R`, `R/retryable_download.R`, `R/retryable_upload.R`: reading
  rows, downloading files, and uploading data, including parallelism.
- `R/auth.R`: OAuth login and cached credentials. `R/config.R`:
  `redivis$config()` and `get_config()` / `resolve_config()`.
- `R/DBI.R`: the DBI driver and the dbplyr backend (SQL translation).
- `R/exceptions.R`: the `abort_redivis_*()` error helpers.
- `R/util.R`: shared helpers, including `str_interp()`, `str_match_group()`,
  `random_id()`, and `future_map_chunked()`.
- `src/`: C code for `File` connections and FUSE mounting (`Directory$mount()`).
  FUSE and libcurl are loaded at runtime with `dlopen`, and the FUSE code isn't
  built on Windows (guarded by `#ifndef _WIN32`).

## Commands

```sh
Rscript -e 'devtools::test(filter = "offline")'          # offline tests (seconds)
Rscript -e 'devtools::test(filter = "offline-uploads")'  # one file
Rscript -e 'devtools::document()'                         # after editing roxygen comments
Rscript -e 'devtools::check()'                            # before a release
```

Run the offline tests after any change to `R/`. Don't run the live tests
(`test-live-*.R`) unless asked: they create and delete real datasets and
workflows, and need a login to a Redivis environment.

## Conventions

- `NAMESPACE`, `man/`, and the `Collate` field in `DESCRIPTION` are generated
  by roxygen. Edit the `#'` comments (and `@include` tags, which order
  `Collate`), then run `devtools::document()`; never edit them by hand.
- String interpolation uses the internal `str_interp("${x}")` (in `util.R`),
  not stringr, which isn't a dependency.
- Raise errors with the `abort_redivis_*()` helpers, so callers can catch them
  by class (e.g., `abort_redivis_value_error()` for invalid arguments).
- API request payloads use the API's camelCase field names
  (`releaseNotes`, `publicAccessLevel`), while R arguments are snake_case. A
  misnamed field is silently ignored by the API, so check names against an
  existing call or the API docs.
- URL-encode names in request paths with `URLencode(name, reserved = TRUE)`.
- Methods that take `progress` or `max_parallelization` default them with
  `resolve_config()`, so that `redivis$config()` applies.
- Comments explain why, in full sentences; match the density of the
  surrounding code.

## Dependencies

Keep them lean: prefer base R, or a package that's already imported.
Packages that only some features use belong in `Suggests`, checked with
`requireNamespace()` before use (as `haven`, `sf`, and `data.table` are), and
skipped in tests with `skip_if_not_installed()`. dbplyr is in `Suggests` too,
with its methods registered lazily via `@exportS3Method dbplyr::<generic>`;
don't add `@importFrom dbplyr`. The package requires R 4.1 or later, so the
native pipe `|>` is fine.

## Tests

- Offline tests run against a mock API (`tests/testthat/helper-mock-api.R`);
  use `local_mock_api()`, and add mock endpoints there as needed. New tests
  follow the `test-offline-*.R` / `test-live-*.R` naming, which the test
  filters rely on.
- Parallel reads, downloads, and uploads run in `future` worker processes,
  which load the *installed* redivis, not your working copy. Tests that
  exercise them can pass or fail against stale code until it's reinstalled
  (`devtools::install()`); passing `max_parallelization = 1` avoids workers.
- Tests that can't run on Windows (FUSE, Unix file permissions) need
  `skip_on_os("windows")`.
- The tests must never read or write the developer's own credentials in
  `~/.redivis`; the live tests use `~/.redivis/test`.

## Changes, versions, and releases

- Commit messages follow Conventional Commits (`feat:`, `fix:`, `refactor:`,
  `test:`, `docs:`, `ci:`, `chore:`).
- Describe user-facing changes in `NEWS.md`, under the heading for the next
  version (e.g., `# redivis 0.14.1`), in the existing "What's new" /
  "Improvements and fixes" style. That section becomes the release notes.
- Changing `Version` in `DESCRIPTION` publishes a release once merged to
  `main` (see the Releasing section of CONTRIBUTING.md), so only bump it when
  asked, and never create tags or GitHub releases by hand.
- CI (`.github/workflows/check.yml`) runs `R CMD check` on Linux, macOS, and
  Windows for pull requests, and fails on warnings.
