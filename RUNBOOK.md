# Runbook

Procedures for taxiflow. `README.md` describes what each command does; this file says when to
run it and in what order.

## On open

```bash
git status --short
git log -3 --oneline
```

## On close

```bash
uv run dbt parse --profiles-dir .
```

The data tests need built models and the downloaded data, so they are not run at close; they
run in CI on a sampled month and locally under §Validate.

## Full rebuild

1. `uv sync`, then `uv run dbt deps --profiles-dir .` (once, or after `make clean`).
2. `uv run python scripts/download_data.py --start YYYY-MM --end YYYY-MM`; set the same range
   in `dbt_project.yml` vars. Whole months are large; two months is the documented default.
3. `uv run dbt build --profiles-dir .`. Facts are incremental: a changed staging rule needs
   `--full-refresh`.
4. `uv run dbt test --profiles-dir .` if build was split; the sample queries in the README run
   against `data/taxiflow.duckdb`.

## Dagster

`make manifest`, then `make dagster` for the UI or `make pipeline` for the full job. The
schedules only fire while the dev server (or a daemon) is running.

## Release

Conventions in `~/developer/hq/CONVENTIONS.md` §Versioning.

1. Bump `version` in `pyproject.toml`; `uv lock`.
2. Commit `chore: Release vX.Y.Z`, then `git tag -a vX.Y.Z -m "vX.Y.Z"` and
   `git push origin main --tags`.
3. `gh release create vX.Y.Z --title "taxiflow vX.Y.Z" --notes "<what changed>"` with the
   compare link to the previous tag, marked latest.
4. CI must be green on the tagged commit (the sampled build). Nothing pins this package.

## Restore on a new machine

1. Clone `git@github.com:m4dd0ck/taxiflow.git`; `uv sync`; `uv run dbt deps --profiles-dir .`.
2. Download data (§Full rebuild). No secrets: the TLC bucket is public.

## Things that bite

- Run dbt from the repo root; `--profiles-dir .` keeps a profile in the home directory from
  shadowing the repo's `profiles.yml`.
- The sample tables in the README come from a full two-month rebuild; a sampled month gives
  different figures. Refresh them only after a full rebuild.

## Validate

```bash
uv run dbt parse --profiles-dir .
uv run dbt build --profiles-dir .
python3 ~/.claude/scripts/doc-check.py --repo .
```
