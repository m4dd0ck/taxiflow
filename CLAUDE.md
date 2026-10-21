# taxiflow: rules for agents

Instructions for coding agents working in this repo; a human reader wants `README.md`. Read
`README.md` for how it works, `RUNBOOK.md` for procedures, `TASK.md` for what is open. The
owner's global agent rules apply on top of these.

- Public repo: no personal data, account figures, third-party names or the owner's email in any file; a pre-commit scan blocks them.
- Real-time commits only; never backdate, never set `GIT_AUTHOR_DATE`.
- Release steps: `RUNBOOK.md` §Release.
- README, RUNBOOK, CLAUDE and TASK each hold one kind of fact; the README commands table is
  generated, never hand-edited.
- Traps: run dbt from the repo root, with `--profiles-dir .` when a home-directory profile could shadow the repo's; the README's figures come from a full two-month rebuild, never from the CI sample; `data/` is gigabytes and never committed; the Dagster ingestion asset imports `scripts.download_data`, so a change to the script's helpers breaks the asset.
