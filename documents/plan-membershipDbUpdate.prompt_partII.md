Plan: Update membership_db from scheduled SQL

TL;DR — Use a small Python package (CLI `uv`) under `membershipDB_refresh/` to run the parameterized
`select_activity_details_parameterized.sql`, validate the output using tests driven by historical
fixtures (`tests/fixtures/`), then inject the result set directly into the production `membership_db`
using a staging-table + atomic rename swap on the target DigitalOcean droplet. Prefer a self-hosted
runner located in the same private network/VPC as the droplets so Actions can access the databases
without exposing ports publicly.

Goals
- Run the parameterized SQL on the source DB on a schedule
- Validate the result set against historical fixtures (sanity checks)
- Inject results directly into `membership_db` on the production droplet using a safe staging + swap
- Keep the Python portion as a small package with a `uv` console script

Steps (high-level)
1. Add Python project skeleton inside `membershipDB_refresh/` (see `python_layout` below).
2. Add a parameterized SQL file `select_activity_details_parameterized.sql` (already prepared).
3. Implement `src/uv/query_runner.py` to run the SQL against the source DB and stream rows.
4. Implement `src/uv/importer.py` or `src/uv/query_runner.py` to insert rows into target DB staging table.
5. Implement `src/uv/validator.py` and `tests/fixtures/` with canonical snapshots used by `pytest`.
6. Add a GitHub Actions workflow `.github/workflows/scheduled-select-activity.yml` that runs on a schedule
   and executes: prepare → query-source → validate → deploy-target. Use `runs-on: [self-hosted, do]`.

Safe import pattern (recommended)
- Create `membership_table_staging` with `CREATE TABLE membership_table_staging LIKE membership_table`.
- `TRUNCATE TABLE membership_table_staging` before import.
- Insert rows in batches to `membership_table_staging`.
- Run validation queries on staging (row counts, sample-key checks, per-column hashes).
- If validated, `RENAME TABLE membership_table TO membership_table_backup, membership_table_staging TO membership_table;`
  (atomic on same MySQL instance). Optionally `DROP TABLE membership_table_backup` after operator confirmation.

DB privileges required (minimal)
- On source DB: `SELECT` on tables used by the query.
- On target DB (import user): `CREATE`, `INSERT`, `SELECT`, `DELETE`, `TRUNCATE`, `RENAME`, `DROP` (as limited as possible).

Python package and tooling (minimal)
- `pyproject.toml` for metadata and entry point `uv` (console script) so `pip install .` exposes `uv`.
- `src/uv/cli.py` — parse args (mode, dry-run, validation thresholds) and call runner functions.
- `src/uv/query_runner.py` — connect to source DB, execute parameterized SQL, stream rows.
- `src/uv/importer.py` — connect to target DB and insert rows into staging table in batches.
- `src/uv/validator.py` — functions to run sanity checks: column names, row-count delta, sample row assertions,
  and stable-column hashes.
- `tests/test_validator.py` and `tests/fixtures/select_activity_snapshot.csv` — canonical small fixture.
- `requirements.txt` or `[project.dependencies]` in `pyproject.toml`: `mysql-connector-python`, `pytest`, `python-dotenv`.

Testing strategy
- Keep small canonical fixtures in `tests/fixtures/` committed to repo for deterministic checks.
- Tests compare: column names, required columns exist, row counts within a configurable threshold, sample-keyed
  value assertions (IDs/emails/dates), and per-column content hash for critical columns.
- Larger historical archives can be stored as workflow artifacts or DigitalOcean Spaces and used for periodic
  regression checks (not on every run).
- CI `validate` job runs `pytest` and gates the deploy job on success.

GitHub Actions workflow outline
- File: `.github/workflows/scheduled-select-activity.yml`
- Triggers: `schedule: cron: '0 2 * * *'` and `workflow_dispatch` (manual)
- Jobs:
  - `prepare`: checkout, setup Python, install deps
  - `query-source`: run `uv query --sql select_activity_details_parameterized.sql --out-format rows` and store artifact
  - `validate`: run `pytest` using produced rows (or call `uv validate --input staging`) — fail if checks fail
  - `deploy-target`: run on `self-hosted` runner; run `uv import --source staging` to perform staged insert + swap

Delivery options (for inject instead of CSV)
- Option A — Direct-insert from runner: runner connects to source and target DBs and streams rows into target staging.
  (Recommended if runner can reach both DBs privately.)
- Option B — Remote-run on droplet: SSH to a droplet (that has network access) and run the import script there.
- Option C — API ingestion: POST result payload to an authenticated endpoint on the target droplet which performs import.

Self-hosted runner requirements (what to install on the droplet)
- OS: Ubuntu 20.04/22.04 or similar.
- Packages: `curl`, `git`, `python3`, `python3-venv`, `python3-pip`, `mysql-client` (for CLI), build tools if needed.
- Download and configure the GitHub Actions runner binary and register it to the repo/org (use a registration token).
- Create a dedicated non-root user (e.g. `actions`) to run the runner service.
- Optionally create a Python venv and preinstall runtime deps (`mysql-connector-python`, `pytest`).

Self-hosted runner vs GitHub-hosted (summary)
- Self-hosted: best for access to private DBs without exposing ports. You maintain the runner (updates/security).
- GitHub-hosted: zero maintenance but ephemeral and cannot access private networked DBs unless you expose ports or
  configure an SSH tunnel/bastion. Choose self-hosted if both DBs are in private droplets.

Secrets and server prerequisites
- GitHub Secrets to add:
  - `SRC_DB_HOST`, `SRC_DB_PORT`, `SRC_DB_USER`, `SRC_DB_PASS`, `SRC_DB_NAME`
  - `TGT_DB_HOST`, `TGT_DB_PORT`, `TGT_DB_USER`, `TGT_DB_PASS`, `TGT_DB_NAME`
  - If using SSH delivery: `DO_SSH_USER`, `DO_SSH_HOST`, `DO_SSH_PRIVATE_KEY` (private key), `DO_SSH_PORT`
  - If using Spaces for artifacts: `DO_SPACES_KEY`, `DO_SPACES_SECRET` and `DO_SPACES_REGION`
- Server-side: MySQL installed and reachable by runner; deploy/import user created with minimal privileges; SSH key auth for deploy user.

Security & operational notes
- Use least-privilege DB accounts; never log secrets or dump creds in CI logs.
- Make import idempotent where possible. Use staging table + `RENAME TABLE` to atomically swap in validated data.
- Use environment protection rules and required reviewers for `environment: production` if you want manual approvals.
- Add job timeouts and retry logic for network operations.

Next steps (I can implement any of these):
- Scaffold the Python package skeleton with `pyproject.toml` and basic `src/uv/` modules (CLI, runner, importer, validator).
- Draft the `.github/workflows/scheduled-select-activity.yml` that targets a self-hosted runner and includes test/deploy jobs.
- Produce the exact runner installation and registration script for your droplet (with placeholders for repo URL and token).

Which of the next steps would you like me to do first?
