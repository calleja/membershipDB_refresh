Plan: Update membership_db from scheduled SQL

TL;DR — Use a small Python package (CLI `uv`) under `membershipDB_refresh/` to run the parameterized `select_activity_details_parameterized.sql`, validate the output against stored historical fixtures using `pytest`, then deliver the validated CSV to the production droplet. Prefer a self-hosted runner on a DigitalOcean droplet (same VPC) and gate deploys behind tests and optional manual approval.

Steps
1. Create Python project: add `pyproject.toml`, `src/uv/` and entry `uv` (CLI) in `membershipDB_refresh/`.
2. Add runner and validator: `src/uv/query_runner.py` (executes SQL, writes CSV) and `src/uv/validator.py` (sanity checks vs `tests/fixtures/`).
3. Add tests and fixtures: `tests/test_query_runner.py` and `tests/fixtures/select_activity_snapshot.csv`.
4. Add GitHub Actions workflow: `.github/workflows/scheduled-select-activity.yml` (prepare → query-source → validate → deploy-target).
5. Choose runner & delivery: set up self-hosted runner on DO droplet and use SCP + remote import script; store secrets in GitHub Secrets.

Further Considerations
1. Runner choice: Self-hosted runner recommended (same private network). Option B: GitHub-hosted + SSH tunnel if self-hosted not possible.
2. Secrets & safety: Use `SRC_DB_*`, `TGT_DB_*`, `DO_SSH_PRIVATE_KEY`, keep least-privilege DB accounts, and use transactional import (temp table + swap).
3. Tests: keep small canonical fixtures in `tests/fixtures/`; larger historical archives in DO Spaces/GitHub artifacts.

Detailed recommendations (for implementation next)

- Repository layout to add (suggested):
  - `membershipDB_refresh/pyproject.toml` — project metadata, dependencies, and console script entry for `uv`.
  - `membershipDB_refresh/src/uv/__init__.py` — package init.
  - `membershipDB_refresh/src/uv/cli.py` — CLI entry implementing `main()` that parses CLI args and calls runner functions.
  - `membershipDB_refresh/src/uv/query_runner.py` — contains `run_query()` to execute SQL, capture results to CSV; supports local `mysql` via SSH or direct DB connection.
  - `membershipDB_refresh/src/uv/validator.py` — contains `validate_csv_against_fixture()` utilities: column checks, row count thresholds, sample-key checks, per-column hash.
  - `membershipDB_refresh/tests/test_query_runner.py` — pytest tests that call validator on sample output.
  - `membershipDB_refresh/tests/fixtures/select_activity_snapshot.csv` — small canonical historical snapshot for a stable date.
  - `.github/workflows/scheduled-select-activity.yml` — the scheduled GitHub Actions workflow.

- Workflow outline (jobs & responsibilities):
  1. `prepare` (runs on ubuntu-latest or self-hosted): set up Python (`actions/setup-python`), install dependencies (`pip install -e .[test]`), restore or download fixtures if needed.
  2. `query-source`: run the SQL against the source DB. Two approaches: run from runner with direct DB connection, or SSH to source droplet and run `mysql < select_activity_details_parameterized.sql` there. Output CSV saved as artifact.
  3. `validate`: run tests with pytest — compare produced CSV to the stored fixture(s). If tests fail, abort pipeline.
  4. `deploy-target`: after validation, deliver CSV to target droplet and run import script there (preferred: SCP + remote import using temporary table and transactional swap). Optionally require `workflow_dispatch` approval for production deploys.

- Testing strategy (sanity checks using historical results):
  - Store canonical small fixtures in `tests/fixtures/` inside repo for deterministic tests.
  - Tests to run in CI: verify column names and count; verify row-count is within acceptable delta (configurable percent or absolute); canonical sample-key checks (e.g., verify specific known contact IDs/emails exist and values unchanged); per-column hash diffs for critical columns (IDs, emails, dates) to quickly detect content drift.
  - For non-deterministic fields (timestamps, generated IDs), tests should normalize or exclude them.
  - Larger historical archives: keep as artifacts in GitHub or in DO Spaces; use them for periodic regression tests but not every scheduled run.

- Delivery options (choose one based on network and security):
  a) SCP + remote import script: runner scp CSV to target droplet, then SSH to execute `import_membership_csv.sh` which loads into a temp table, validates row counts, then swaps it into production table in a transaction.
  b) Direct MySQL connection from runner: runner connects to target DB using `mysqlclient`/`mysql` CLI and writes rows — simpler but requires DB port open to runner and strong network controls.
  c) Authenticated HTTP ingestion endpoint: POST CSV to a secure endpoint on the target droplet which runs the import logic — good if you want an API-driven flow with auth/audit.
  Recommendation: (a) if DBs are private, or (c) if you already have an ingestion API.

- GitHub Secrets & server prerequisites to configure:
  - On GitHub (repo secrets): `SRC_DB_HOST`, `SRC_DB_PORT`, `SRC_DB_USER`, `SRC_DB_PASS`, `SRC_DB_NAME`.
  - `TGT_DB_HOST`, `TGT_DB_PORT`, `TGT_DB_USER`, `TGT_DB_PASS`, `TGT_DB_NAME`.
  - If using SSH delivery: `DO_SSH_USER`, `DO_SSH_HOST` (target), `DO_SSH_PORT`, `DO_SSH_PRIVATE_KEY` (private key), optionally `DO_SSH_KNOWN_HOSTS`.
  - If using API delivery: `TGT_API_URL`, `TGT_API_TOKEN`.
  - Server-side: ensure MySQL present and import user has appropriate privileges (CREATE TEMPORARY TABLE, LOAD DATA INFILE or INSERT, SELECT), SSH keys configured for deploy user, firewall rules allow runner IPs or self-hosted runner in VPC.
  - Tests/fixtures: store canonical small fixtures in `membershipDB_refresh/tests/fixtures/` in repo; store larger historical archives as workflow artifacts or in DigitalOcean Spaces (configured with `DO_SPACES_KEY/SECRET` secrets if needed).

- Security & operational notes:
  - Use least-privilege DB accounts; never log secrets; redact outputs that may contain secrets.
  - Use transactional import patterns: import into temp table, run validation, then atomic rename into production table if validation passes.
  - Implement retries for transient failures and step timeouts in workflow.
  - Optionally gate production deploy behind manual approval (`environment` with required reviewers) in GitHub Actions.
  - Rotate SSH keys and DB credentials on schedule.

Next steps I can implement for you (pick one):
- Create the Python project skeleton + `pyproject.toml` and small `cli.py`/`query_runner.py` stubs in `membershipDB_refresh/`.
- Draft the GitHub Actions YAML file `.github/workflows/scheduled-select-activity.yml` with the described jobs and secrets usage.
- Create the parameterized SQL file `membershipDB_refresh/select_activity_details_parameterized.sql` (I previously produced one) and add it to repo.

Would you like me to create the file skeletons or the workflow YAML next?
