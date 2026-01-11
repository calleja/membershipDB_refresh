# Plan: Run proc, fetch results, inject into source DB

TL;DR: Build a small Python CLI that connects to two MySQL databases (each on a DigitalOcean droplet), optionally deploys/compiles a stored procedure on the remote droplet, executes it (or runs a provided query), streams results in batches, and inserts them into a staging table on the source droplet before an optional atomic swap.

TODO
- [ ] Inspect SQL files (e.g., drupaldb_instrx.sql, select_activity_details_raw.sql) for proc/query and columns
- [ ] Create `sync_proc_results.py` and `requirements.txt`
- [ ] Add SSH tunneling or private-network connection support
- [ ] Implement batching, transactions, and validation
- [ ] Add CLI args and ENV-based credential handling
- [ ] Add optional table swap and cleanup

Steps
1. Inputs: path-to-sql-file or proc-name, remote DB connection, source DB connection, batch-size, staging-table-name, optional SSH/ssh-key.
2. If SQL file contains `CREATE PROCEDURE`/`CREATE FUNCTION`, execute it on the remote DB to compile. Otherwise skip compile.
3. Execute `CALL proc_name(...)` or run provided SELECT on the remote DB; fetch results as a stream/iterator.
4. Insert rows into a staging table on the source DB using prepared statements in batches (default 1000). Use transactions per-batch; on failure, rollback and report.
5. Validate row counts and optionally perform atomic swap: RENAME staging->target and target->backup, then drop backup after verification.

Further considerations
- Credentials: use environment variables or a `.env` file (do not hardcode). Use DigitalOcean private networking when possible; otherwise use SSH tunneling (sshtunnel).
- Schema: ensure destination table column order matches result set; include simple transformation/column-mapping if required.
- Performance: use INSERT ... VALUES with multi-row batches; adjust batch size based on network/DB performance; use server-side cursors for large result sets.

Example packages: mysql-connector-python (or PyMySQL), sshtunnel (if needed), python-dotenv (optional).

Usage example (CLI idea)
- sync_proc_results.py --remote-sql ./proc.sql --remote-db "host:port,user,pass,db" --local-db "host:port,user,pass,db" --staging-table my_schema.staging_table --batch-size 1000 --ssh-host remote-droplet --ssh-key ~/.ssh/id_rsa

Security
- Use least-privilege DB users, TLS/SSL, and secure handling of SSH keys and env vars. Log minimal sensitive info.

Next: implement `sync_proc_results.py` and `requirements.txt`.