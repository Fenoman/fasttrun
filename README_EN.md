# fasttrun

> Russian version: [README.md](README.md)

fasttrun is an extension for PostgreSQL 16, PostgreSQL 17, and PostgreSQL 18.
It speeds up a recurring workflow built around temporary tables:

1. clear the table
2. refill it
3. update planner statistics
4. run the calculation

Two functions cover this cycle:

- `fasttruncate()`, which clears a local temporary table.
- `fasttrun_analyze()`, which updates planner estimates after the table is filled.

Regular `TRUNCATE` and `ANALYZE` notify other PostgreSQL server processes about
changes to the system catalogs and relation files. Under high concurrency,
processing those notifications can consume a noticeable amount of CPU.
fasttrun performs its main work only in the current server process and, in its
standard mode, sends no shared invalidation messages.

CPU load of a production cluster before and after the rollout:

![CPU before fasttrun](docs/images/prod-cpu-before.png)

![CPU after fasttrun](docs/images/prod-cpu-after.png)

This is one specific case, not a guarantee of the same result on another system.

Installing the extension does not make anything faster by itself. The
application must explicitly call fasttrun functions instead of the built-in
commands.

## Who it is for

Consider fasttrun when:

- the application repeatedly reuses local temporary tables
- dozens of long-lived server connections operate concurrently
- regular `TRUNCATE` and `ANALYZE` produce measurable overhead
- the application can be changed to call fasttrun functions explicitly
- temporary-table contents do not have to be restored after `ROLLBACK`

A typical use case is a large PL/pgSQL calculation behind a connection pooler.
A PostgreSQL backend - the server process behind one physical connection - lives
for a long time, serves many clients, and gradually accumulates temporary tables
for reuse.

You do not need fasttrun if regular `TRUNCATE` and `ANALYZE` do not cause
noticeable overhead. Measure the problem on your own system first.

## Before you deploy

- `fasttruncate()` is not transactional: once the physical cleanup has
  started, `ROLLBACK` does not restore the previous data.
- The extension works with standalone local temporary heap tables. Foreign keys
  are not checked, and `CASCADE`, TRUNCATE triggers, and sequence resets are
  not performed.
- Call `fasttrun_analyze()` after every refill.
- If the cleanup fails after the files have changed, the table is blocked with
  `SQLSTATE 55000` until it is recovered, see
  [recovering from errors](docs/en/usage.md#recovering-from-errors).
- fasttrun is not a security boundary between pooler clients.

The full checklist and the known limitations are in
[limitations.md](docs/en/limitations.md).

## Quick start

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

```sql
CREATE EXTENSION fasttrun;

CREATE TEMP TABLE temp_work (id bigint, value numeric);

SELECT fasttruncate('pg_temp.temp_work');
INSERT INTO temp_work SELECT n, n * 1.5 FROM generate_series(1, 10000) AS g(n);
SELECT fasttrun_analyze('pg_temp.temp_work');
-- Queries against temp_work.
```

The main functions need neither a PostgreSQL restart nor
`shared_preload_libraries`: the library loads on the first call. For
statistics wear from `UPDATE` to be counted per column, client sessions need
`session_preload_libraries = 'fasttrun'`, see
[statistics.md](docs/en/statistics.md#column-statistics). Privileges,
installation into a separate schema, and upgrades are described in
[install.md](docs/en/install.md).

## Documentation

| Document | Contents |
|---|---|
| [install.md](docs/en/install.md) | Building, privileges, verifying the installation, upgrading, removing, compatibility |
| [usage.md](docs/en/usage.md) | SQL and PL/pgSQL templates, connection poolers, functions, recovering from errors |
| [statistics.md](docs/en/statistics.md) | How statistics are updated, settings, memory |
| [tracking.md](docs/en/tracking.md) | Tracking of `CREATE TEMP TABLE` and table prewarming |
| [limitations.md](docs/en/limitations.md) | Pre-deployment checklist, known limitations |
| [internals.md](docs/en/internals.md) | Internals, performance, file layout |
| [testing.md](docs/en/testing.md) | Checks and the release run |
