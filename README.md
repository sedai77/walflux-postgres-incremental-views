# WalFlux

**Millisecond-fresh materialized views for Postgres — no extensions, no triggers, no second database.**

[![CI](https://github.com/nexzensoftware-ship-it/walflux-postgres-incremental-views/actions/workflows/ci.yml/badge.svg)](https://github.com/nexzensoftware-ship-it/walflux-postgres-incremental-views/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/walflux)](https://pypi.org/project/walflux/)
[![Python versions](https://img.shields.io/pypi/pyversions/walflux)](https://pypi.org/project/walflux/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

WalFlux is a single daemon that keeps aggregate tables (`COUNT` / `SUM` / `AVG` with
`GROUP BY`) continuously up to date from your Postgres write-ahead log, applying each
source transaction's deltas **exactly once** — provably, across crashes, including
`kill -9`.

## Why

The standard answers to "I need fast aggregates over a busy table" all hurt somewhere:

- **`REFRESH MATERIALIZED VIEW`** recomputes the entire view every time and takes
  locks — there is no real "postgres materialized view auto refresh"; you get a cron
  job and staleness measured in minutes.
- **Trigger-based counters** are fresh, but they run inside your write path: every
  `INSERT` pays the tax, hot rows contend, and deadlocks arrive with scale.
- **Stream processors** (Kafka + Flink, Materialize, ...) solve it properly — at the
  cost of operating a second distributed system and a change-data-capture pipeline to
  feed it.

WalFlux takes a fourth path: **incremental view maintenance** driven by **logical
replication**. It reads committed changes from a replication slot using the built-in
`pgoutput` decoder — the same change data capture mechanism Postgres uses for its own
replicas — folds them into per-group deltas, and applies them to plain tables in your
existing database. Because `pgoutput` is built into Postgres, WalFlux works on managed
services where you cannot install extensions: **RDS, Cloud SQL, Supabase, Neon**, and
any self-hosted Postgres 15+ with `wal_level=logical`. One daemon, your database,
nothing else to run.

## 60-second quickstart

The [`demo/`](demo/) directory is a self-contained docker-compose setup: Postgres 16,
a workload generator doing ~30 mixed writes/sec, and WalFlux maintaining two views.

```bash
git clone https://github.com/nexzensoftware-ship-it/walflux-postgres-incremental-views.git
cd walflux
make demo        # bring it up and follow the daemon's flush log
```

Then run the proof — kill the daemon with `SIGKILL` mid-write-storm, let writes pile
up while it's dead, restart it, and verify the targets against ground truth:

```bash
make kill9
```

Expected ending (abridged):

```
view orders_by_status  (walflux.orders_by_status vs GROUP BY over public.orders)
group             column           ground truth  walflux   result
----------------  ---------------  ------------  --------  ------
status=cancelled  order_count      57            57        ok
status=cancelled  revenue          13921.66      13921.66  ok
...

================================================================
  PASS — every walflux target matches its ground-truth GROUP BY
================================================================
```

It passes because each batch of deltas commits in the **same target transaction** as
its replication checkpoint, and redelivered transactions are discarded by comparing
their commit LSN against that checkpoint. No timing luck involved — see
[How it works](#how-it-works).

## Real setup

```bash
pip install walflux
```

Write a config:

```yaml
database:
  dsn: "postgresql://user:pass@localhost:5432/db"   # or set WALFLUX_DSN
views:
  - name: orders_by_status
    source: public.orders
    group_by: [status]
    aggregates:
      - { fn: count, as: order_count }                  # COUNT(*)
      - { fn: count, column: coupon, as: with_coupon }  # COUNT(coupon)
      - { fn: sum, column: total, as: revenue }
      - { fn: avg, column: total, as: avg_order_value }
```

Then:

```bash
walflux setup -c walflux.yaml   # publication + slot + target tables + consistent backfill
walflux run   -c walflux.yaml   # the daemon (foreground; logs to stderr)
walflux status -c walflux.yaml  # slot, lag, checkpoint, per-view row counts
```

Query your aggregates as ordinary tables:

```sql
SELECT status, order_count, revenue, avg_order_value
FROM walflux.orders_by_status;
```

Run `walflux run` under a supervisor (systemd `Restart=on-failure`, or compose
`restart: unless-stopped`) — restarts are the retry story, and they are safe by
construction. `walflux teardown -c walflux.yaml --yes` removes the slot, publication,
and the `walflux` schema when you're done.

## How it works

```mermaid
sequenceDiagram
    participant WAL as Postgres WAL
    participant Slot as Logical slot (pgoutput v1)
    participant D as walflux daemon
    participant T as walflux.* target tables

    WAL->>Slot: committed transactions
    Slot->>D: Begin / Insert / Update / Delete / Commit
    D->>D: decode, buffer whole transactions
    D->>D: fold batch into per-group deltas
    D->>T: BEGIN - upsert deltas + write checkpoint LSN - COMMIT
    Note over D,T: deltas and checkpoint are one transaction
    D->>Slot: ack(checkpointed LSN)
    Slot->>WAL: earlier WAL may be recycled
```

The exactly-once argument in one paragraph: logical replication is at-least-once —
the slot re-sends anything not acknowledged. WalFlux writes each batch's aggregate
deltas and the replication checkpoint in **one target transaction**, so they are
atomically both-or-neither; on restart, any redelivered transaction whose commit LSN
is at or below the checkpoint is discarded. The slot is only ever acknowledged up to
a *committed* checkpoint, so it can never discard WAL that wasn't durably applied.
Every crash window is enumerated and argued in [DESIGN.md](DESIGN.md) — that document
is the heart of this repo.

## Limitations

Honesty table. Every row has a reason, most have a roadmap entry.

| Limitation | Why |
|---|---|
| Single-table views only (no joins yet) | Incremental joins need delta-join plumbing and per-side state; it's the top [roadmap](#roadmap) item. |
| `count` / `sum` / `avg` only — no `min`/`max` | Min/max can't be maintained from deltas alone: deleting the current max needs the runner-up ([the deletable-aggregate problem](DESIGN.md#12-why-only-count--sum--avg-no-minmax)). |
| Sources get `REPLICA IDENTITY FULL` | Deletes/updates must carry full old rows so groups can be decremented; costs extra WAL on update/delete-heavy tables. |
| Postgres 15+ | Target upserts rely on `UNIQUE ... NULLS NOT DISTINCT` so NULL group keys behave like `GROUP BY`. |
| Targets live in the same database as sources | Exactly-once rides on deltas + checkpoint sharing one transaction; cross-database would need 2PC. |
| Source schema changes require `walflux setup --force` | On drift the daemon halts loudly rather than maintain silently-wrong aggregates; additive columns are fine. |

## Comparison

All of these are good tools; the question is which constraints you have.

| | WalFlux | `REFRESH MAT. VIEW` | Triggers | [pg_ivm](https://github.com/sraoss/pg_ivm) | [Materialize](https://materialize.com) | Debezium + Flink |
|---|---|---|---|---|---|---|
| Freshness | ~batch interval (200 ms default) | Minutes (cron cadence) | Synchronous | Synchronous | Sub-second | Sub-second |
| Works on managed PG (RDS, Cloud SQL, ...) | Yes — pgoutput is built in | Yes | Yes | Only where you can install extensions | Yes (reads via CDC) | Yes (reads via CDC) |
| Operational footprint | One daemon | None (but locks + full recompute) | None (but write-path latency, deadlock risk) | None — in-database, excellent if extensions are allowed | A separate streaming database | Kafka + Connect + Flink cluster |
| Joins | Not yet | Full SQL | Hand-written | Yes (inner joins) | Full SQL, incremental | Full SQL, incremental |

If you can install extensions, **pg_ivm** is a great in-database answer. If you need
incremental joins over many sources today, **Materialize** or **Flink** are built for
it. WalFlux sits in the gap: managed Postgres, aggregate views, one small process.

## Roadmap

- **Delta joins** — two-table inner joins with per-side state tables.
- **`min`/`max`** — via auxiliary per-group heaps (see the
  [design note](DESIGN.md#12-why-only-count--sum--avg-no-minmax)).
- **Snapshot re-sync without full backfill** — repair one view without rebuilding all.
- **Prometheus metrics endpoint** — flush latency, batch sizes, replication lag.

## FAQ

**The daemon was down for a while — is that dangerous?**
Correctness-wise, no: it resumes from its checkpoint. Operationally, the replication
slot retains WAL while nobody consumes it, so disk usage grows. Set
`max_slot_wal_keep_size` to bound it (if exceeded, the slot is invalidated and you
re-run `walflux setup --force`), and monitor lag with `walflux status`. Details in
[DESIGN.md §9](DESIGN.md#9-slot-bloat-the-operational-contract).

**Can the target tables live in a different database?**
Not yet. The exactly-once guarantee comes from writing deltas and the checkpoint in
one transaction, which requires them in the same database as each other — and the
backfill handshake currently assumes that's also the source database. Splitting
source from target needs a checkpoint on the target side plus a re-thought bootstrap;
it's on the radar but not the roadmap's front.

**What happens when I `ALTER TABLE` a source?**
If the change touches a column a view needs (a group key or aggregated column), the
daemon exits with a `SchemaDriftError` telling you to re-run `walflux setup --force`
(a fresh backfill). Adding unrelated columns is harmless and needs nothing.

**How big can a batch get? What about huge transactions?**
Batches are capped by `batch.max_txns` (default 500) and `batch.max_ms` (default
200 ms) — deltas collapse per group, so even large batches usually flush as few rows.
Separately, a single *source* transaction is buffered in memory until its commit
arrives (WalFlux speaks pgoutput protocol v1, which never streams in-progress
transactions — [why](DESIGN.md#11-why-pgoutput-protocol-version-1)), so a
100M-row bulk `UPDATE` will cost memory and latency. Batch bulk loads accordingly.

**Why psycopg2 and not psycopg3?**
Because the replication protocol is the one thing psycopg3 doesn't do yet: as of
psycopg 3.3, replication connections (`START_REPLICATION`) are not implemented — it's
an open feature request, [psycopg/psycopg#71](https://github.com/psycopg/psycopg/issues/71).
psycopg2's `LogicalReplicationConnection` remains the maintained Python client for
walsender-mode connections. When psycopg3 ships replication support, migrating is a
small, contained change (`walflux/replication.py` and `walflux/bootstrap.py`).

**Does WalFlux slow down my writes?**
No triggers, no hooks: source transactions commit exactly as before, and WalFlux
reads the WAL after the fact. The one write-path cost is indirect — `REPLICA
IDENTITY FULL` makes updates and deletes log full old rows, which increases WAL
volume (not commit latency in most workloads).

## Documentation

- [DESIGN.md](DESIGN.md) — the crash-by-crash correctness argument.
- [docs/SPEC.md](docs/SPEC.md) — the internal architecture contract.
- [CONTRIBUTING.md](CONTRIBUTING.md) — dev setup, tests, style.
- [demo/](demo/) — the compose demo and the `kill -9` proof.

## License

[MIT](LICENSE) — Copyright (c) 2026 WalFlux contributors.
