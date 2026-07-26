# WalFlux Internals Specification

This document is the authoritative contract for WalFlux's internal architecture.
Contributors (human or otherwise) implement against these interfaces exactly.
Public-facing rationale lives in `DESIGN.md`; this file is the "what", that one is the "why".

## What WalFlux is

A single daemon that:

1. Creates a Postgres **logical replication slot** using the built-in `pgoutput` plugin
   (no extension installation — works on RDS, Cloud SQL, Supabase, Neon, self-hosted).
2. **Backfills** aggregate target tables from the slot's exported consistent snapshot.
3. Streams decoded WAL changes and **incrementally maintains** per-view aggregate tables
   (`COUNT` / `SUM` / `AVG` with `GROUP BY`), applying each source transaction's deltas
   and the replication-position checkpoint **in one target transaction** — this is what
   makes delivery exactly-once across crashes.

Requires Postgres **15+** (we rely on `UNIQUE NULLS NOT DISTINCT` indexes) and
`wal_level=logical`. Python 3.10+. Runtime deps: `psycopg2-binary` (the replication
protocol client), `PyYAML`. Nothing else. Use stdlib `logging`, `argparse`, `dataclasses`.

## Module map and ownership

```
walflux/
  __init__.py      version only (exists; single-sourced via hatch)
  __main__.py      `python -m walflux` shim over cli.main       [Module C]
  py.typed         PEP 561 marker (empty)
  protocol.py      pgoutput binary message decoder            [Module A]
  aggregates.py    delta accumulation + SQL generation        [Module B]
  config.py        YAML config loading/validation             [Module C]
  cli.py           argparse CLI entry point                   [Module C]
  checkpoint.py    checkpoint table read/write                [Module D]
  bootstrap.py     setup: slot + snapshot backfill            [Module D]
  replication.py   replication stream client (psycopg2)       [Module D]
  daemon.py        main loop: buffer txns, flush batches      [Module D]
tests/
  test_protocol.py                                            [Module A]
  test_aggregates.py                                          [Module B]
  test_config.py                                              [Module C]
  integration/test_end_to_end.py   (pytest -m integration)    [Module D]
demo/              docker-compose demo + kill -9 proof        [Module E]
README.md, DESIGN.md, LICENSE, CONTRIBUTING.md, .github/      [Module F]
```

Shared primitives live in `walflux/common.py` (already written — do not modify):
`WalfluxError` and subclasses `ProtocolError` / `ConfigError` / `SchemaDriftError` /
`SetupError`, the `UNCHANGED_TOAST` sentinel, `parse_lsn` / `format_lsn`, and
`quote_ident`. All modules import these from `walflux.common`; `protocol.py`
re-exports `UNCHANGED_TOAST`, `parse_lsn`, and `format_lsn` for its public API.

---

## Module A — `walflux/protocol.py`

Decodes **pgoutput protocol version 1** binary messages (the payload bytes of XLogData
frames). Reference: PostgreSQL docs "Logical Streaming Replication Protocol" and
"Logical Replication Message Formats" (verify field layouts against the official docs
for PG 15/16, protocol version 1 — not 2+; we do not enable streaming of in-progress
transactions).

### Types (all `@dataclass(frozen=True)`)

```python
UNCHANGED_TOAST: Final = object()   # sentinel, exported
# A column value in a decoded tuple: None (NULL) | UNCHANGED_TOAST | str (text format)
ColumnValue = Union[None, object, str]
TupleData = tuple[ColumnValue, ...]

@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    type_mod: int
    is_key: bool          # bit 1 of the per-column flags byte

@dataclass(frozen=True)
class Relation:
    rel_id: int
    namespace: str        # "" in the wire format means pg_catalog; pass through as decoded
    name: str
    replica_identity: str  # one of 'd' (default), 'n' (nothing), 'f' (full), 'i' (index)
    columns: tuple[Column, ...]

@dataclass(frozen=True)
class Begin:
    final_lsn: int        # LSN of the transaction's commit record
    commit_ts: datetime   # UTC
    xid: int

@dataclass(frozen=True)
class Commit:
    flags: int
    commit_lsn: int
    end_lsn: int          # first LSN after this transaction — used for checkpoints
    commit_ts: datetime

@dataclass(frozen=True)
class Insert:
    rel_id: int
    new: TupleData

@dataclass(frozen=True)
class Update:
    rel_id: int
    old: TupleData | None   # present iff 'O' submessage (REPLICA IDENTITY FULL)
    key: TupleData | None   # present iff 'K' submessage (identity-index columns only)
    new: TupleData

@dataclass(frozen=True)
class Delete:
    rel_id: int
    old: TupleData | None   # 'O' variant
    key: TupleData | None   # 'K' variant

@dataclass(frozen=True)
class Truncate:
    options: int            # bit 0: CASCADE, bit 1: RESTART IDENTITY
    rel_ids: tuple[int, ...]

@dataclass(frozen=True)
class Origin:      # decoded and ignored by the daemon
    commit_lsn: int
    name: str

@dataclass(frozen=True)
class TypeInfo:    # 'Y' message; decoded and ignored by the daemon
    type_oid: int
    namespace: str
    name: str

Message = Union[Begin, Commit, Relation, Insert, Update, Delete, Truncate, Origin, TypeInfo]
```

### Functions

```python
def decode_message(buf: bytes) -> Message
    # Dispatch on buf[0]; raise ProtocolError with the message-type byte and offset
    # context for unknown types or truncated buffers.

# parse_lsn / format_lsn / UNCHANGED_TOAST come from walflux.common; re-export them.

def convert_value(text: str | None, type_oid: int):
    # None -> None. bool (16) -> True/False from 't'/'f'. int2/int4/int8/oid
    # (21, 23, 20, 26) -> int. float4/float8 (700, 701) -> float.
    # numeric (1700) -> decimal.Decimal. Everything else -> str unchanged.
    # UNCHANGED_TOAST must be handled by callers before calling this.
```

Implementation notes:

- All integers big-endian (`struct` format `>`). Strings are NUL-terminated C strings,
  decode UTF-8.
- Timestamps are `int64` **microseconds since 2000-01-01 00:00:00 UTC**; convert to
  timezone-aware `datetime`.
- TupleData wire format: `int16` column count, then per column a category byte:
  `'n'` NULL, `'u'` unchanged TOAST, `'t'` text (`int32` length + bytes). Category
  `'b'` (binary) is protocol ≥2 territory; raise `ProtocolError` if seen.
- Update submessages: optional `'K'` or `'O'` tuple, then mandatory `'N'` tuple.
  Delete: exactly one of `'K'` or `'O'`.

Tests (`tests/test_protocol.py`): build wire bytes with `struct.pack` helpers (write a
small `encode_*` builder inside the test module — it doubles as documentation), then
assert full round-trips: every message type, NULL columns, unchanged-TOAST columns,
Update with and without 'O', Delete 'K' vs 'O', Truncate with several rel_ids, LSN
parse/format round-trip, timestamp epoch conversion, unknown type byte and truncated
buffer errors. No database needed.

---

## Module B — `walflux/aggregates.py`

Pure logic: accumulate per-transaction deltas in memory and emit SQL. **No database
connections in this module.** All aggregate arithmetic uses `decimal.Decimal`.

### Target table shape

For a view `v` with `group_by = [g1, g2]`, the target table `walflux."<v.name>"`:

| column | type | notes |
|---|---|---|
| `g1`, `g2` | source column types | resolved at setup time by Module D |
| `__walflux_rows` | `bigint not null` | live source rows in this group; group is deleted when it reaches 0 |
| per aggregate | see below | |

- `count` (no column, i.e. `COUNT(*)`): `"<alias>" bigint not null`
- `count` of a column (counts non-NULLs): `"<alias>" bigint not null`
- `sum(col)`: `"<alias>__sum" numeric`, `"<alias>__nn" bigint not null`, and a stored
  generated column `"<alias>" numeric GENERATED ALWAYS AS (CASE WHEN "<alias>__nn" > 0
  THEN "<alias>__sum" END) STORED` — because SQL `SUM` over zero non-NULL inputs is
  NULL, not 0. Same trick for `avg`:
- `avg(col)`: `"<alias>__sum" numeric`, `"<alias>__nn" bigint not null`, generated
  `"<alias>" numeric GENERATED ALWAYS AS (CASE WHEN "<alias>__nn" > 0 THEN
  "<alias>__sum" / "<alias>__nn" END) STORED`
- Unique index: `CREATE UNIQUE INDEX ... ON walflux."<v>" (g1, g2) NULLS NOT DISTINCT`
  (this is why we require PG 15+; NULL group keys must upsert into one row, matching
  `GROUP BY` semantics).

If `group_by` is empty (global aggregate), the target has exactly one row; use a
constant `__walflux_all boolean not null default true` key column with a unique index
so upserts work, and never delete the row — when `__walflux_rows` is 0 the generated
columns yield NULL, which matches `SELECT sum(x) FROM t` on an empty table returning
one row. `count` columns are set to `GREATEST(count_value, 0)`... no — counts can
simply be stored; with no rows they are 0, which also matches SQL (`COUNT` of empty
set is 0, one row returned). The `DELETE ... WHERE __walflux_rows <= 0` cleanup below
must therefore be skipped for grouped-by-nothing views.

### API

```python
@dataclass
class AggSpec:            # mirrors config; Module C constructs these
    fn: str               # 'count' | 'sum' | 'avg'
    column: str | None    # None only valid for count
    alias: str

@dataclass
class ViewSpec:
    name: str             # target table name (validated identifier)
    source_schema: str
    source_table: str
    group_by: list[str]
    aggregates: list[AggSpec]

def target_ddl(view: ViewSpec, group_col_types: dict[str, str]) -> list[str]
    # DROP TABLE IF EXISTS + CREATE TABLE + CREATE UNIQUE INDEX statements.

def backfill_sql(view: ViewSpec) -> str
    # INSERT INTO walflux."<v>" SELECT <groups>, count(*), <agg storage cols>...
    # FROM "<schema>"."<table>" GROUP BY <groups>;  populates __sum/__nn columns
    # (generated columns are never written). For group_by=[] emit the single-row
    # variant (no GROUP BY; always exactly one row via plain aggregate SELECT).

class DeltaBatch:
    # Accumulates deltas for MANY views across one flush batch.
    def __init__(self, views: list[ViewSpec]): ...
    def apply_insert(self, view_name: str, row: dict[str, object]) -> None
    def apply_delete(self, view_name: str, row: dict[str, object]) -> None
    def apply_update(self, view_name: str, old: dict, new: dict) -> None
        # equivalent to delete(old) + insert(new)
    def apply_truncate(self, view_name: str) -> None
        # discard deltas accumulated so far for that view, set truncated flag
    def is_empty(self) -> bool
    def statements(self) -> list[tuple[str, tuple]]
        # Ordered (sql, params) list for ALL touched views:
        #   1. for truncated views: DELETE FROM walflux."<v>";  (all rows)
        #   2. one upsert per touched group:
        #      INSERT INTO walflux."<v>" (<groups>, __walflux_rows, <storage cols>)
        #      VALUES (%s, ...) ON CONFLICT (<groups>) DO UPDATE SET
        #        __walflux_rows = walflux."<v>".__walflux_rows + EXCLUDED.__walflux_rows,
        #        <each storage col> = walflux."<v>".<col> + EXCLUDED.<col>
        #   3. per non-global touched view: DELETE FROM walflux."<v>" WHERE __walflux_rows <= 0
```

Delta arithmetic per row event, for each aggregate:

- `count(*)`-style alias: ±1 per insert/delete (it equals `__walflux_rows` but is kept
  as its own column so the target reads naturally).
- `count(col)`: ±1 only when `row[col] is not None`.
- `sum/avg(col)`: when value non-NULL: `__sum ± Decimal(value)`, `__nn ± 1`. NULL
  values contribute nothing.
- Values arrive already converted (`convert_value`); non-numeric types for sum/avg
  are a config-time error (Module C validates where it can; runtime `Decimal`
  conversion failure raises `WalfluxError` naming the view, column, and value type).

`UNCHANGED_TOAST` appearing in any column a view needs (group key or aggregated
column) raises `SchemaDriftError`. Note: even with REPLICA IDENTITY FULL, pgoutput
inlines TOASTed values only in the *old* tuple of an update/delete (Postgres
flattens TOAST pointers when building the old row image); the *new* tuple of an
UPDATE still carries the unchanged-TOAST marker for TOASTed columns the update did
not rewrite. The daemon therefore substitutes the old value for every
unchanged-TOAST column in the new tuple before feeding the batch (unchanged means
new == old), so a marker reaching `DeltaBatch` means a row image that should have
been complete was not — i.e. the table lacks REPLICA IDENTITY FULL. See DESIGN.md.

Tests (`tests/test_aggregates.py`): DDL text for a representative view (snapshot-style
assertions on the emitted SQL), backfill SQL, insert/delete/update delta accumulation
incl. NULL handling, avg storage arithmetic, truncate ordering (truncate then
post-truncate inserts survive), group cancellation to zero emits the cleanup DELETE,
global (no group_by) view variant, Decimal precision (0.1 + 0.2 style cases).

---

## Module C — `walflux/config.py` and `walflux/cli.py`

### Config file format (YAML)

```yaml
database:
  dsn: "postgresql://user:pass@localhost:5432/db"   # WALFLUX_DSN env var overrides
slot: walflux            # default "walflux"
publication: walflux     # default "walflux"
batch:
  max_ms: 200            # flush at least this often when txns are pending (default 200)
  max_txns: 500          # or when this many txns are buffered (default 500)
views:
  - name: orders_by_status
    source: public.orders          # schema optional, defaults to public
    group_by: [status]             # may be empty/omitted for a global aggregate
    aggregates:
      - { fn: count, as: order_count }                  # COUNT(*)
      - { fn: count, column: coupon, as: with_coupon }  # COUNT(coupon)
      - { fn: sum, column: total, as: revenue }
      - { fn: avg, column: total, as: avg_order_value }
```

`load_config(path: str) -> Config` where `Config` is a dataclass holding `dsn`,
`slot`, `publication`, `batch_max_ms`, `batch_max_txns`, `views: list[ViewSpec]`
(constructing Module B's `ViewSpec`/`AggSpec`). Validation with precise `ConfigError`
messages: unknown `fn`; `column` required for sum/avg and forbidden nowhere (allowed
for count); `as` required, a valid unquoted PG identifier (`[a-z_][a-z0-9_]*`, ≤ 63
bytes), unique within the view and not colliding with group column names or the
reserved `__walflux_rows` / `__sum` / `__nn` suffixes; view names valid identifiers,
unique; `source` must be `table` or `schema.table`; group_by columns must not repeat;
slot/publication valid identifiers. `WALFLUX_DSN` env var overrides `database.dsn`;
one of the two must be present.

### CLI (`walflux/cli.py`, argparse, entry point `main()`)

```
walflux setup    -c CONFIG [--force]   # create publication, slot, targets, backfill
walflux run      -c CONFIG             # run the daemon (foreground, logs to stderr)
walflux status   -c CONFIG             # slot / lag / checkpoint / per-view row counts
walflux teardown -c CONFIG --yes       # drop slot, publication, walflux schema
walflux --version
```

`status` prints a small aligned plain-text table: slot name & whether active,
`restart_lsn`, `confirmed_flush_lsn`, current server LSN, byte lag
(`pg_wal_lsn_diff`), checkpoint LSN from `walflux.checkpoint`, then one line per view:
target row count and last-flush freshness if cheaply available (it is not tracked
separately — omit freshness, keep it honest). Exit codes: 0 ok, 1 error, 2 usage.
Implementation calls into Modules B/D; import `bootstrap`/`daemon` lazily inside
subcommand handlers so `--help` stays fast.

Tests (`tests/test_config.py`): happy path parse, every validation error path (match
on message substrings), env-var override, defaults.

---

## Module D — replication core

### `walflux/checkpoint.py`

Schema objects (idempotent DDL executed by bootstrap):

```sql
CREATE SCHEMA IF NOT EXISTS walflux;
CREATE TABLE IF NOT EXISTS walflux.checkpoint (
  slot        text PRIMARY KEY,
  commit_lsn  bigint NOT NULL,       -- Commit.end_lsn of last applied txn
  updated_at  timestamptz NOT NULL DEFAULT now()
);
```

`read_checkpoint(conn, slot) -> int | None`, and `write_checkpoint(cur, slot, lsn)`
which executes an upsert **using the caller's open cursor/transaction** — never
commits itself. That property is the heart of exactly-once and must be documented in
the docstring.

### `walflux/bootstrap.py` — `walflux setup`

Order matters; this is the consistent-bootstrap handshake:

1. Regular connection: validate sources exist; check/require `wal_level=logical`;
   create `walflux` schema + checkpoint table; create publication if missing
   (`CREATE PUBLICATION <pub> FOR TABLE <sources>`) or `ALTER PUBLICATION ... SET TABLE`
   to match config; set `REPLICA IDENTITY FULL` on each source table that doesn't have
   it (log a WARNING explaining the WAL-volume tradeoff).
2. Replication connection (psycopg2 `LogicalReplicationConnection`): execute
   `CREATE_REPLICATION_SLOT "<slot>" LOGICAL pgoutput EXPORT_SNAPSHOT` and read the
   result row: `(slot_name, consistent_point, snapshot_name, output_plugin)`.
   If the slot exists: with `--force`, drop and recreate (and re-run backfill);
   without, fail with a message pointing at `--force` and `teardown`.
   **Keep this connection open and idle until step 3 commits** — the exported
   snapshot lives only that long.
3. Regular connection: `BEGIN ISOLATION LEVEL REPEATABLE READ`;
   `SET TRANSACTION SNAPSHOT '<snapshot_name>'`; resolve group-by column types from
   `pg_catalog` via `format_type(a.atttypid, a.atttypmod)` (NOT
   `information_schema.columns.data_type`, which reads `'USER-DEFINED'` for enums
   and `'ARRAY'` for arrays — not valid DDL); run `target_ddl` + `backfill_sql` for
   every view; `write_checkpoint(cur, slot, parse_lsn(consistent_point))`; `COMMIT`.
   Close the replication connection.

Step 1 must also DELETE any existing `walflux.checkpoint` row for the slot in the
same autocommit step that drops (or precedes creating) the slot: a crash after the
new slot exists but before step 3 commits must leave "no checkpoint" — which
`walflux run` refuses to start from — never a stale checkpoint that would pair with
the new slot and silently skip every transaction committed between the old
checkpoint and the new consistent point.

Every transaction that committed before `consistent_point` is in the snapshot (and
thus the backfill); every one after it will be delivered by the stream and applied
exactly once by the daemon's skip rule. State this invariant in a comment.

DDL of targets uses `DROP TABLE IF EXISTS` — setup is destructive to targets by
design (they are derived data); say so in `--help`.

### `walflux/replication.py`

Thin wrapper over psycopg2's replication support (verify exact API against psycopg2
docs — `psycopg2.extras.LogicalReplicationConnection`, `ReplicationCursor.
start_replication(slot_name=..., decode=False, options={'proto_version': '1',
'publication_names': '<pub>'})`, `read_message()`, `send_feedback(flush_lsn=...)`).

```python
class ReplicationStream:
    def __init__(self, dsn: str, slot: str, publication: str): ...
    def messages(self) -> Iterator[tuple[int, bytes]]:
        # yields (wal_end, payload) for each XLogData; uses select() with a ~1s
        # timeout so the loop can wake to send periodic status/feedback and notice
        # shutdown; sends keepalive feedback every ~10s regardless of progress.
    def ack(self, flush_lsn: int) -> None:
        # send_feedback(flush_lsn=...); called ONLY after the target txn containing
        # the matching checkpoint has committed.
    def close(self) -> None
```

### `walflux/daemon.py`

```python
class Daemon:
    def __init__(self, config: Config): ...
    def run(self) -> None   # blocks; SIGINT/SIGTERM => graceful stop
```

Loop semantics:

- Maintain `relations: dict[rel_id, Relation]` from Relation messages, and a mapping
  `(namespace, name) -> list[ViewSpec]`. Relation messages re-arrive whenever a
  table's schema changes: if a needed column (group key or aggregated) is missing,
  raise `SchemaDriftError` and exit non-zero with a clear remediation message
  (re-run `walflux setup --force`). Extra/unknown columns are fine.
- Buffer per transaction: `Begin` opens a txn buffer; row messages append; `Commit`
  seals it. **Skip rule:** if `Commit.end_lsn <= checkpoint_lsn`, discard the txn
  (it was applied before a crash) — this plus the atomic flush is exactly-once.
- Row decoding: zip TupleData with `Relation.columns`, `convert_value` each, build a
  dict. Updates use `old` (REPLICA IDENTITY FULL guarantees it); if `old is None`,
  raise `SchemaDriftError` mentioning replica identity. In an Update's *new* dict,
  replace every `UNCHANGED_TOAST` value with the old dict's value for that column
  (pgoutput elides unchanged TOASTed values from new tuples regardless of replica
  identity; the old image is complete under REPLICA IDENTITY FULL).
- A `Relation` message whose `(namespace, name)` matches no configured source, for a
  `rel_id` that previously resolved to one or more views, means the table was renamed
  or moved to another schema (publication membership follows the OID): raise
  `SchemaDriftError` naming old and new — never silently freeze the views.
- Sealed txns feed a `DeltaBatch`. Flush when `batch.max_txns` sealed txns are
  buffered OR `batch.max_ms` has elapsed since the oldest unflushed sealed txn OR the
  stream is idle and there is anything pending. Flush = one target transaction:
  `DeltaBatch.statements()` + `write_checkpoint(cur, slot, last_end_lsn)` + commit;
  then `stream.ack(last_end_lsn)`; then log at INFO: txn count, groups touched,
  end LSN, flush latency ms.
- Consistency rule: a flush contains only whole source transactions, in commit order.
- On any error: log, close cleanly, exit non-zero. Crash-safety comes from the
  checkpoint, not from error handling cleverness. systemd/compose restart policy is
  the retry story (README covers it).

Also provide `status(config) -> dict` here (used by `walflux status`): queries
`pg_replication_slots`, `pg_current_wal_lsn()`, `pg_wal_lsn_diff`, checkpoint row,
per-view counts.

### `tests/integration/test_end_to_end.py` (marked `integration`)

Needs `WALFLUX_TEST_DSN` pointing at a superuser-ish Postgres 15+ with
`wal_level=logical` (CI provides it; skip with a clear reason otherwise). Flow:
create a scratch schema + `orders` table; write a temp config with two views (grouped
+ global); run `setup` programmatically; apply a mixed workload (inserts, updates
incl. group-moving updates, deletes, one TRUNCATE, NULL values); run the daemon in a
**subprocess** until caught up (poll checkpoint vs `pg_current_wal_lsn`); `SIGKILL`
the daemon mid-second-workload; restart it; wait for catch-up; assert target tables
byte-equal the ground-truth `GROUP BY` queries (compare via ORDER BY'd fetches, using
`Decimal` comparisons); assert no group row with `__walflux_rows <= 0` except the
global view's single row. This test IS the kill -9 proof.

---

## Module E — `demo/`

- `demo/docker-compose.yml`: `postgres` (image `postgres:16`, `command: postgres -c
  wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8`, healthcheck
  `pg_isready`, seeds via `/docker-entrypoint-initdb.d` mount of `seed.sql`);
  `walflux` (build from repo-root `Dockerfile`, runs `walflux run -c /demo/config.yaml`,
  `restart: "no"` — the kill demo restarts it explicitly); `generator` (same image,
  runs `python /demo/generate.py`). Compose project name `walflux-demo`.
- `Dockerfile` (repo root): `python:3.12-slim`, two stages — `runtime` (package
  only; published to ghcr.io by the release workflow) and `demo` (adds `demo/`;
  the compose build target).
- `demo/seed.sql`: `orders(id bigserial PK, status text NOT NULL, total numeric(10,2),
  coupon text, created_at timestamptz DEFAULT now())` + ~50 seed rows across statuses.
- `demo/config.yaml`: the two views from the config example, DSN pointing at the
  compose `postgres` service.
- `demo/generate.py`: steady mixed workload (~30 writes/sec: 70% insert, 15% update
  that often moves rows between statuses, 15% delete; occasional NULL totals/coupons),
  prints a heartbeat line every 5s. Pure psycopg2, no other deps.
- `demo/verify.py`: connects, waits until `walflux.checkpoint` catches up to
  `pg_current_wal_lsn()` (with generator paused — see script), then compares
  ground-truth `GROUP BY` queries against both target tables and prints a
  side-by-side diff table ending in a loud `PASS`/`FAIL` line; exit code accordingly.
- `demo/kill9.sh` (bash, `set -euo pipefail`, run from repo root): brings up postgres,
  runs `walflux setup` one-off, starts walflux + generator, lets it run ~20s, prints
  a status snapshot, then `docker compose kill -s SIGKILL walflux`, lets the
  generator keep writing ~10s (daemon dead, WAL accumulating), restarts walflux,
  stops the generator, runs `verify.py` in the walflux container, prints an epilogue
  explaining WHY it passed (two sentences: atomic checkpoint+apply; skip rule for
  redelivered txns). Idempotent: `docker compose down -v` first.
- `Makefile` (repo root): `make demo` (up + follow walflux logs), `make kill9`
  (runs demo/kill9.sh), `make verify`, `make down`, `make test`, `make lint`.

## Module F — README.md, DESIGN.md, LICENSE, CONTRIBUTING.md, CI

README requirements (discoverability is a feature; write for a skimming reader):

- H1 `WalFlux` + one-line tagline: millisecond-fresh materialized views for Postgres —
  no extensions, no triggers, no second database. Badges: CI, MIT (the shields.io
  PyPI + Python-versions badges return once the first release is actually on PyPI —
  broken badges are worse than none).
- "Why": `REFRESH MATERIALIZED VIEW` recomputes everything and locks; trigger-based
  counters add write-path latency and deadlock risk; stream processors are a second
  system to operate. WalFlux is one daemon + your existing Postgres. Works where you
  can't install extensions (RDS, Cloud SQL, Supabase, Neon) because pgoutput is
  built in — this paragraph should naturally contain the phrases people search:
  "incremental view maintenance", "postgres materialized view auto refresh",
  "change data capture", "logical replication", "pgoutput".
- 60-second Quickstart: the compose demo, then the `kill -9` proof (`make kill9`)
  with expected PASS output excerpt.
- Real-setup section: pip install, config file, `walflux setup` / `run` / `status`.
- "How it works" mermaid sequence diagram (slot → decode → delta → atomic
  apply+checkpoint → ack) + a short exactly-once explanation linking DESIGN.md.
- Honest **Limitations** table: single-table views (no joins yet); count/sum/avg only
  (no min/max — deletable-aggregate problem, link DESIGN.md); REPLICA IDENTITY FULL
  required (WAL volume cost); PG 15+; targets live in the same database; schema
  changes on source tables require re-setup. Each with the one-line reason.
- Comparison table: WalFlux vs REFRESH MATERIALIZED VIEW vs triggers vs pg_ivm vs
  Materialize vs Debezium+Flink (rows: freshness, works on managed PG, operational
  footprint, joins support — be generous to the alternatives; pg_ivm is great if you
  can install extensions).
- Roadmap (delta joins, min/max via auxiliary heaps, snapshot re-sync without full
  backfill, Prometheus metrics endpoint). FAQ (5-6 real questions: slot bloat when
  daemon is down + `max_slot_wal_keep_size`; can targets live in another database —
  not yet, why; what happens on schema change; how big can batches get; why psycopg2
  not psycopg3 — verify the current state of psycopg3 replication-protocol support
  via its docs/changelog and state it accurately with a link).
- GitHub repo metadata suggestions belong in CONTRIBUTING or the PR description, not
  the README.

DESIGN.md: the full correctness argument. Enumerate every crash point in the
apply/ack sequence and show why each is safe (crash before flush → redelivery,
skip rule; crash between commit and ack → slot re-sends, skip rule discards; crash
mid-backfill → setup is re-runnable with --force). Explain why feedback/ack uses the
checkpointed LSN only. Cover: batching vs latency; the SUM-of-empty-set-is-NULL
storage scheme; NULL group keys and NULLS NOT DISTINCT; TRUNCATE ordering; TOAST and
REPLICA IDENTITY FULL; slot-bloat operational guidance; schema-drift halt rationale
(halt loudly beats silently-wrong aggregates); why protocol v1 (no streamed
in-progress txns) is the right v0 choice.

CI (`.github/workflows/ci.yml`): on push/PR. Jobs: `lint` (ruff check via uv);
`unit` (Python 3.10 + 3.13 matrix, `uv run --extra dev pytest -m "not integration"`);
`integration` — start Postgres manually (service containers can't override command):
`docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
-c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8`, wait for
pg_isready in a retry loop, then `WALFLUX_TEST_DSN=postgresql://postgres:postgres@localhost:5432/postgres
uv run --extra dev pytest -m integration -v`.

LICENSE: MIT, "Copyright (c) 2026 WalFlux contributors". CONTRIBUTING.md: dev setup
with uv, test matrix, how to run the integration test locally via docker, style
(ruff), spec pointer to this file.

---

## Style rules (all modules)

- Full type hints; `from __future__ import annotations` at top of every module.
- Docstrings on public functions; comments only for invariants code can't express
  (the skip rule, the snapshot handshake, feedback ordering).
- stdlib logging (`logging.getLogger("walflux.<module>")`); daemon configures a
  stderr handler with `%(asctime)s %(levelname)s %(name)s %(message)s`.
- SQL identifiers always double-quoted via `walflux.common.quote_ident()` (validate
  then quote); values always parameterized (`%s`) — never interpolated.
- No dependencies beyond psycopg2-binary + PyYAML at runtime; pytest + ruff for dev.
- Keep modules importable without a database (psycopg2 imports live inside
  `replication.py` / `bootstrap.py` / `checkpoint.py` / `daemon.py` only).
