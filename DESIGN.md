# WalFlux Design: The Correctness Argument

This document argues, crash point by crash point, that WalFlux applies every source
transaction to its aggregate targets **exactly once** — no lost updates, no double
counting — using nothing but a logical replication slot, one checkpoint row, and
transaction atomicity in the target database. It is written for the reader who does
not want to take that on faith.

The interfaces referenced here are specified in [`docs/SPEC.md`](docs/SPEC.md).

## 1. The claim, and the two primitives it rests on

Postgres logical replication gives you **at-least-once** delivery: a slot re-sends
anything after its `confirmed_flush_lsn`, and that pointer only advances when the
client explicitly acknowledges. At-least-once becomes exactly-once if — and only if —
the client can *deterministically discard duplicates*. WalFlux gets that from two
primitives:

1. **Atomic apply+checkpoint.** Every flush is a single transaction on the target
   database: the aggregate deltas for a batch of source transactions, plus an upsert
   of `walflux.checkpoint.commit_lsn` to the `end_lsn` of the last source transaction
   in the batch. Either both are durable or neither is. There is no state in which the
   deltas exist without the checkpoint recording them, or vice versa.

2. **The skip rule.** On the streaming side, when a transaction's `Commit` arrives,
   the daemon compares its `end_lsn` to the checkpoint read at startup (and maintained
   in memory since): if `end_lsn <= checkpoint_lsn`, the transaction was already
   applied before some crash, and it is discarded without touching the targets.
   `end_lsn` values are strictly increasing in commit order, so this comparison is a
   complete and unambiguous "have I applied this?" test.

Everything else in this document is showing that no crash, at any instant, can break
the correspondence between the checkpoint and the targets, or cause the slot to
forget a transaction the checkpoint has not recorded.

## 2. The steady-state sequence

Per flush cycle, the daemon executes these steps in order:

```
S1. Receive XLogData payloads; decode; buffer rows per in-flight transaction.
S2. On Commit: apply the skip rule; discard or seal the transaction.
S3. Fold sealed transactions into a DeltaBatch (pure in-memory arithmetic).
S4. BEGIN on the target connection.
S5. Execute DeltaBatch.statements()  (truncate deletes, group upserts, zero-row cleanup).
S6. write_checkpoint(cur, slot, last_end_lsn)   -- same transaction, same cursor.
S7. COMMIT.
S8. stream.ack(last_end_lsn)   -- send_feedback(flush_lsn=...); the slot may now
                                  advance confirmed_flush_lsn and recycle WAL.
```

### Crash-point enumeration

| Crash at | Target state | Slot state | Why it is safe |
|---|---|---|---|
| During S1–S3 | Unchanged | Unacked | All buffered state was memory-only. On restart the slot re-sends everything after `confirmed_flush_lsn`; already-applied transactions fail the skip rule and are discarded; the rest are re-buffered and applied. Nothing was lost, nothing applied twice. |
| During S4–S6 (target txn open) | Open transaction **rolls back** when the connection dies | Unacked | Deltas and checkpoint vanish *together* — that is the whole point of putting them in one transaction. Indistinguishable from crashing during S1–S3. |
| S7 sent, result never seen (in-doubt commit) | Either committed or not | Unacked | If it did not commit: same as the previous row. If it did: the checkpoint committed with the deltas, so on restart every redelivered transaction in the batch has `end_lsn <= checkpoint_lsn` and is discarded. The in-doubt commit — the classic hard case for two-system pipelines — costs WalFlux nothing, because the daemon never needs to *know* whether S7 succeeded; the checkpoint answers on its behalf after restart. |
| Between S7 and S8 | Committed | Unacked | The slot's `confirmed_flush_lsn` is stale, so the server redelivers the batch's transactions on restart. All of them fail the skip rule. This window is why the checkpoint lives in the target database rather than relying on the slot position: the slot may lag the truth, and lagging is harmless. |
| After S8 | Committed | Acked to `last_end_lsn` | Restart streams from the acknowledged position. Steady state. |

There is no fifth place to crash. Applying without checkpointing (double-apply risk)
and checkpointing without applying (lost-update risk) are both impossible by
transaction atomicity; the remaining failure mode — the slot discarding WAL the
checkpoint has not recorded — is prevented by the ack discipline below.

### Why `ack` sends only the checkpointed LSN

`send_feedback(flush_lsn=X)` is a promise: "everything up to X is durable on my side;
you may recycle that WAL and never send it again." It is irrevocable. If the daemon
acked on *receipt* (or on any position beyond the committed checkpoint) and then
crashed before S7, the slot would skip past transactions that were never applied —
silent, permanent data loss, the one failure the design must exclude.

So the rule is asymmetric on purpose: **the checkpoint may run ahead of the ack, the
ack may never run ahead of the checkpoint.** A stale ack merely causes redelivery,
which the skip rule absorbs for free. `ack(lsn)` is therefore called exactly once per
flush, after `COMMIT` returns, with exactly the LSN that transaction wrote into
`walflux.checkpoint`.

## 3. Bootstrap: the consistent snapshot handshake

Backfilling a target while writes continue is the other classic way to lose or double
count rows: any transaction that commits *while* the backfill runs is in danger of
being counted by both the backfill and the stream, or by neither. WalFlux uses the
slot-creation snapshot to make the boundary exact:

```
B1. Regular connection: schema, checkpoint table, publication, REPLICA IDENTITY FULL.
B2. Replication connection: CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT
    -> (slot_name, consistent_point, snapshot_name, ...).
    This connection is held open, idle, until B3 commits — the exported snapshot
    only exists while the creating transaction is alive.
B3. Regular connection: BEGIN ISOLATION LEVEL REPEATABLE READ;
    SET TRANSACTION SNAPSHOT '<snapshot_name>';
    create targets + run backfill SELECTs + write_checkpoint(consistent_point);
    COMMIT.
```

The invariant this buys: **every transaction that committed before `consistent_point`
is visible to the backfill; every transaction that commits after it will be delivered
by the slot.** The two sets partition the write history — no gap, no overlap. Writing
`consistent_point` as the initial checkpoint (inside the same transaction as the
backfill itself) arms the skip rule from the first streamed message.

Bootstrap crash points:

- **Crash in B1** — everything in B1 is idempotent (`CREATE ... IF NOT EXISTS`,
  `ALTER PUBLICATION ... SET TABLE`). Re-run `walflux setup`.
- **Crash after B2, before B3 commits** — the slot exists but the backfill
  transaction rolled back: targets and checkpoint are untouched (or still reflect a
  previous successful setup). Re-running `setup` refuses to reuse the half-born slot
  and points at `--force`, which drops the slot, recreates it (obtaining a *new*
  snapshot and consistent point), and re-runs the backfill. Setup is destructive to
  targets by design — they are derived data, so "recompute from scratch" is always a
  legal recovery.
- **Crash after B3 commits** — setup is complete; the daemon starts from the
  checkpoint. The dangling replication connection (if the process died before closing
  it) is torn down by the server; the slot itself is durable.

## 4. Batching: throughput without giving up the invariant

Applying one target transaction per source transaction would be correct but
punishing: write amplification on the target, checkpoint churn, and fsync latency on
every source commit. WalFlux batches instead — up to `batch.max_txns` source
transactions or `batch.max_ms` milliseconds (defaults 500 / 200ms), whichever comes
first, folded into one `DeltaBatch` and one target transaction.

Batching also *collapses* work: a thousand inserts into the same group become a single
upsert row in the flush, because the deltas sum in memory first. Hot groups cost one
target row-write per flush regardless of source write volume.

Two things batching deliberately does **not** relax:

- **Whole transactions only.** A flush contains complete source transactions, in
  commit order. The targets therefore always equal the ground-truth `GROUP BY` as of
  some exact transaction boundary — a consistent prefix of history, never a state
  that no serial execution could produce.
- **One checkpoint per flush.** The checkpoint is the `end_lsn` of the *last*
  transaction in the batch; the skip rule discards whole transactions, so batch
  boundaries after a crash need not (and do not) match the pre-crash ones.

The price is freshness bounded by `batch.max_ms` — which is the product's promise
("millisecond-fresh"), not a compromise of it.

## 5. Aggregate storage: the empty-set problem

SQL's `SUM` over zero non-NULL inputs is `NULL`, not `0` — and `AVG` divides by the
count of non-NULL inputs, not the row count. A naive `sum` column cannot express the
difference between "the values summed to zero" and "there are no values left" after
increments and decrements cancel out.

So WalFlux never stores the user-visible aggregate directly. For each `sum`/`avg` it
stores two plain columns — `"<alias>__sum" numeric` and `"<alias>__nn" bigint`
(the count of non-NULL contributions) — and exposes the alias as a **stored generated
column**:

```sql
"<alias>" numeric GENERATED ALWAYS AS
  (CASE WHEN "<alias>__nn" > 0 THEN "<alias>__sum" END) STORED          -- sum
  (CASE WHEN "<alias>__nn" > 0 THEN "<alias>__sum" / "<alias>__nn" END) -- avg
```

Deltas only ever touch `__sum`/`__nn` (generated columns are not writable); Postgres
recomputes the visible value on every write. When the last non-NULL value leaves a
group, `__nn` hits 0 and the alias correctly reads `NULL`. All delta arithmetic is
`decimal.Decimal` end to end — WAL text values are parsed to `Decimal`, never
`float`, so `0.1 + 0.2` style drift cannot creep into `numeric` targets.

Row lifecycle: `__walflux_rows` counts live source rows per group. A grouped view
deletes a group's row when it reaches 0 (matching `GROUP BY`, which emits no row for
an empty group). A **global** view (empty `group_by`) instead keeps its single row
forever — `SELECT sum(x) FROM t` on an empty table returns one row of `NULL`, and
`COUNT` returns one row of `0`, which is exactly what the kept row with `__nn = 0`
and count `0` reads as. The `DELETE ... WHERE __walflux_rows <= 0` cleanup is
skipped for global views for precisely this reason.

## 6. NULL group keys and `NULLS NOT DISTINCT`

`GROUP BY status` puts every `NULL` status in **one** group. But a standard unique
index treats NULLs as distinct, so `ON CONFLICT` upserts on `(status)` would insert a
fresh row for every NULL-keyed delta — fragmenting the NULL group and corrupting the
counts. WalFlux creates its target indexes as:

```sql
CREATE UNIQUE INDEX ... ON walflux."<view>" (g1, g2) NULLS NOT DISTINCT
```

which makes the NULL group a single upsertable row, matching `GROUP BY` semantics
exactly. `NULLS NOT DISTINCT` arrived in Postgres 15 — this one clause is why
WalFlux requires PG 15+ rather than emulating the behavior with COALESCE sentinels
(which would collide with real values) or expression indexes (which `ON CONFLICT`
inference handles poorly).

## 7. TRUNCATE ordering

`TRUNCATE` arrives in the stream as its own message, transactional like everything
else. Within a `DeltaBatch`, correctness requires replaying batch-internal order:
deltas accumulated *before* the truncate are annihilated by it, deltas from
transactions *after* it must survive. `apply_truncate` therefore discards the view's
accumulated deltas and sets a flag; `statements()` emits, in order:

1. `DELETE FROM walflux."<view>"` (all rows) for truncated views,
2. the group upserts — which at this point contain only post-truncate deltas,
3. the zero-row cleanup.

The result is identical to applying every event sequentially, while still being one
batch, one transaction, one checkpoint.

## 8. TOAST and `REPLICA IDENTITY FULL`

To decrement a group on `DELETE` — or move a row between groups on `UPDATE` — the
daemon must know the row's *old* values: which group it was in, and what value it
contributed to each aggregate. Postgres's default replica identity logs only the
primary key of the old row, which is not enough. `walflux setup` therefore sets
`REPLICA IDENTITY FULL` on each source table (with a logged warning: full old tuples
increase WAL volume for update/delete-heavy tables — that is the honest cost of this
architecture).

With `REPLICA IDENTITY FULL`, updates and deletes carry complete *old* tuples —
Postgres flattens TOAST pointers when it builds the old row image, so TOASTed values
arrive there in full. The *new* tuple of an update is different: pgoutput elides any
TOASTed value the update did not rewrite, sending an "unchanged TOAST" marker
(`'u'`) instead, *regardless of replica identity*. Unchanged means the new value
equals the old one, and the old image is complete, so the daemon substitutes the old
value before computing deltas. If the marker ever appears where the image should
have been complete — an old/delete tuple — WalFlux raises `SchemaDriftError` and
halts rather than guess — a defensive invariant, since subtracting a placeholder
from an aggregate would corrupt it silently. Halting loudly is the design's answer to
every "the data isn't what I assumed" situation (see §10).

## 9. Slot bloat: the operational contract

A replication slot pins WAL: while the daemon is down, the server retains every WAL
segment the slot has not confirmed, and disk usage grows without bound by default.
This is not a WalFlux quirk — it is how slots work — but WalFlux makes you opt into
the risk knowingly:

- **Bound the damage** with `max_slot_wal_keep_size` (e.g. `10GB`). If the daemon
  stays down past the bound, Postgres invalidates the slot instead of filling the
  disk; recovery is `walflux setup --force` (full re-backfill). That trade —
  re-backfill instead of an outage — is usually right, and it is yours to make.
- **Monitor** `pg_replication_slots.wal_status` / `safe_wal_size` and the byte lag
  that `walflux status` prints (`pg_wal_lsn_diff` between the server position and
  `confirmed_flush_lsn`). Alert on sustained growth.
- **Tear down what you stop using.** An abandoned slot is a disk-filling machine;
  `walflux teardown` exists so decommissioning is one command.

Run the daemon under a supervisor (systemd `Restart=on-failure`, compose
`restart: unless-stopped`). Crash-safety is the checkpoint's job; *availability* is
the supervisor's.

## 10. Schema drift: halt loudly

pgoutput re-sends a `Relation` message whenever a table's structure changes. If a
column a view depends on (a group key or an aggregated column) disappears or is
renamed, the daemon raises `SchemaDriftError` and exits non-zero with the remediation
in the message (`walflux setup --force`). Additive changes — new columns the views do
not reference — are ignored and flow through without interruption.

The alternative — limping on, skipping rows, or treating a missing column as NULL —
produces aggregates that are *plausibly wrong*: they keep updating, they look alive,
and they are quietly diverging from the truth. For derived data whose entire value is
being trustworthy, a hard stop with a clear message beats availability every time.
The same philosophy backs the `UNCHANGED_TOAST` halt (§8) and the `Decimal`
conversion errors: every path where correctness cannot be guaranteed is a loud exit,
never a best effort.

## 11. Why pgoutput protocol version 1

Protocol version 2 adds streaming of large *in-progress* transactions: the server may
ship a transaction's changes before it commits, interleaved with other transactions,
with the possibility of a later abort. Supporting that means handling stream-abort
(un-buffering work), out-of-order completion, and partial-transaction state — and the
skip rule's clean premise ("complete transactions arrive in commit order, each sealed
by a Commit with a monotone `end_lsn`") no longer holds structurally; it must be
re-established with substantially more bookkeeping.

Version 1 delivers exactly the shape the correctness argument needs: whole, committed
transactions, in commit order, nothing speculative. The cost is that a very large
source transaction is buffered in daemon memory until its commit arrives, and adds
its full decode time to latency. For v0, that is the right trade; revisiting it
belongs alongside the other roadmap work, not before the simple thing is proven.

## 12. Why only `count` / `sum` / `avg` (no `min`/`max`)

`count`, `sum`, and `avg` (as sum + count) are *self-maintainable*: an insert or
delete updates them with the delta of that one row, no other state needed. `MIN` and
`MAX` are not — this is the **deletable-aggregate problem**. When the current maximum
is deleted, the new maximum is the runner-up, and the runner-up is not in the
aggregate: answering requires the full multiset of values (or an auxiliary structure
such as a per-group heap or a sorted secondary table). Any "incremental" min/max
without that state is wrong on the first delete of an extreme value.

Rather than ship a min/max that is subtly broken under deletes — the failure mode
this entire document exists to avoid — WalFlux omits them. Auxiliary per-group heaps
are on the [roadmap](README.md#roadmap); they change the storage story enough to
deserve their own design pass.
