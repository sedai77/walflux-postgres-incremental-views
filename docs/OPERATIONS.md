# Operating WalFlux

The operator's page: what to watch, how to upgrade, how to walk away cleanly.
The one failure mode that matters is **slot bloat** — a replication slot nobody
consumes pins WAL and fills the disk
([DESIGN.md §9](../DESIGN.md#9-slot-bloat-the-operational-contract) has the
reasoning; this page has the copy-paste).

## Run it under a supervisor

`walflux run` is a foreground process; restarts are the retry story and are
safe by construction. Use [`deploy/walflux.service`](../deploy/walflux.service)
(systemd, `Restart=on-failure`) or
[`deploy/docker-compose.yml`](../deploy/docker-compose.yml)
(`restart: unless-stopped`).

## Monitoring

Everything worth alerting on lives in `pg_replication_slots`:

```sql
SELECT slot_name, active, wal_status, safe_wal_size,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'walflux';
```

Alert on:

| Condition | Meaning |
|---|---|
| `active = false` for > 5 minutes | The daemon is down or cannot connect; WAL is piling up. |
| `wal_status IN ('unreserved', 'lost')` | The slot is at (or past) the point of invalidation. |
| `lag_bytes` above a byte budget you choose (e.g. 1 GB) | The daemon runs but is not keeping up. |

And bound the blast radius with `max_slot_wal_keep_size` (e.g. `10GB`): if the
daemon stays down past it, Postgres invalidates the slot instead of filling the
disk, and recovery is `walflux setup --force` (a re-backfill, not an outage).

As a [postgres_exporter](https://github.com/prometheus-community/postgres_exporter)
custom query:

```yaml
walflux_slot:
  query: |
    SELECT slot_name, active::int,
           (wal_status IN ('unreserved', 'lost'))::int AS endangered,
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
    FROM pg_replication_slots WHERE slot_name = 'walflux'
  metrics:
    - slot_name: { usage: LABEL }
    - active: { usage: GAUGE, description: "1 while the walflux daemon is connected" }
    - endangered: { usage: GAUGE, description: "1 when wal_status is unreserved or lost" }
    - lag_bytes: { usage: GAUGE, description: "bytes of WAL not yet confirmed by walflux" }
```

As a cron check (prints the problem and exits non-zero when unhealthy):

```bash
psql "$DSN" -Atc "SELECT format('walflux slot: active=%s wal_status=%s', active, wal_status)
  FROM pg_replication_slots
  WHERE slot_name = 'walflux' AND (NOT active OR wal_status IN ('unreserved', 'lost'))" \
  | { ! grep .; }
```

For humans, `walflux status -c walflux.yaml` prints the same numbers plus the
checkpoint LSN and per-view row counts. A Prometheus metrics endpoint in the
daemon itself is a [roadmap item](../README.md#roadmap).

## Upgrades

An upgrade is a restart: install the new version (`pip install -U ...`, or pull
the new image) and restart the daemon. The checkpoint in `walflux.checkpoint`
carries across restarts, so the stream resumes exactly where it left off.

Compatibility policy for 0.x: the on-database layout (the `walflux.checkpoint`
table and the target-table shape) stays compatible with in-place upgrades
within 0.x. If a release ever needs `walflux setup --force` (a re-backfill),
its [CHANGELOG](../CHANGELOG.md) entry says so explicitly — read it before
upgrading.

## Decommissioning

Order matters: stopping the daemon does **not** remove the slot, and an
abandoned slot is a disk-filling machine.

1. Stop the supervisor so nothing restarts the daemon:
   `systemctl disable --now walflux`, or `docker compose down`.
2. Immediately drop the replication state:
   `walflux teardown -c walflux.yaml --yes` — removes the slot, the
   publication, and the `walflux` schema (targets included; they are derived
   data).

Two loose ends:

- **Forgot step 2 and the disk is filling?** Run the teardown now — dropping
  the slot releases the pinned WAL. If the slot was already invalidated by
  `max_slot_wal_keep_size`, it no longer pins WAL; teardown still cleans it up.
- **Replica identity.** Setup put `REPLICA IDENTITY FULL` on the source tables
  and teardown deliberately leaves it (it is your table setting now). To undo:
  `ALTER TABLE ... REPLICA IDENTITY DEFAULT`.
