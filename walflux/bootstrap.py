"""``walflux setup``: the consistent-bootstrap handshake.

Order matters: create the slot with an exported snapshot, then backfill the
target tables *inside that snapshot* and record the slot's consistent point as
the checkpoint — all before the first streamed transaction is applied.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from walflux.aggregates import ViewSpec, backfill_sql, target_ddl
from walflux.checkpoint import ensure_checkpoint_table, write_checkpoint
from walflux.common import SetupError, parse_lsn, quote_ident

if TYPE_CHECKING:
    from psycopg2.extensions import cursor as Cursor

    from walflux.config import Config

logger = logging.getLogger("walflux.bootstrap")


def setup(config: Config, *, force: bool = False) -> None:
    """Create publication, slot, target tables, and a snapshot-consistent backfill.

    Destructive to target tables by design (``DROP TABLE IF EXISTS``): targets
    are derived data and are rebuilt from the slot's exported snapshot.
    """
    conn = psycopg2.connect(config.dsn)
    conn.autocommit = True
    repl_conn = None
    try:
        with conn.cursor() as cur:
            _check_wal_level(cur)
            _ensure_sources(cur, config.views)
            ensure_checkpoint_table(cur)
            _ensure_publication(cur, config)
            _prepare_slot(cur, config.slot, force=force)
            # Any pre-existing checkpoint row is now meaningless: the slot it
            # belonged to is gone (or never existed). Delete it in this
            # autocommit step, BEFORE the new slot exists, so a crash anywhere
            # short of step 3's commit leaves "no checkpoint" — which
            # `walflux run` refuses to start from — rather than a stale
            # position that would silently skip every transaction committed
            # between it and the new slot's consistent point.
            cur.execute("DELETE FROM walflux.checkpoint WHERE slot = %s", (config.slot,))

        # Step 2 — replication connection: create the slot and export its
        # snapshot. The exported snapshot stays usable only while this
        # connection stays open and idle, so it must not be touched (or
        # closed) until the backfill transaction below has committed.
        repl_conn = psycopg2.connect(config.dsn, connection_factory=LogicalReplicationConnection)
        repl_cur = repl_conn.cursor()
        repl_cur.execute(
            f"CREATE_REPLICATION_SLOT {quote_ident(config.slot)} LOGICAL pgoutput EXPORT_SNAPSHOT"
        )
        row = repl_cur.fetchone()
        if row is None:
            raise SetupError("CREATE_REPLICATION_SLOT returned no result row")
        consistent_point, snapshot_name = row[1], row[2]
        logger.info(
            "created slot %r at consistent point %s (snapshot %s)",
            config.slot,
            consistent_point,
            snapshot_name,
        )

        # Step 3 — regular connection, pinned to the exported snapshot.
        #
        # Invariant: every transaction that committed before consistent_point
        # is visible in this snapshot and therefore included in the backfill;
        # every transaction after it will be delivered by the stream and
        # applied exactly once by the daemon's skip rule
        # (Commit.end_lsn <= checkpoint => discard).
        with conn.cursor() as cur:
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
            for view in config.views:
                columns = _source_columns(cur, view)
                missing = sorted(_required_columns(view) - columns.keys())
                if missing:
                    raise SetupError(
                        f"source table {view.source_schema}.{view.source_table} is missing "
                        f"column(s) {', '.join(missing)} needed by view {view.name!r}"
                    )
                group_types = {name: columns[name] for name in view.group_by}
                for statement in target_ddl(view, group_types):
                    cur.execute(statement)
                cur.execute(backfill_sql(view))
                logger.info(
                    "backfilled walflux.%s from %s.%s",
                    view.name,
                    view.source_schema,
                    view.source_table,
                )
            write_checkpoint(cur, config.slot, parse_lsn(consistent_point))
            cur.execute("COMMIT")
        logger.info("setup complete; checkpoint at %s", consistent_point)
    finally:
        if repl_conn is not None:
            repl_conn.close()
        conn.close()


def teardown(config: Config) -> None:
    """Drop the slot, the publication, and the whole ``walflux`` schema."""
    conn = psycopg2.connect(config.dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                (config.slot,),
            )
            row = cur.fetchone()
            if row is not None:
                if row[0] is not None:
                    cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
                _drop_slot_with_retry(cur, config.slot)
            cur.execute(f"DROP PUBLICATION IF EXISTS {quote_ident(config.publication)}")
            cur.execute("DROP SCHEMA IF EXISTS walflux CASCADE")
        logger.info("teardown complete: slot, publication, and walflux schema removed")
    finally:
        conn.close()


def _check_wal_level(cur: Cursor) -> None:
    cur.execute("SHOW wal_level")
    level = cur.fetchone()[0]
    if level != "logical":
        raise SetupError(
            f"wal_level is {level!r}; logical replication requires wal_level=logical "
            "(change it in postgresql.conf and restart Postgres)"
        )


def _ensure_sources(cur: Cursor, views: list[ViewSpec]) -> None:
    """Validate each distinct source table exists and has REPLICA IDENTITY FULL."""
    for schema, table in dict.fromkeys((v.source_schema, v.source_table) for v in views):
        cur.execute(
            "SELECT c.relreplident FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'",
            (schema, table),
        )
        row = cur.fetchone()
        if row is None:
            raise SetupError(f"source table {schema}.{table} does not exist")
        if row[0] != "f":
            cur.execute(
                f"ALTER TABLE {quote_ident(schema)}.{quote_ident(table)} REPLICA IDENTITY FULL"
            )
            logger.warning(
                "set REPLICA IDENTITY FULL on %s.%s: every UPDATE/DELETE now writes the "
                "whole old row to WAL (higher WAL volume) — WalFlux needs the old values "
                "to maintain aggregates",
                schema,
                table,
            )


def _ensure_publication(cur: Cursor, config: Config) -> None:
    tables = dict.fromkeys((v.source_schema, v.source_table) for v in config.views)
    table_list = ", ".join(f"{quote_ident(s)}.{quote_ident(t)}" for s, t in tables)
    cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (config.publication,))
    if cur.fetchone() is None:
        cur.execute(f"CREATE PUBLICATION {quote_ident(config.publication)} FOR TABLE {table_list}")
        logger.info("created publication %r for %s", config.publication, table_list)
    else:
        cur.execute(f"ALTER PUBLICATION {quote_ident(config.publication)} SET TABLE {table_list}")
        logger.info("publication %r now set to %s", config.publication, table_list)


def _prepare_slot(cur: Cursor, slot: str, *, force: bool) -> None:
    cur.execute(
        "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = %s",
        (slot,),
    )
    row = cur.fetchone()
    if row is None:
        return
    if not force:
        raise SetupError(
            f"replication slot {slot!r} already exists; re-run with --force to drop and "
            "recreate it (this re-runs the backfill), or remove everything with "
            "`walflux teardown`"
        )
    active, pid = row
    if active:
        raise SetupError(
            f"replication slot {slot!r} is in use by pid {pid}; stop the daemon before "
            "running setup --force"
        )
    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
    logger.info("dropped existing replication slot %r", slot)


def _drop_slot_with_retry(cur: Cursor, slot: str, attempts: int = 10) -> None:
    # A just-terminated walsender can hold the slot active for a moment.
    for attempt in range(attempts):
        try:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            return
        except psycopg2.Error:
            if attempt == attempts - 1:
                raise
            time.sleep(0.3)


def _source_columns(cur: Cursor, view: ViewSpec) -> dict[str, str]:
    """Map column name -> DDL-usable type name for the view's source table.

    Resolved from pg_catalog with ``format_type`` rather than
    ``information_schema.columns``: the latter's ``data_type`` reads
    ``'USER-DEFINED'`` for enum/domain columns and ``'ARRAY'`` for array
    columns — not valid DDL. ``format_type`` yields interpolatable names for
    enums, domains, arrays, and typmods alike (e.g. ``numeric(10,2)``).
    """
    cur.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
        "FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r' "
        "AND a.attnum > 0 AND NOT a.attisdropped",
        (view.source_schema, view.source_table),
    )
    return {name: type_name for name, type_name in cur.fetchall()}


def _required_columns(view: ViewSpec) -> set[str]:
    """Source columns the view cannot do without (group keys + aggregated)."""
    needed = set(view.group_by)
    needed.update(agg.column for agg in view.aggregates if agg.column is not None)
    return needed
