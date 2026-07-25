"""Daemon main loop: buffer whole source transactions, flush batches exactly-once.

A flush applies the deltas of one or more *whole* source transactions, in
commit order, together with the checkpoint update, in ONE target transaction.
Only after that transaction commits is the position acknowledged to the server.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg2

from walflux.aggregates import DeltaBatch, ViewSpec
from walflux.checkpoint import read_checkpoint, write_checkpoint
from walflux.common import ProtocolError, SchemaDriftError, SetupError, format_lsn, quote_ident
from walflux.protocol import (
    UNCHANGED_TOAST,
    Begin,
    Commit,
    Delete,
    Insert,
    Origin,
    Relation,
    Truncate,
    TupleData,
    TypeInfo,
    Update,
    convert_value,
    decode_message,
)
from walflux.replication import ReplicationStream

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Connection

    from walflux.config import Config

logger = logging.getLogger("walflux.daemon")


@dataclass(frozen=True)
class _Op:
    """One row-level event, already resolved to a view and decoded to dicts."""

    kind: str  # 'insert' | 'update' | 'delete' | 'truncate'
    view: str
    old: dict[str, object] | None = None
    new: dict[str, object] | None = None


@dataclass
class _SealedTxn:
    """A whole source transaction, Begin..Commit, ready to flush."""

    end_lsn: int
    ops: list[_Op]


class Daemon:
    """Consumes the replication stream and maintains all configured views."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._views_by_table: dict[tuple[str, str], list[ViewSpec]] = {}
        for view in config.views:
            key = (view.source_schema, view.source_table)
            self._views_by_table.setdefault(key, []).append(view)
        self._relations: dict[int, Relation] = {}
        self._view_rel_ids: set[int] = set()  # rel_ids that ever resolved to a view
        self._checkpoint_lsn = 0
        self._stop = False
        self._stream: ReplicationStream | None = None

    def run(self) -> None:
        """Block until SIGINT/SIGTERM (graceful stop) or a fatal error (raises)."""
        _configure_logging()
        conn = psycopg2.connect(self._config.dsn)
        stream: ReplicationStream | None = None
        try:
            try:
                checkpoint = read_checkpoint(conn, self._config.slot)
            except psycopg2.errors.UndefinedTable as exc:
                # walflux.checkpoint does not exist: setup has never run here.
                raise SetupError(
                    f"no checkpoint table for slot {self._config.slot!r}; "
                    "run `walflux setup` first"
                ) from exc
            conn.rollback()  # end the implicit read transaction
            if checkpoint is None:
                raise SetupError(
                    f"no checkpoint for slot {self._config.slot!r}; run `walflux setup` first"
                )
            self._checkpoint_lsn = checkpoint
            stream = self._stream = ReplicationStream(
                self._config.dsn, self._config.slot, self._config.publication
            )
            signal.signal(signal.SIGINT, self._request_stop)
            signal.signal(signal.SIGTERM, self._request_stop)
            logger.info(
                "streaming from slot %r, checkpoint %s",
                self._config.slot,
                format_lsn(checkpoint),
            )
            self._loop(conn, stream)
            logger.info("stopped cleanly at checkpoint %s", format_lsn(self._checkpoint_lsn))
        except Exception:
            logger.exception("daemon exiting on error")
            raise
        finally:
            if stream is not None:
                stream.close()
            conn.close()

    def _request_stop(self, signum: int, frame: object) -> None:
        # Signal handler: set flags only — the stream's ~1s wake notices them.
        self._stop = True
        if self._stream is not None:
            self._stream.stop()

    def _loop(self, conn: Connection, stream: ReplicationStream) -> None:
        cfg = self._config
        pending: list[_SealedTxn] = []
        oldest: float | None = None  # arrival time of the oldest unflushed sealed txn
        open_ops: list[_Op] | None = None  # buffer of the currently open Begin..Commit

        # Wake at least every batch_max_ms so the flush deadline is honored
        # even when the stream goes quiet mid-batch.
        wake = min(1.0, cfg.batch_max_ms / 1000)
        for item in stream.messages(wake_seconds=wake):
            if self._stop:
                break
            if item is None:
                # Deadline wake: no decoded message within the wake window —
                # flush whatever is sealed.
                if pending:
                    self._flush(conn, stream, pending)
                    pending, oldest = [], None
                continue
            _wal_end, payload = item
            msg = decode_message(payload)
            if isinstance(msg, Begin):
                open_ops = []
            elif isinstance(msg, Commit):
                ops = open_ops if open_ops is not None else []
                open_ops = None
                # Skip rule: a transaction whose end_lsn is at or before the
                # checkpoint was already applied (and made durable) before a
                # crash — discard the redelivery. This plus the atomic
                # flush+checkpoint transaction is what makes apply exactly-once.
                if msg.end_lsn > self._checkpoint_lsn:
                    pending.append(_SealedTxn(msg.end_lsn, ops))
                    if oldest is None:
                        oldest = time.monotonic()
            elif isinstance(msg, Relation):
                self._on_relation(msg)
            elif isinstance(msg, (Insert, Update, Delete, Truncate)):
                if open_ops is None:
                    raise ProtocolError(f"{type(msg).__name__} message outside Begin..Commit")
                open_ops.extend(self._row_ops(msg))
            elif isinstance(msg, (Origin, TypeInfo)):
                pass  # decoded and ignored by design
            if pending and (
                len(pending) >= cfg.batch_max_txns
                or (time.monotonic() - oldest) * 1000.0 >= cfg.batch_max_ms
            ):
                self._flush(conn, stream, pending)
                pending, oldest = [], None

        # Graceful stop: flush the sealed transactions we have; an open
        # (unsealed) buffer is dropped — it was never checkpointed, so the
        # server redelivers it in full on the next start.
        if pending:
            self._flush(conn, stream, pending)

    def _on_relation(self, rel: Relation) -> None:
        previous = self._relations.get(rel.rel_id)
        self._relations[rel.rel_id] = rel
        views = self._views_by_table.get((rel.namespace, rel.name), [])
        if not views:
            # Publication membership follows the table OID, so a renamed (or
            # re-schema'd) source keeps streaming under its new name — which no
            # longer matches any configured view. Halt loudly instead of
            # silently dropping every subsequent change for its views.
            if rel.rel_id in self._view_rel_ids:
                was = (
                    f"{previous.namespace}.{previous.name}"
                    if previous is not None
                    else "a configured source table"
                )
                raise SchemaDriftError(
                    f"source table {was} is now {rel.namespace}.{rel.name} (renamed or "
                    "moved to another schema); update the config and re-run "
                    "`walflux setup --force`"
                )
            return
        self._view_rel_ids.add(rel.rel_id)
        available = {column.name for column in rel.columns}
        for view in views:
            missing = sorted(_required_columns(view) - available)
            if missing:
                raise SchemaDriftError(
                    f"source table {rel.namespace}.{rel.name} no longer has column(s) "
                    f"{', '.join(missing)} needed by view {view.name!r}; fix the schema "
                    "and re-run `walflux setup --force`"
                )

    def _row_ops(self, msg: Insert | Update | Delete | Truncate) -> list[_Op]:
        """Resolve a row message to per-view ops with decoded row dicts."""
        if isinstance(msg, Truncate):
            ops: list[_Op] = []
            for rel_id in msg.rel_ids:
                rel = self._relations.get(rel_id)
                if rel is None:
                    continue
                for view in self._views_by_table.get((rel.namespace, rel.name), []):
                    ops.append(_Op("truncate", view.name))
            return ops
        rel = self._relations.get(msg.rel_id)
        if rel is None:
            raise ProtocolError(f"row message for unknown relation id {msg.rel_id}")
        views = self._views_by_table.get((rel.namespace, rel.name), [])
        if not views:
            return []
        if isinstance(msg, Insert):
            row = _decode_row(rel, msg.new)
            return [_Op("insert", view.name, new=row) for view in views]
        if isinstance(msg, Update):
            if msg.old is None:
                raise SchemaDriftError(
                    f"update on {rel.namespace}.{rel.name} arrived without the old row; "
                    "the table needs REPLICA IDENTITY FULL — re-run `walflux setup --force`"
                )
            old, new = _decode_row(rel, msg.old), _decode_row(rel, msg.new)
            # Even under REPLICA IDENTITY FULL, pgoutput inlines TOASTed values
            # only in the OLD tuple (Postgres flattens them when building the
            # old row image); the NEW tuple still carries the unchanged-TOAST
            # marker for any TOASTed column the UPDATE did not rewrite.
            # Unchanged means new == old and the old image is complete, so
            # substitute the old value. A marker that survives (i.e. one in the
            # old image itself) still halts in DeltaBatch — that genuinely
            # indicates the replica identity is not FULL.
            for name, value in new.items():
                if value is UNCHANGED_TOAST:
                    new[name] = old[name]
            return [_Op("update", view.name, old=old, new=new) for view in views]
        if msg.old is None:
            raise SchemaDriftError(
                f"delete on {rel.namespace}.{rel.name} arrived without the old row; "
                "the table needs REPLICA IDENTITY FULL — re-run `walflux setup --force`"
            )
        row = _decode_row(rel, msg.old)
        return [_Op("delete", view.name, old=row) for view in views]

    def _flush(
        self, conn: Connection, stream: ReplicationStream, pending: list[_SealedTxn]
    ) -> None:
        """Apply whole sealed transactions + checkpoint atomically, then ack."""
        started = time.monotonic()
        batch = DeltaBatch(self._config.views)
        for txn in pending:
            for op in txn.ops:
                if op.kind == "insert":
                    batch.apply_insert(op.view, op.new)
                elif op.kind == "update":
                    batch.apply_update(op.view, op.old, op.new)
                elif op.kind == "delete":
                    batch.apply_delete(op.view, op.old)
                else:
                    batch.apply_truncate(op.view)
        end_lsn = pending[-1].end_lsn
        statements = batch.statements()
        # ONE target transaction: deltas and checkpoint become durable together;
        # only after the commit is the position acknowledged to the server.
        with conn.cursor() as cur:
            for sql, params in statements:
                cur.execute(sql, params)
            write_checkpoint(cur, self._config.slot, end_lsn)
        conn.commit()
        if os.environ.get("WALFLUX_TEST_CRASH") == "after_commit_before_ack":
            # Fault injection for the integration suite ONLY: die hard inside
            # the commit-to-ack window so the server redelivers a batch that is
            # already durably applied — the skip rule must discard it.
            os._exit(1)
        stream.ack(end_lsn)
        self._checkpoint_lsn = end_lsn
        upserts = sum(1 for sql, _ in statements if sql.lstrip().upper().startswith("INSERT"))
        logger.info(
            "flushed %d txn(s), %d group upsert(s), end LSN %s, %d ms",
            len(pending),
            upserts,
            format_lsn(end_lsn),
            int((time.monotonic() - started) * 1000),
        )


def status(config: Config) -> dict[str, object]:
    """Snapshot of slot / lag / checkpoint / per-view row counts for `walflux status`."""
    conn = psycopg2.connect(config.dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT active, restart_lsn::text, confirmed_flush_lsn::text, "
                "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (config.slot,),
            )
            slot_row = cur.fetchone()
            cur.execute("SELECT pg_current_wal_lsn()::text")
            current_lsn = cur.fetchone()[0]
            try:
                checkpoint = read_checkpoint(conn, config.slot)
            except psycopg2.Error:
                checkpoint = None  # walflux schema not created yet
            views: dict[str, int | None] = {}
            for view in config.views:
                try:
                    cur.execute(f"SELECT count(*) FROM walflux.{quote_ident(view.name)}")
                    views[view.name] = int(cur.fetchone()[0])
                except psycopg2.Error:
                    views[view.name] = None
        lag = slot_row[3] if slot_row is not None else None
        return {
            "slot": config.slot,
            "slot_exists": slot_row is not None,
            "active": bool(slot_row[0]) if slot_row is not None else None,
            "restart_lsn": slot_row[1] if slot_row is not None else None,
            "confirmed_flush_lsn": slot_row[2] if slot_row is not None else None,
            "lag_bytes": int(lag) if lag is not None else None,
            "current_lsn": current_lsn,
            "checkpoint_lsn": format_lsn(checkpoint) if checkpoint is not None else None,
            "views": views,
        }
    finally:
        conn.close()


def _decode_row(rel: Relation, data: TupleData) -> dict[str, object]:
    """Zip TupleData with the relation's columns into a converted row dict."""
    row: dict[str, object] = {}
    for column, value in zip(rel.columns, data, strict=True):
        if value is None or value is UNCHANGED_TOAST:
            row[column.name] = value
        else:
            row[column.name] = convert_value(value, column.type_oid)
    return row


def _required_columns(view: ViewSpec) -> set[str]:
    """Source columns the view cannot do without (group keys + aggregated)."""
    needed = set(view.group_by)
    needed.update(agg.column for agg in view.aggregates if agg.column is not None)
    return needed


def _configure_logging() -> None:
    """Attach the stderr handler once (idempotent within a process)."""
    root = logging.getLogger("walflux")
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        root.addHandler(handler)
    root.setLevel(logging.INFO)
