"""Checkpoint table: the replication position the daemon has durably applied.

The checkpoint row is written in the *same* target transaction as the aggregate
deltas it accounts for; together with the daemon's skip rule this is what makes
WalFlux exactly-once across crashes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Connection
    from psycopg2.extensions import cursor as Cursor

#: Idempotent DDL for the checkpoint machinery; executed by ``walflux setup``.
CHECKPOINT_DDL: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS walflux",
    """\
CREATE TABLE IF NOT EXISTS walflux.checkpoint (
  slot        text PRIMARY KEY,
  commit_lsn  bigint NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
)""",
)


def ensure_checkpoint_table(cur: Cursor) -> None:
    """Create the ``walflux`` schema and checkpoint table if missing (idempotent)."""
    for statement in CHECKPOINT_DDL:
        cur.execute(statement)


def read_checkpoint(conn: Connection, slot: str) -> int | None:
    """Return the last durably applied ``Commit.end_lsn`` for *slot*.

    Returns ``None`` when no checkpoint row exists (setup has not run).
    Leaves transaction handling to the caller.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT commit_lsn FROM walflux.checkpoint WHERE slot = %s", (slot,))
        row = cur.fetchone()
    return int(row[0]) if row is not None else None


def write_checkpoint(cur: Cursor, slot: str, lsn: int) -> None:
    """Upsert the checkpoint row using the caller's open cursor/transaction.

    This function deliberately NEVER commits. The caller includes it in the
    same target transaction as the aggregate deltas being applied, so the
    deltas and the replication position they correspond to become durable
    atomically — that atomicity is the heart of WalFlux's exactly-once
    delivery guarantee.
    """
    cur.execute(
        "INSERT INTO walflux.checkpoint (slot, commit_lsn, updated_at) "
        "VALUES (%s, %s, now()) "
        "ON CONFLICT (slot) DO UPDATE SET commit_lsn = EXCLUDED.commit_lsn, updated_at = now()",
        (slot, lsn),
    )
