"""Unit tests for daemon row-op resolution and relation tracking (no database)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from walflux.aggregates import AggSpec, ViewSpec
from walflux.common import UNCHANGED_TOAST, SchemaDriftError
from walflux.config import Config
from walflux.daemon import Daemon
from walflux.protocol import Column, Insert, Relation, Update

VIEW = "orders_by_status"


def _config() -> Config:
    view = ViewSpec(
        name=VIEW,
        source_schema="public",
        source_table="orders",
        group_by=["status"],
        aggregates=[
            AggSpec(fn="count", column=None, alias="order_count"),
            AggSpec(fn="sum", column="total", alias="revenue"),
        ],
    )
    return Config(
        dsn="postgresql://unused/unused",
        slot="walflux",
        publication="walflux",
        batch_max_ms=200,
        batch_max_txns=500,
        views=[view],
    )


def _orders_relation(rel_id: int = 1, namespace: str = "public", name: str = "orders") -> Relation:
    return Relation(
        rel_id=rel_id,
        namespace=namespace,
        name=name,
        replica_identity="f",
        columns=(
            Column(name="status", type_oid=25, type_mod=-1, is_key=True),
            Column(name="total", type_oid=1700, type_mod=-1, is_key=True),
            Column(name="note", type_oid=25, type_mod=-1, is_key=True),
        ),
    )


# --- unchanged TOAST in an UPDATE's new tuple --------------------------------


def test_update_new_tuple_unchanged_toast_coalesces_from_old() -> None:
    """pgoutput sends 'u' for unrewritten TOASTed columns in the NEW tuple even
    under REPLICA IDENTITY FULL; the daemon must substitute the (complete) old
    value instead of halting."""
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation())
    msg = Update(
        rel_id=1,
        old=("paid", "10.50", "x" * 10),
        key=None,
        new=("paid", UNCHANGED_TOAST, UNCHANGED_TOAST),
    )
    ops = daemon._row_ops(msg)
    assert len(ops) == 1
    op = ops[0]
    assert op.kind == "update"
    assert op.old == {"status": "paid", "total": Decimal("10.50"), "note": "x" * 10}
    assert op.new == {"status": "paid", "total": Decimal("10.50"), "note": "x" * 10}
    assert UNCHANGED_TOAST not in op.new.values()


def test_update_changed_columns_still_win_over_old() -> None:
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation())
    msg = Update(
        rel_id=1,
        old=("paid", "10.50", "long"),
        key=None,
        new=("shipped", "12.00", UNCHANGED_TOAST),
    )
    (op,) = daemon._row_ops(msg)
    assert op.new == {"status": "shipped", "total": Decimal("12.00"), "note": "long"}


def test_update_without_old_row_raises() -> None:
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation())
    msg = Update(rel_id=1, old=None, key=("paid",), new=("paid", "1.00", "n"))
    with pytest.raises(SchemaDriftError, match="REPLICA IDENTITY FULL"):
        daemon._row_ops(msg)


# --- source table rename detection -------------------------------------------


def test_relation_rename_of_source_table_raises() -> None:
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation())
    with pytest.raises(SchemaDriftError, match=r"public\.orders is now public\.orders_old"):
        daemon._on_relation(_orders_relation(name="orders_old"))


def test_relation_set_schema_of_source_table_raises() -> None:
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation())
    with pytest.raises(SchemaDriftError, match=r"archive\.orders"):
        daemon._on_relation(_orders_relation(namespace="archive"))


def test_relation_for_unconfigured_table_is_ignored() -> None:
    daemon = Daemon(_config())
    daemon._on_relation(_orders_relation(rel_id=7, name="unrelated"))  # no error
    assert daemon._row_ops(Insert(rel_id=7, new=("a", "1", "b"))) == []


def test_relation_missing_needed_column_raises() -> None:
    daemon = Daemon(_config())
    rel = _orders_relation()
    dropped = Relation(
        rel_id=rel.rel_id,
        namespace=rel.namespace,
        name=rel.name,
        replica_identity="f",
        columns=tuple(c for c in rel.columns if c.name != "total"),
    )
    with pytest.raises(SchemaDriftError, match="total"):
        daemon._on_relation(dropped)
