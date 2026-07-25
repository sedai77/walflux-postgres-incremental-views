"""Tests for walflux.aggregates (Module B): DDL, backfill SQL, delta batches."""

from __future__ import annotations

from decimal import Decimal

import pytest

from walflux.aggregates import AggSpec, DeltaBatch, ViewSpec, backfill_sql, target_ddl
from walflux.common import UNCHANGED_TOAST, SchemaDriftError, WalfluxError

GROUPED = "orders_by_status"
GLOBAL = "orders_totals"


def orders_view() -> ViewSpec:
    """The representative grouped view from the SPEC's config example."""
    return ViewSpec(
        name=GROUPED,
        source_schema="public",
        source_table="orders",
        group_by=["status"],
        aggregates=[
            AggSpec(fn="count", column=None, alias="order_count"),
            AggSpec(fn="count", column="coupon", alias="with_coupon"),
            AggSpec(fn="sum", column="total", alias="revenue"),
            AggSpec(fn="avg", column="total", alias="avg_order_value"),
        ],
    )


def global_view() -> ViewSpec:
    """A grouped-by-nothing (single-row) view."""
    return ViewSpec(
        name=GLOBAL,
        source_schema="public",
        source_table="orders",
        group_by=[],
        aggregates=[
            AggSpec(fn="count", column=None, alias="n_orders"),
            AggSpec(fn="sum", column="total", alias="revenue"),
        ],
    )


def order(
    status: str | None = "paid",
    total: object = None,
    coupon: str | None = None,
) -> dict[str, object]:
    return {"status": status, "total": total, "coupon": coupon}


GROUPED_CLEANUP = 'DELETE FROM "walflux"."orders_by_status" WHERE "__walflux_rows" <= 0'

# Param layout for orders_by_status upserts:
#   (status, __walflux_rows, order_count, with_coupon,
#    revenue__sum, revenue__nn, avg_order_value__sum, avg_order_value__nn)
REVENUE_SUM_SLOT = 4


# --- target_ddl -------------------------------------------------------------


def test_target_ddl_grouped_snapshot() -> None:
    drop, create, index = target_ddl(orders_view(), {"status": "text"})
    assert drop == 'DROP TABLE IF EXISTS "walflux"."orders_by_status"'
    assert create == "\n".join(
        [
            'CREATE TABLE "walflux"."orders_by_status" (',
            '    "status" text,',
            '    "__walflux_rows" bigint NOT NULL,',
            '    "order_count" bigint NOT NULL,',
            '    "with_coupon" bigint NOT NULL,',
            '    "revenue__sum" numeric,',
            '    "revenue__nn" bigint NOT NULL,',
            (
                '    "revenue" numeric GENERATED ALWAYS AS '
                '(CASE WHEN "revenue__nn" > 0 THEN "revenue__sum" END) STORED,'
            ),
            '    "avg_order_value__sum" numeric,',
            '    "avg_order_value__nn" bigint NOT NULL,',
            (
                '    "avg_order_value" numeric GENERATED ALWAYS AS '
                '(CASE WHEN "avg_order_value__nn" > 0 THEN '
                '"avg_order_value__sum" / "avg_order_value__nn" END) STORED'
            ),
            ")",
        ]
    )
    assert index == (
        'CREATE UNIQUE INDEX ON "walflux"."orders_by_status" ("status") NULLS NOT DISTINCT'
    )


def test_target_ddl_global_uses_constant_key() -> None:
    drop, create, index = target_ddl(global_view(), {})
    assert drop == 'DROP TABLE IF EXISTS "walflux"."orders_totals"'
    assert '"__walflux_all" boolean NOT NULL DEFAULT true,' in create
    assert '"status"' not in create
    assert index == (
        'CREATE UNIQUE INDEX ON "walflux"."orders_totals" ("__walflux_all") NULLS NOT DISTINCT'
    )


def test_target_ddl_missing_group_type_raises() -> None:
    with pytest.raises(WalfluxError, match="status"):
        target_ddl(orders_view(), {})


def test_unknown_aggregate_fn_raises() -> None:
    view = orders_view()
    view.aggregates.append(AggSpec(fn="min", column="total", alias="smallest"))
    with pytest.raises(WalfluxError, match="min"):
        target_ddl(view, {"status": "text"})
    with pytest.raises(WalfluxError, match="min"):
        DeltaBatch([view])


# --- backfill_sql -----------------------------------------------------------


def test_backfill_grouped_snapshot() -> None:
    assert backfill_sql(orders_view()) == (
        'INSERT INTO "walflux"."orders_by_status" '
        '("status", "__walflux_rows", "order_count", "with_coupon", '
        '"revenue__sum", "revenue__nn", "avg_order_value__sum", "avg_order_value__nn")\n'
        'SELECT "status", count(*), count(*), count("coupon"), '
        'coalesce(sum("total"), 0), count("total"), '
        'coalesce(sum("total"), 0), count("total")\n'
        'FROM "public"."orders"\n'
        'GROUP BY "status"'
    )


def test_backfill_global_snapshot() -> None:
    assert backfill_sql(global_view()) == (
        'INSERT INTO "walflux"."orders_totals" '
        '("__walflux_all", "__walflux_rows", "n_orders", "revenue__sum", "revenue__nn")\n'
        "SELECT true, count(*), count(*), coalesce(sum(\"total\"), 0), count(\"total\")\n"
        'FROM "public"."orders"'
    )


# --- upsert SQL shape -------------------------------------------------------


def test_upsert_sql_snapshot() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order())
    sql, _ = batch.statements()[0]
    lines = sql.split("\n")
    assert lines[0] == (
        'INSERT INTO "walflux"."orders_by_status" '
        '("status", "__walflux_rows", "order_count", "with_coupon", '
        '"revenue__sum", "revenue__nn", "avg_order_value__sum", "avg_order_value__nn")'
    )
    assert lines[1] == "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    assert lines[2] == 'ON CONFLICT ("status") DO UPDATE SET'
    for col in [
        "__walflux_rows",
        "order_count",
        "with_coupon",
        "revenue__sum",
        "revenue__nn",
        "avg_order_value__sum",
        "avg_order_value__nn",
    ]:
        expected = f'"{col}" = "walflux"."orders_by_status"."{col}" + EXCLUDED."{col}"'
        assert expected in sql


# --- delta accumulation -----------------------------------------------------


def test_insert_deltas_and_null_handling() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(total=Decimal("10.50"), coupon="SAVE5"))
    batch.apply_insert(GROUPED, order(total=None, coupon=None))  # NULLs contribute nothing
    statements = batch.statements()
    assert len(statements) == 2  # one group upsert + cleanup
    _, params = statements[0]
    assert params == ("paid", 2, 2, 1, Decimal("10.50"), 1, Decimal("10.50"), 1)
    assert statements[1] == (GROUPED_CLEANUP, ())


def test_delete_negates_insert() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_delete(GROUPED, order(total=Decimal("4.20"), coupon="X"))
    _, params = batch.statements()[0]
    assert params == ("paid", -1, -1, -1, Decimal("-4.20"), -1, Decimal("-4.20"), -1)


def test_update_moving_between_groups() -> None:
    batch = DeltaBatch([orders_view()])
    old = order(status="paid", total=Decimal("10"))
    new = order(status="shipped", total=Decimal("10"))
    batch.apply_update(GROUPED, old, new)
    statements = batch.statements()
    assert len(statements) == 3  # two group upserts + cleanup
    assert statements[0][1] == ("paid", -1, -1, 0, Decimal("-10"), -1, Decimal("-10"), -1)
    assert statements[1][1] == ("shipped", 1, 1, 0, Decimal("10"), 1, Decimal("10"), 1)
    assert statements[2] == (GROUPED_CLEANUP, ())


def test_update_within_group_changes_only_sums() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_update(GROUPED, order(total=Decimal("10")), order(total=Decimal("12.5")))
    _, params = batch.statements()[0]
    assert params == ("paid", 0, 0, 0, Decimal("2.5"), 0, Decimal("2.5"), 0)


def test_avg_storage_arithmetic() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(total=Decimal("1")))
    batch.apply_insert(GROUPED, order(total=Decimal("2")))
    batch.apply_insert(GROUPED, order(total=None))
    batch.apply_delete(GROUPED, order(total=Decimal("1")))
    _, params = batch.statements()[0]
    # rows 3-1=2; avg storage: __sum 1+2-1=2, __nn 1+1-1=1 (NULL row contributes nothing)
    assert params == ("paid", 2, 2, 0, Decimal("2"), 1, Decimal("2"), 1)


def test_null_group_key_folds_into_one_group() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(status=None, total=Decimal("1")))
    batch.apply_insert(GROUPED, order(status=None, total=Decimal("2")))
    statements = batch.statements()
    assert len(statements) == 2  # single NULL-keyed group + cleanup
    _, params = statements[0]
    assert params == (None, 2, 2, 0, Decimal("3"), 2, Decimal("3"), 2)


def test_group_cancellation_emits_cleanup_delete() -> None:
    batch = DeltaBatch([orders_view()])
    row = order(total=Decimal("7"), coupon="C")
    batch.apply_insert(GROUPED, row)
    batch.apply_delete(GROUPED, row)
    statements = batch.statements()
    assert statements[0][1] == ("paid", 0, 0, 0, Decimal("0"), 0, Decimal("0"), 0)
    assert statements[-1] == (GROUPED_CLEANUP, ())


def test_decimal_precision() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(total=Decimal("0.1")))
    batch.apply_insert(GROUPED, order(total=Decimal("0.2")))
    total = batch.statements()[0][1][REVENUE_SUM_SLOT]
    assert isinstance(total, Decimal)
    assert str(total) == "0.3"  # exact, no float artifacts


def test_float_values_use_shortest_repr() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(total=0.1))
    batch.apply_insert(GROUPED, order(total=0.2))
    total = batch.statements()[0][1][REVENUE_SUM_SLOT]
    assert total == Decimal("0.3")


# --- truncate ---------------------------------------------------------------


def test_truncate_discards_prior_deltas_and_orders_statements() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_insert(GROUPED, order(status="pending", total=Decimal("99")))
    batch.apply_truncate(GROUPED)
    batch.apply_insert(GROUPED, order(status="paid", total=Decimal("5")))
    statements = batch.statements()
    assert len(statements) == 3
    assert statements[0] == ('DELETE FROM "walflux"."orders_by_status"', ())
    # Only the post-truncate insert survives, as absolute values into the emptied table.
    assert statements[1][1] == ("paid", 1, 1, 0, Decimal("5"), 1, Decimal("5"), 1)
    assert statements[2] == (GROUPED_CLEANUP, ())


def test_truncate_alone_still_flushes() -> None:
    batch = DeltaBatch([orders_view()])
    batch.apply_truncate(GROUPED)
    assert not batch.is_empty()
    assert batch.statements() == [
        ('DELETE FROM "walflux"."orders_by_status"', ()),
        (GROUPED_CLEANUP, ()),
    ]


# --- global (empty group_by) views ------------------------------------------


def test_global_view_upsert_and_no_cleanup() -> None:
    batch = DeltaBatch([global_view()])
    batch.apply_insert(GLOBAL, {"total": Decimal("3")})
    batch.apply_insert(GLOBAL, {"total": None})
    statements = batch.statements()
    assert len(statements) == 1  # no zero-row cleanup for a single-row target
    sql, params = statements[0]
    assert 'ON CONFLICT ("__walflux_all")' in sql
    assert params == (True, 2, 2, Decimal("3"), 1)


def test_global_truncate_recreates_single_row() -> None:
    batch = DeltaBatch([global_view()])
    batch.apply_insert(GLOBAL, {"total": Decimal("8")})
    batch.apply_truncate(GLOBAL)
    statements = batch.statements()
    # DELETE empties the target, then a zeroed upsert re-creates the single row;
    # its counts read 0 and generated columns yield NULL — never a missing row.
    assert statements[0] == ('DELETE FROM "walflux"."orders_totals"', ())
    assert statements[1][1] == (True, 0, 0, Decimal("0"), 0)
    assert len(statements) == 2


def test_global_truncate_then_inserts_accumulate() -> None:
    batch = DeltaBatch([global_view()])
    batch.apply_truncate(GLOBAL)
    batch.apply_insert(GLOBAL, {"total": Decimal("2")})
    statements = batch.statements()
    assert statements[1][1] == (True, 1, 1, Decimal("2"), 1)


# --- multi-view batches -----------------------------------------------------


def test_statement_ordering_across_views() -> None:
    batch = DeltaBatch([orders_view(), global_view()])
    batch.apply_insert(GROUPED, order(total=Decimal("1")))
    batch.apply_truncate(GLOBAL)
    statements = batch.statements()
    assert [sql.split("\n")[0] for sql, _ in statements] == [
        'DELETE FROM "walflux"."orders_totals"',  # truncates first
        (
            'INSERT INTO "walflux"."orders_by_status" '
            '("status", "__walflux_rows", "order_count", "with_coupon", '
            '"revenue__sum", "revenue__nn", "avg_order_value__sum", "avg_order_value__nn")'
        ),
        (
            'INSERT INTO "walflux"."orders_totals" '
            '("__walflux_all", "__walflux_rows", "n_orders", "revenue__sum", "revenue__nn")'
        ),
        GROUPED_CLEANUP,  # cleanup last, grouped views only
    ]


# --- errors and emptiness ---------------------------------------------------


def test_is_empty() -> None:
    batch = DeltaBatch([orders_view()])
    assert batch.is_empty()
    batch.apply_insert(GROUPED, order())
    assert not batch.is_empty()


def test_unchanged_toast_raises_schema_drift() -> None:
    batch = DeltaBatch([orders_view()])
    old = {"status": "paid", "total": UNCHANGED_TOAST, "coupon": None}
    with pytest.raises(SchemaDriftError, match="REPLICA IDENTITY FULL"):
        batch.apply_update(GROUPED, old, order(total=Decimal("1")))


def test_non_numeric_sum_value_raises() -> None:
    batch = DeltaBatch([orders_view()])
    with pytest.raises(WalfluxError, match=r"'total'.*str"):
        batch.apply_insert(GROUPED, order(total="not-a-number"))


def test_missing_needed_column_raises_schema_drift() -> None:
    batch = DeltaBatch([orders_view()])
    with pytest.raises(SchemaDriftError, match="coupon"):
        batch.apply_insert(GROUPED, {"status": "paid", "total": None})


def test_unknown_view_raises() -> None:
    batch = DeltaBatch([orders_view()])
    with pytest.raises(WalfluxError, match="unknown view"):
        batch.apply_insert("nope", order())


def test_non_finite_value_halts_loudly() -> None:
    # NaN/Infinity cannot be cancelled additively (NaN - NaN = NaN), so the
    # daemon halts rather than letting the aggregate silently diverge.
    batch = DeltaBatch([orders_view()])
    for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(WalfluxError, match="non-finite"):
            batch.apply_insert(GROUPED, order(total=bad))
