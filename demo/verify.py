#!/usr/bin/env python3
"""Verify walflux target tables against ground-truth GROUP BY queries.

Run with the write generator STOPPED (kill9.sh and `make verify` do this)
so the source table holds still. The script waits — pause-tolerantly, with
a timeout — for the daemon's checkpoint to pass the current WAL position,
then compares every view in the demo config against a fresh GROUP BY over
its source table and prints a side-by-side table ending in a loud PASS or
FAIL line. Exit code: 0 on PASS, 1 on FAIL or catch-up timeout.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extensions
import yaml

from walflux.common import format_lsn, parse_lsn, quote_ident

CONFIG_PATH = os.environ.get("WALFLUX_CONFIG", "/demo/config.yaml")
TIMEOUT_S = float(os.environ.get("WALFLUX_VERIFY_TIMEOUT", "120"))
POLL_S = 0.5
# Counts and sums must match exactly; averages get a hair of tolerance because
# the target's generated column divides at a different numeric scale than avg().
AVG_TOLERANCE = Decimal("0.000001")


@dataclass(frozen=True)
class Agg:
    fn: str
    column: str | None
    alias: str


@dataclass(frozen=True)
class View:
    name: str
    schema: str
    table: str
    group_by: tuple[str, ...]
    aggregates: tuple[Agg, ...]


def log(msg: str) -> None:
    """Print one progress line, unbuffered so `docker logs` streams it."""
    print(f"[verify] {msg}", flush=True)


def banner(text: str) -> str:
    """A loud, README-quotable banner line."""
    bar = "=" * 64
    return f"\n{bar}\n  {text}\n{bar}"


def load_views(cfg: dict[str, Any]) -> list[View]:
    """Build View specs from the raw YAML config mapping."""
    views: list[View] = []
    for raw in cfg["views"]:
        schema, _, table = str(raw["source"]).rpartition(".")
        views.append(
            View(
                name=raw["name"],
                schema=schema or "public",
                table=table,
                group_by=tuple(raw.get("group_by") or ()),
                aggregates=tuple(
                    Agg(fn=a["fn"], column=a.get("column"), alias=a["as"])
                    for a in raw["aggregates"]
                ),
            )
        )
    return views


def _read_checkpoint(conn: psycopg2.extensions.connection, slot: str) -> int | None:
    """Return the daemon's checkpoint LSN, or None while setup has not run yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('walflux.checkpoint')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute("SELECT commit_lsn FROM walflux.checkpoint WHERE slot = %s", (slot,))
        row = cur.fetchone()
        return None if row is None else int(row[0])


def wait_for_catchup(conn: psycopg2.extensions.connection, slot: str) -> bool:
    """Wait until walflux.checkpoint passes the WAL position captured on entry.

    The target LSN is captured BEFORE the marker writes below, so it covers
    every transaction the (now stopped) generator committed. The two marker
    transactions — a net-zero insert then delete — commit after the target
    and flow through the slot, guaranteeing the daemon applies something
    whose commit LSN exceeds the target even on an otherwise idle server
    (background WAL activity alone never advances the checkpoint).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()")
        target = parse_lsn(cur.fetchone()[0])
        # Demo-specific marker rows in orders; net effect on every aggregate is zero.
        cur.execute(
            "INSERT INTO orders (status, total, coupon)"
            " VALUES ('walflux_verify_marker', NULL, NULL) RETURNING id"
        )
        marker_id = cur.fetchone()[0]
        cur.execute("DELETE FROM orders WHERE id = %s", (marker_id,))

    log(f"waiting for daemon checkpoint to reach {format_lsn(target)}"
        f" (timeout {TIMEOUT_S:.0f}s)")
    deadline = time.monotonic() + TIMEOUT_S
    last_report = 0.0
    while time.monotonic() < deadline:
        ckpt = _read_checkpoint(conn, slot)
        if ckpt is not None and ckpt >= target:
            log(f"caught up at checkpoint {format_lsn(ckpt)}")
            return True
        now = time.monotonic()
        if now - last_report >= 2.0:
            last_report = now
            if ckpt is None:
                log("no checkpoint yet (daemon starting, or setup has not run)")
            else:
                log(f"checkpoint {format_lsn(ckpt)} is {target - ckpt} bytes behind; waiting")
        time.sleep(POLL_S)
    return False


def _agg_expr(agg: Agg) -> str:
    """The ground-truth SQL expression for one configured aggregate."""
    if agg.fn == "count" and agg.column is None:
        return "count(*)"
    return f"{agg.fn}({quote_ident(agg.column or '')})"


def _ground_truth_sql(view: View) -> str:
    """A fresh GROUP BY over the source table, aligned with the view's columns."""
    cols = [quote_ident(g) for g in view.group_by] + [_agg_expr(a) for a in view.aggregates]
    sql = (f"SELECT {', '.join(cols)}"
           f" FROM {quote_ident(view.schema)}.{quote_ident(view.table)}")
    if view.group_by:
        sql += " GROUP BY " + ", ".join(quote_ident(g) for g in view.group_by)
    return sql


def _target_sql(view: View) -> str:
    """Select the group keys and aggregate aliases from the walflux target table."""
    names = (*view.group_by, *(a.alias for a in view.aggregates))
    cols = ", ".join(quote_ident(n) for n in names)
    return f"SELECT {cols} FROM walflux.{quote_ident(view.name)}"


def _rows(cur: psycopg2.extensions.cursor, sql: str, n_group_cols: int) -> dict[tuple, tuple]:
    """Fetch rows keyed by group-key tuple, valued by aggregate tuple."""
    cur.execute(sql)
    return {tuple(r[:n_group_cols]): tuple(r[n_group_cols:]) for r in cur.fetchall()}


def _cells_match(fn: str, expected: Any, actual: Any) -> bool:
    """Exact Decimal comparison, except a tiny tolerance for avg (scale differs)."""
    if expected is None or actual is None:
        return expected is None and actual is None
    if fn == "avg":
        return abs(Decimal(expected) - Decimal(actual)) <= AVG_TOLERANCE
    return Decimal(expected) == Decimal(actual)


def _fmt(value: Any) -> str:
    return "NULL" if value is None else str(value)


def _null_safe_key(key: tuple) -> tuple:
    # Sort NULL group keys first, everything else by string form.
    return tuple((v is not None, str(v)) for v in key)


def _group_label(view: View, key: tuple) -> str:
    if not view.group_by:
        return "(all rows)"
    return ", ".join(
        f"{g}={_fmt(v)}" for g, v in zip(view.group_by, key, strict=True)
    )


def _render(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    """Plain-text aligned table."""
    widths = [len(h) for h in headers]
    for row in rows:
        widths = [max(w, len(c)) for w, c in zip(widths, row, strict=True)]

    def line(cells: tuple[str, ...]) -> str:
        return "  ".join(c.ljust(w) for c, w in zip(cells, widths, strict=True)).rstrip()

    sep = tuple("-" * w for w in widths)
    return "\n".join([line(headers), line(sep), *(line(r) for r in rows)])


def _dead_group_count(cur: psycopg2.extensions.cursor, view: View) -> int:
    """Group rows that should have been cleaned up (the global row may sit at 0)."""
    if not view.group_by:
        return 0
    cur.execute(
        f"SELECT count(*) FROM walflux.{quote_ident(view.name)} WHERE __walflux_rows <= 0"
    )
    return int(cur.fetchone()[0])


def verify_view(cur: psycopg2.extensions.cursor, view: View) -> int:
    """Print the side-by-side table for one view; return its mismatch count."""
    expected = _rows(cur, _ground_truth_sql(view), len(view.group_by))
    actual = _rows(cur, _target_sql(view), len(view.group_by))

    headers = ("group", "column", "ground truth", "walflux", "result")
    rows: list[tuple[str, ...]] = []
    mismatches = 0
    for key in sorted(set(expected) | set(actual), key=_null_safe_key):
        label = _group_label(view, key)
        if key not in actual:
            rows.append((label, "*", "group present", "group MISSING", "MISMATCH"))
            mismatches += 1
            continue
        if key not in expected:
            rows.append((label, "*", "no such group", "extra group", "MISMATCH"))
            mismatches += 1
            continue
        for agg, exp, act in zip(view.aggregates, expected[key], actual[key], strict=True):
            ok = _cells_match(agg.fn, exp, act)
            mismatches += 0 if ok else 1
            rows.append((label, agg.alias, _fmt(exp), _fmt(act), "ok" if ok else "MISMATCH"))

    print(f"\nview {view.name}"
          f"  (walflux.{view.name} vs GROUP BY over {view.schema}.{view.table})")
    print(_render(headers, rows))

    dead = _dead_group_count(cur, view)
    if dead:
        print(f"MISMATCH: {dead} group row(s) with __walflux_rows <= 0 were not cleaned up")
        mismatches += dead
    return mismatches


def main() -> int:
    """Wait for catch-up, compare every configured view, print PASS or FAIL."""
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    dsn = os.environ.get("WALFLUX_DSN") or cfg["database"]["dsn"]
    slot = str(cfg.get("slot") or "walflux")
    views = load_views(cfg)

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    log(f"comparing {len(views)} view(s) from {CONFIG_PATH}")
    log("(the write generator must be stopped, or the target keeps moving)")
    if not wait_for_catchup(conn, slot):
        print(banner("FAIL — daemon checkpoint did not catch up within the timeout"))
        return 1

    # One REPEATABLE READ snapshot for all comparison queries, so ground truth
    # and target tables are read at the same instant.
    conn.autocommit = False
    conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    total_mismatches = 0
    with conn.cursor() as cur:
        for view in views:
            total_mismatches += verify_view(cur, view)
    conn.rollback()
    conn.close()

    if total_mismatches:
        print(banner(f"FAIL — {total_mismatches} mismatch(es); see the table above"))
        return 1
    print(banner("PASS — every walflux target matches its ground-truth GROUP BY"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
