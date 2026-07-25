"""End-to-end exactly-once proof: kill -9 the daemon mid-workload and verify.

Flow (SPEC Module D): scratch schema + ``orders`` table; two views (grouped +
global); programmatic ``setup`` with snapshot backfill; mixed workload
(inserts, group-moving updates, deletes, one TRUNCATE, NULLs); daemon in a
subprocess; SIGKILL mid-second-workload; restart; wait for catch-up; target
tables must equal the ground-truth GROUP BY queries.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import textwrap
import threading
import time
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from walflux.bootstrap import setup, teardown
from walflux.common import parse_lsn
from walflux.config import load_config

DSN = os.environ.get("WALFLUX_TEST_DSN")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not DSN,
        reason="WALFLUX_TEST_DSN not set (needs Postgres 15+ with wal_level=logical)",
    ),
]

_RUNNER = textwrap.dedent(
    """\
    import sys

    from walflux.config import load_config
    from walflux.daemon import Daemon

    Daemon(load_config(sys.argv[1])).run()
    """
)

_AGGREGATES_YAML = """\
    aggregates:
      - fn: count
        as: order_count
      - fn: count
        column: coupon
        as: with_coupon
      - fn: sum
        column: total
        as: revenue
      - fn: avg
        column: total
        as: avg_order_value
"""


def _config_yaml(schema: str, slot: str, grouped_view: str, global_view: str) -> str:
    return (
        f'database:\n  dsn: "{DSN}"\n'
        f"slot: {slot}\n"
        f"publication: {slot}\n"
        "batch:\n  max_ms: 100\n  max_txns: 50\n"
        "views:\n"
        f"  - name: {grouped_view}\n"
        f"    source: {schema}.orders\n"
        "    group_by: [status]\n"
        f"{_AGGREGATES_YAML}"
        f"  - name: {global_view}\n"
        f"    source: {schema}.orders\n"
        "    group_by: []\n"
        f"{_AGGREGATES_YAML}"
    )


def _start_daemon(
    runner: Path,
    cfg_path: Path,
    log_path: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    env = {**os.environ, **(extra_env or {})}
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(  # noqa: S603 - test drives its own daemon
            [sys.executable, str(runner), str(cfg_path)], stdout=log, stderr=log, env=env
        )
    time.sleep(1.0)
    assert proc.poll() is None, f"daemon exited at startup; log:\n{log_path.read_text()}"
    return proc


def _wait_caught_up(
    conn: psycopg2.extensions.connection,
    slot: str,
    schema: str,
    log_path: Path,
    timeout: float = 90.0,
) -> None:
    """Poll until the checkpoint reaches the WAL position captured at entry.

    The target is captured ONCE, up front: every flush the daemon makes writes
    WAL of its own (the target-table upserts + checkpoint row), so the
    checkpoint can never reach a target that is re-captured each round — it
    would chase its own tail forever. Reaching the entry position is the right
    barrier anyway: it proves every source transaction committed before the
    call has been applied.

    WAL can sit past the last commit record (background activity records), so
    ONE small "nudge" transaction is committed after capturing the target: its
    Commit is decodable and its end_lsn lies strictly past the target,
    guaranteeing the checkpoint can pass it. Exactly one — a nudge per round
    would race the caller's assertions (ground truth would see a nudge the
    daemon has not applied at the moment this returns). The comparison is
    STRICT: the last pre-target transaction's end_lsn can equal the target
    exactly, and the nudge is the only transaction past it, so
    ``checkpoint > target`` proves the nudge itself was applied. Nudge rows
    are ordinary data and are included in the ground truth.
    """
    deadline = time.monotonic() + timeout
    with conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        target = parse_lsn(cur.fetchone()[0])
        cur.execute(
            f'INSERT INTO "{schema}".orders (status, total, coupon) VALUES (%s, %s, %s)',
            ("nudge", Decimal("1.00"), None),
        )
        while True:
            cur.execute("SELECT commit_lsn FROM walflux.checkpoint WHERE slot = %s", (slot,))
            row = cur.fetchone()
            if row is not None and int(row[0]) > target:
                return
            if time.monotonic() > deadline:
                tail = log_path.read_text()[-2000:] if log_path.exists() else "<no log>"
                pytest.fail(
                    f"daemon did not catch up (checkpoint={row}, target={target}); "
                    f"daemon log tail:\n{tail}"
                )
            time.sleep(0.25)


def _wait_slot_released(
    conn: psycopg2.extensions.connection, slot: str, timeout: float = 30.0
) -> None:
    """After SIGKILL, wait for the dead daemon's walsender to release the slot."""
    deadline = time.monotonic() + timeout
    with conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,))
            row = cur.fetchone()
            if row is not None and not row[0]:
                return
            time.sleep(0.2)
    pytest.fail(f"slot {slot!r} still active long after the daemon was killed")


def _mixed_txns(
    conn: psycopg2.extensions.connection,
    schema: str,
    start: int,
    count: int,
    sleep: float = 0.0,
) -> None:
    """*count* small transactions: inserts (with NULLs), group-moving updates, deletes."""
    statuses = ("new", "paid", "shipped", None)
    with conn.cursor() as cur:
        for i in range(start, start + count):
            cur.execute(
                f'INSERT INTO "{schema}".orders (status, total, coupon) '
                "VALUES (%s, %s, %s), (%s, %s, %s)",
                (
                    statuses[i % 4],
                    Decimal(i) / 4 if i % 5 else None,
                    f"c{i}" if i % 3 == 0 else None,
                    statuses[(i + 1) % 4],
                    Decimal("0.10") * i,
                    None,
                ),
            )
            if i % 4 == 0:
                cur.execute(
                    f'UPDATE "{schema}".orders SET status = %s WHERE id %% 9 = %s',
                    (statuses[i % 3], i % 9),
                )
            if i % 6 == 0:
                cur.execute(f'DELETE FROM "{schema}".orders WHERE id %% 17 = %s', (i % 17,))
            conn.commit()
            if sleep:
                time.sleep(sleep)


def _workload_one(conn: psycopg2.extensions.connection, schema: str) -> None:
    """Mixed writes, then move rows to the NULL group, then TRUNCATE + re-seed."""
    _mixed_txns(conn, schema, start=1, count=20)
    with conn.cursor() as cur:
        cur.execute(f'UPDATE "{schema}".orders SET status = NULL WHERE id %% 11 = %s', (0,))
        conn.commit()
        cur.execute(f'TRUNCATE TABLE "{schema}".orders')
        conn.commit()
    _mixed_txns(conn, schema, start=100, count=15)


def _assert_views_match(
    conn: psycopg2.extensions.connection, schema: str, grouped_view: str, global_view: str
) -> None:
    """Targets must equal ground-truth GROUP BY results, via ORDER BY'd fetches."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT status, count(*), count(coupon), sum(total), avg(total) "
            f'FROM "{schema}".orders GROUP BY status ORDER BY status NULLS LAST'
        )
        truth_grouped = cur.fetchall()
        cur.execute(
            f"SELECT status, order_count, with_coupon, revenue, avg_order_value "
            f'FROM walflux."{grouped_view}" ORDER BY status NULLS LAST'
        )
        got_grouped = cur.fetchall()
        assert len(truth_grouped) >= 4, "workload should span several groups incl. NULL"
        assert got_grouped == truth_grouped

        cur.execute(
            f'SELECT count(*), count(coupon), sum(total), avg(total) FROM "{schema}".orders'
        )
        truth_global = cur.fetchall()
        cur.execute(
            f"SELECT order_count, with_coupon, revenue, avg_order_value "
            f'FROM walflux."{global_view}"'
        )
        got_global = cur.fetchall()
        assert len(got_global) == 1, "global view must hold exactly one row"
        assert got_global == truth_global

        cur.execute(f'SELECT count(*) FROM walflux."{grouped_view}" WHERE __walflux_rows <= 0')
        assert cur.fetchone()[0] == 0, "empty groups must be deleted, never negative"


def test_kill9_exactly_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WALFLUX_DSN", raising=False)  # the file's dsn must win
    suffix = secrets.token_hex(3)
    schema = f"wf_e2e_{suffix}"
    slot = f"walflux_e2e_{suffix}"
    grouped_view = f"e2e_by_status_{suffix}"
    global_view = f"e2e_global_{suffix}"
    log_path = tmp_path / "daemon.log"

    admin = psycopg2.connect(DSN)
    admin.autocommit = True
    config = None
    proc: subprocess.Popen[bytes] | None = None
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f'CREATE TABLE "{schema}".orders ('
                "id bigserial PRIMARY KEY, status text, total numeric(10,2), coupon text)"
            )
            # Pre-snapshot rows: these are covered by the backfill, not the stream.
            cur.execute(
                f'INSERT INTO "{schema}".orders (status, total, coupon) '
                "SELECT (ARRAY['new', 'paid', 'shipped'])[1 + i % 3], (i + 10)::numeric(10,2), "
                "CASE WHEN i % 4 = 0 THEN 'save' || i END "
                "FROM generate_series(1, 30) AS g(i)"
            )
            cur.execute(
                f'INSERT INTO "{schema}".orders (status, total, coupon) '
                "VALUES (NULL, NULL, NULL), (NULL, 5.00, 'x')"
            )

        cfg_path = tmp_path / "walflux.yaml"
        cfg_path.write_text(_config_yaml(schema, slot, grouped_view, global_view))
        config = load_config(str(cfg_path))
        setup(config, force=True)

        runner = tmp_path / "run_daemon.py"
        runner.write_text(_RUNNER)
        proc = _start_daemon(runner, cfg_path, log_path)

        writer = psycopg2.connect(DSN)
        try:
            _workload_one(writer, schema)
            _wait_caught_up(admin, slot, schema, log_path)
            _assert_views_match(admin, schema, grouped_view, global_view)

            # kill -9 mid-second-workload: the timer fires while the writer is
            # still committing (count * sleep >> 0.15s), so the SIGKILL lands
            # while the daemon is actively flushing — then the writer keeps
            # going and WAL accumulates against a dead consumer.
            killer = threading.Timer(0.15, proc.kill)
            killer.start()
            try:
                _mixed_txns(writer, schema, start=200, count=15, sleep=0.03)
            finally:
                killer.join()
            proc.wait(timeout=10)
            _mixed_txns(writer, schema, start=300, count=15)
        finally:
            writer.close()

        _wait_slot_released(admin, slot)
        proc = _start_daemon(runner, cfg_path, log_path)
        _wait_caught_up(admin, slot, schema, log_path)
        _assert_views_match(admin, schema, grouped_view, global_view)

        # Deterministic crash in the commit-to-ack window: the daemon applies
        # deltas + checkpoint, commits, then dies before acknowledging. The
        # server's confirmed_flush stays behind the checkpoint, so on restart
        # it redelivers transactions that are already durably applied — the
        # skip rule (Commit.end_lsn <= checkpoint) must discard them, or the
        # ground-truth comparison below would show doubled aggregates.
        proc.terminate()
        proc.wait(timeout=10)
        proc = _start_daemon(
            runner,
            cfg_path,
            log_path,
            extra_env={"WALFLUX_TEST_CRASH": "after_commit_before_ack"},
        )
        writer = psycopg2.connect(DSN)
        try:
            _mixed_txns(writer, schema, start=400, count=5)
        finally:
            writer.close()
        assert proc.wait(timeout=30) == 1, "daemon should exit hard after its first flush"
        _wait_slot_released(admin, slot)
        proc = _start_daemon(runner, cfg_path, log_path)
        _wait_caught_up(admin, slot, schema, log_path)
        _assert_views_match(admin, schema, grouped_view, global_view)
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()  # SIGTERM: exercises the graceful-stop path
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if config is not None:
            try:
                teardown(config)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
